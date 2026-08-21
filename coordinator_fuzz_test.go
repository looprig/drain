package drain_test

// The interleaving fuzz target. Every other test in this suite drives one
// arrangement of holds, releases and cancellations that a person thought of;
// this one drives arrangements nobody did.
//
// What it searches is the SHAPE of a run: which reasons are held, at which
// point each hold is taken and at which it is dropped, which cause starts the
// drain, and whether and when Wait's context is cancelled. Each arrangement is
// driven to completion and checked against the four invariants below. The space
// is large and its interesting members are tedious to write by hand — a hold
// taken in the settle window of a signal drain, released from inside the
// OnDeadline of a different reason, while the context is cancelled at the
// ceiling — which is the case for generating them instead.
//
// The class of defect it exists for is a hold the coordinator grants and then
// loses track of. A mutex documented as guarding "only the counters" admits an
// Acquire that reads admission as open, is overtaken by the cut and its
// snapshot, and then increments into a snapshot already taken — a hold that is
// nowhere in Waited, that nothing waits for, and that surfaces in Abandoned on
// a drain that reported itself clean.
//
// That it finds that defect is measured rather than asserted. With exactly that
// split injected into admit, the target fails in seconds — 3.4s and 1.4s on two
// independent runs from an empty corpus under -race, each minimizing to a
// single-hold input — and invariant 3 is what fails, by both of its halves at
// once.
//
// # What it took to make that true, and what it costs if it regresses
//
// The finding above is only reachable through acquireRacingTheCut, and reaching
// it depends on two things that are easy to lose silently.
//
// The acquisition points must actually be three distinct values. They were
// briefly not: a const block with no iota repeats its last expression, so
// acquireRacingTheCut became a second spelling of fuzzInSettle and
// numAcquirePoints became 1, which made the decoder's "% numAcquirePoints" pin
// every hold to phase 0. The target still passed everything, and it passed the
// injected defect too — 5,001,635 executions over 90s under -race, reported
// PASS — because no input could reach the one acquisition point that races the
// cut. A fuzz target that cannot fail is worse than no fuzz target, so that
// block is now written out longhand with the trap named in its comment.
//
// And the racing acquisition must stay unordered. Every other acquisition here
// is separated from the cut by a synctest.Wait, and a target whose every
// acquisition sits behind a barrier can never produce an Acquire that overlaps
// the cut's critical section, however many inputs it tries. Ordering is what
// buys reproducibility everywhere else in this file; at that one point it would
// buy the wrong thing.
//
// The bubble does not make this search vacuous, but it does bound it. A bubble
// makes scheduling deterministic per input, so this target does not re-run one
// input against many schedules the way a stress loop does — it varies the input
// and takes one schedule from each. That is enough here because the racing
// point puts the Acquire and the cut on two goroutines with no barrier between
// them, and the surrounding plan varies what else is in flight. It is not a
// general substitute for driving two specific operations at each other:
// TestAdmissionCutoffAndSnapshotAreOneCriticalSection in cutoff_test.go does
// that, outside any bubble, on the real scheduler, and it kills the same
// injected defect at trial 0. The two are complementary — that one knows which
// critical section to attack and nothing about the shapes a drain can take;
// this one knows nothing about the implementation and searches the shapes.
//
// # The invariants
//
//  1. No counter ever goes negative. Checked as an equality rather than a
//     bound: at every quiescent point the coordinator's Holds must equal the
//     harness's own count of grants minus releases, which fails on a phantom
//     hold as well as on a dropped one.
//  2. Wait always returns within the grace ceiling. The driver advances the
//     clock to the ceiling and no further; a Wait that has not returned by then
//     leaves the bubble deadlocked, which synctest reports as a failure naming
//     the goroutine.
//  3. A hold granted at any point is either waited for or reported in
//     Abandoned. A clean return means nothing was outstanding; any other return
//     must account in Abandoned for everything still held, and Abandoned's
//     reasons must all appear in Waited — a reason that never reached the cut
//     cannot be outstanding at the return, because admission closed there.
//  4. Outcome is non-zero on every return path.
//
// # Two traps, both paid for once already
//
// ONE ADVANCING GOROUTINE. Holds are taken and dropped from a goroutine each,
// but only the driver advances the clock. An Advance overtaken by a concurrent
// one is a complete no-op — the target is computed at entry and the exit is
// guarded by a comparison against it — so a target that advanced from several
// goroutines would leave the clock short of the ceiling, and invariant 2 would
// become unfalsifiable rather than tested: the ceiling is read through Now.
//
// NO IDLE TIMEOUT. draintest deliberately retains stopped-unfired timers so
// FireStopped can reach them, and the coordinator re-arms the idle timer on
// every nonzero-to-zero transition of the total. A target that churned holds
// against an idle-enabled coordinator would accumulate one dead timer per
// transition and pay a full scan per firing — quadratic, and seconds of clock
// bookkeeping before any coordinator logic ran. The idle path has its own
// deterministic tests in idle_test.go, including the forced lost race that only
// FireStopped can produce.
//
// # Why a synctest bubble rather than BlockUntil
//
// The plan's first rule is to order the arm before the advance, and the suite's
// mechanism for that is rig.reach, which knows how many timers the coordinator
// arms in each phase. A fuzz target cannot know that number: it varies with the
// plan, with which acquisitions were refused, and with which deadlines were
// already spent when the wait phase began. The bubble answers the question the
// count was standing in for — synctest.Wait returns only once every other
// goroutine is durably blocked, and a coordinator that is about to arm a timer
// is not — so every advance here is ordered against every arm by construction.
//
// It also makes a failure reproducible. The engine reports the input that
// failed, and an input that only fails on some schedules is a corpus entry that
// wastes a maintainer's afternoon. The bound that determinism puts on what this
// target can search, and why acquireRacingTheCut still gets past it, are in the
// header.

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/looprig/drain"
	"github.com/looprig/drain/draintest"
)

// The policy every plan runs against. The three reasons cover the three
// deadline shapes the module has: a zero deadline that is spent before the wait
// phase begins, an ordinary one that expires inside it, and no configuration at
// all, whose deadline is the grace period and therefore collides with the
// ceiling.
const (
	fuzzGrace        = 30 * time.Second
	fuzzSettle       = 5 * time.Second
	fuzzTurnDeadline = 10 * time.Second
)

// fuzzReasons is indexed by the plan; the order is the encoding, so it is not
// rearranged without invalidating the corpus.
var fuzzReasons = [...]Reason{reasonTurn, reasonGate, reasonProc}

// The reason indices, for the seed corpus.
const (
	idxTurn byte = iota
	idxGate
	idxProc
)

// The points a plan can act at. They are the driver's timeline: a gate is
// closed at each one, and every hold worker is parked on the gate it acquires
// at and then on the gate it releases at.
const (
	// fuzzBeforeBegin is before the drain starts, where admission is open
	// whatever the cause.
	fuzzBeforeBegin = iota

	// fuzzInSettle is after Begin and before the settle window expires. It is
	// the phase the cutoff asymmetry lives in: a signal drain still grants
	// here, and every other cause has already refused.
	fuzzInSettle

	// fuzzInWait is after the cut, at the instant the settle window expired.
	fuzzInWait

	// fuzzLateInWait is deeper into the wait phase, past the ordinary
	// per-reason deadline.
	fuzzLateInWait

	// numFuzzPhases bounds the two selectors above.
	numFuzzPhases
)

// The points a hold can be TAKEN at. The first two are gates, and their values
// coincide with the phases of the same name so that a worker can park on
// gates[acquireAt] directly. The third is not a gate and not ordered.
//
// Every value here is written out. A const block whose first line carries an
// expression and whose later lines do not REPEATS that expression rather than
// continuing a sequence, and this block has no iota to make the repetition
// visible — written as bare names, acquireRacingTheCut silently became a second
// spelling of fuzzInSettle and numAcquirePoints became 1, which made the
// decoder's "% numAcquirePoints" pin every hold to phase 0 and left the racing
// path unreachable by any input.
const (
	acquireBeforeBegin = fuzzBeforeBegin
	acquireInSettle    = fuzzInSettle

	// acquireRacingTheCut releases the worker and advances the clock into the
	// admission cutoff without an intervening synctest.Wait, so the Acquire and
	// the cut are concurrent on two goroutines rather than separated by a
	// barrier.
	//
	// It is the one deliberately UNORDERED point in the target, and it is the
	// only one that reaches the defect invariant 3 exists for: a target whose
	// every acquisition is separated from the cut by a synctest.Wait can never
	// produce an Acquire that overlaps the cut's own critical section, however
	// many inputs it tries. Ordering buys reproducibility everywhere else; here
	// it would buy the wrong thing. This is load-bearing and it has been
	// verified — see the header for the injected defect and the times.
	acquireRacingTheCut = acquireInSettle + 1

	// numAcquirePoints bounds the acquisition selector.
	numAcquirePoints = acquireRacingTheCut + 1
)

// The release points that are not a phase.
const (
	// releaseNever holds until after Wait has returned, which is what puts the
	// hold in Abandoned. The release still runs at the end of the run, where
	// the coordinator must still honour it.
	releaseNever = numFuzzPhases + iota

	// releaseInCallback releases from inside OnDeadline, on the callback's own
	// stack. It is the expected shape of abandonment — the owner interrupts
	// work whose deferred release then runs there — and it is the interleaving
	// that deadlocks against any implementation that invokes a callback under
	// the coordinator's mutex.
	releaseInCallback

	// numReleasePoints bounds the release selector.
	numReleasePoints
)

// noCancel is the cancellation selector's out-of-band value: this plan never
// cancels Wait's context.
const noCancel = numFuzzPhases

// fuzzHold is one hold in a plan: which reason, when it is taken, and when it
// is dropped.
type fuzzHold struct {
	reason    Reason
	acquireAt int
	release   int
}

// fuzzPlan is a whole interleaving, decoded from the fuzz input.
type fuzzPlan struct {
	cause    drain.Cause
	cancelAt int
	holds    []fuzzHold
}

// maxFuzzHolds bounds a plan. The bound is not for the coordinator's sake — the
// counters are ints — but for the engine's: a target whose cost grows with the
// input spends its budget on one enormous plan instead of on many small ones,
// and every defect this is looking for is reachable with a handful of holds.
const maxFuzzHolds = 6

// byteStream reads a plan out of the fuzz input, yielding zero once the input
// is exhausted. Being total is the point: every []byte is a valid plan, so the
// engine never wastes an execution on an input the decoder rejected, and a
// corpus entry never becomes invalid because the decoder grew a field.
type byteStream struct {
	data []byte
	i    int
}

func (s *byteStream) next() byte {
	if s.i >= len(s.data) {
		return 0
	}
	b := s.data[s.i]
	s.i++
	return b
}

// decodePlan derives an interleaving from the fuzz input.
//
// Every selector is taken modulo the size of its domain rather than rejected
// when out of range, which biases the low values slightly and is worth it: a
// decoder that skipped inputs would hand the engine's coverage feedback a cliff
// where a one-byte mutation turns a plan into no plan at all.
func decodePlan(data []byte) fuzzPlan {
	in := &byteStream{data: data}

	var plan fuzzPlan
	switch in.next() % 3 {
	case 0:
		plan.cause = drain.CauseSignal
	case 1:
		plan.cause = drain.CauseExplicit
	default:
		plan.cause = drain.CauseIdle
	}

	plan.cancelAt = int(in.next()) % (numFuzzPhases + 1)

	n := 1 + int(in.next())%maxFuzzHolds
	plan.holds = make([]fuzzHold, 0, n)
	for range n {
		where, when := int(in.next()), int(in.next())
		plan.holds = append(plan.holds, fuzzHold{
			reason:    fuzzReasons[where%len(fuzzReasons)],
			acquireAt: (where / len(fuzzReasons)) % numAcquirePoints,
			release:   when % numReleasePoints,
		})
	}
	return plan
}

// fuzzSeed encodes a plan into the bytes decodePlan reads back, so the corpus
// can name the interleavings that are known to matter instead of hoping the
// engine stumbles into them. It is the decoder's inverse and shares its
// constants; a change to one that is not made to the other shows up as a seed
// that no longer describes what its comment says it does.
func fuzzSeed(cause, cancelAt byte, holds ...[2]byte) []byte {
	seed := []byte{cause, cancelAt, byte(len(holds) - 1)}
	for _, hold := range holds {
		seed = append(seed, hold[0], hold[1])
	}
	return seed
}

// seedHold encodes one hold for fuzzSeed.
func seedHold(reason, acquireAt, release byte) [2]byte {
	return [2]byte{reason + byte(len(fuzzReasons))*acquireAt, release}
}

// The cause selectors, encoder side.
const (
	seedSignal byte = iota
	seedExplicit
	seedIdle
)

// FuzzCoordinatorInterleavings drives a whole drain sequence per input: holds
// taken and dropped from a goroutine each, one goroutine advancing the clock,
// and the four invariants above asserted at every quiescent point and at the
// return.
func FuzzCoordinatorInterleavings(f *testing.F) {
	// The pending gate: a zero deadline that is already spent when the wait
	// phase begins, on a hold that never releases. Abandoned by design.
	f.Add(fuzzSeed(seedSignal, noCancel, seedHold(idxGate, fuzzBeforeBegin, releaseNever)))

	// The ordinary shape: a turn that releases inside the wait phase, before
	// its own deadline.
	f.Add(fuzzSeed(seedSignal, noCancel, seedHold(idxTurn, fuzzBeforeBegin, fuzzInWait)))

	// A hold taken INSIDE the open settle window, which only a signal drain
	// grants, released after its deadline has passed.
	f.Add(fuzzSeed(seedSignal, noCancel, seedHold(idxTurn, fuzzInSettle, fuzzLateInWait)))

	// The same acquisition under a cause that closes admission at Begin, where
	// it must be refused.
	f.Add(fuzzSeed(seedExplicit, noCancel, seedHold(idxTurn, fuzzInSettle, fuzzInWait)))

	// Release from inside OnDeadline, on an unconfigured reason whose deadline
	// is the ceiling itself.
	f.Add(fuzzSeed(seedSignal, noCancel, seedHold(idxProc, fuzzBeforeBegin, releaseInCallback)))

	// Cancellation before anything called Begin: the CauseContext path.
	f.Add(fuzzSeed(seedSignal, fuzzBeforeBegin, seedHold(idxTurn, fuzzBeforeBegin, releaseNever)))

	// Cancellation mid-wait with two reasons outstanding, one of which releases
	// from inside the callback the cancellation sweep dispatches.
	f.Add(fuzzSeed(seedIdle, fuzzInWait,
		seedHold(idxTurn, fuzzBeforeBegin, releaseNever),
		seedHold(idxGate, fuzzBeforeBegin, releaseInCallback),
	))

	// Three holds of one reason, dropped at three different points: the
	// counter arithmetic that a per-reason release guard would break.
	f.Add(fuzzSeed(seedSignal, noCancel,
		seedHold(idxTurn, fuzzBeforeBegin, fuzzInSettle),
		seedHold(idxTurn, fuzzBeforeBegin, fuzzInWait),
		seedHold(idxTurn, fuzzBeforeBegin, releaseNever),
	))

	// The unordered acquisition, which no other seed reaches: Acquire driven in
	// a loop against the cut with no barrier between them. The release selector
	// is ignored for a racing hold, so it is set to the shape a maintainer
	// reading the corpus should not expect to matter.
	f.Add(fuzzSeed(seedSignal, noCancel, seedHold(idxTurn, acquireRacingTheCut, releaseNever)))

	// The same, under a cause that has already closed admission at Begin: every
	// attempt in the loop must be refused rather than the first few granted.
	f.Add(fuzzSeed(seedExplicit, noCancel, seedHold(idxProc, acquireRacingTheCut, releaseNever)))

	// The degenerate inputs, which the decoder must survive: nothing at all,
	// and every selector at its maximum.
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		plan := decodePlan(data)
		synctest.Test(t, func(t *testing.T) { runFuzzPlan(t, plan) })
	})
}

// fuzzRun is the harness's own account of what was granted and released. It is
// the other half of every assertion: the coordinator's numbers are only
// checkable against numbers kept independently of it.
type fuzzRun struct {
	t     *testing.T
	coord *drain.Coordinator

	// mu guards the four maps below. The hold workers, the OnDeadline
	// callbacks, and the driver all touch them, and although every assertion is
	// made at a point where the bubble is quiescent, the mutations are not.
	mu       sync.Mutex
	granted  map[Reason]int
	released map[Reason]int
	pending  map[Reason][]func()
	reported map[Reason]int
}

func newFuzzRun(t *testing.T) *fuzzRun {
	return &fuzzRun{
		t:        t,
		granted:  make(map[Reason]int),
		released: make(map[Reason]int),
		pending:  make(map[Reason][]func()),
		reported: make(map[Reason]int),
	}
}

// acquire takes a hold and records it if it was granted. A refusal is recorded
// nowhere, which is the point: a refused Acquire must take no state with it,
// and the returned release must be a no-op that the harness can still call.
func (run *fuzzRun) acquire(r Reason) (func(), bool) {
	release, ok := run.coord.Acquire(r)
	if release == nil {
		run.t.Errorf("Acquire(%q) returned a nil release with ok == %t", r, ok)
		return func() {}, false
	}
	if !ok {
		return release, false
	}

	run.mu.Lock()
	run.granted[r]++
	run.mu.Unlock()

	// Once-guarded on the harness's side as well as the coordinator's. The
	// coordinator's release is idempotent, so a double call decrements once;
	// the harness's own count has to agree with that rather than with the
	// number of calls.
	var once sync.Once
	return func() {
		once.Do(func() {
			release()
			run.mu.Lock()
			run.released[r]++
			run.mu.Unlock()
		})
	}, true
}

// maxChurn bounds the churn loop. Reaching it means admission never closed,
// which is a coordinator bug and must be reported as one: an unbounded spin
// would instead leave a goroutine that is never durably blocked, and the whole
// bubble would hang on the next synctest.Wait with nothing to name the cause.
const maxChurn = 1 << 16

// churn takes and drops holds of reason r until admission refuses one. It is
// what the racing acquisition point actually does, and the loop rather than a
// single attempt is the difference between exercising the cut's critical
// section and merely being scheduled near it.
//
// A single Acquire released concurrently with the advance almost always loses:
// waking a goroutine costs more than the cut's whole critical section, so the
// acquisition lands after admission has closed and is refused, which is correct
// behaviour and tests nothing. A loop is already running when the cut happens,
// and it stops the moment admission closes — which is the very transition it is
// racing.
//
// Each grant is held across a Gosched rather than released immediately, so a
// hold that the cut should have seen exists for long enough to be missing from
// somewhere. The plan's release point is unused for a racing hold; the loop
// owns both ends.
func (run *fuzzRun) churn(r Reason) {
	for range maxChurn {
		release, ok := run.acquire(r)
		if !ok {
			return
		}
		runtime.Gosched()
		release()
	}
	run.t.Errorf("Acquire(%q) was still granting after %d attempts across the admission cutoff, want a refusal",
		r, maxChurn)
}

// oweCallback registers a release for the OnDeadline callback of reason r to
// run. A callback that never fires simply never runs it, and the hold stays
// outstanding — which is a legitimate plan rather than a harness failure.
func (run *fuzzRun) oweCallback(r Reason, release func()) {
	run.mu.Lock()
	defer run.mu.Unlock()
	run.pending[r] = append(run.pending[r], release)
}

// onDeadline is the plan's WithOnDeadline. It asserts the two properties of the
// invocation itself and then runs whatever releases the plan owes this reason,
// on the callback's own stack.
func (run *fuzzRun) onDeadline(_ context.Context, r Reason, outstanding int) {
	run.mu.Lock()
	run.reported[r]++
	reports := run.reported[r]
	releases := run.pending[r]
	delete(run.pending, r)
	run.mu.Unlock()

	if outstanding <= 0 {
		run.t.Errorf("OnDeadline(%q, %d), want a positive count: a reason whose holds are gone gets no callback at all",
			r, outstanding)
	}
	if reports > 1 {
		run.t.Errorf("OnDeadline fired %d times for %q, want once per reason", reports, r)
	}

	for _, release := range releases {
		release()
	}
}

// outstanding reports the harness's own count of what is still held.
func (run *fuzzRun) outstanding() map[Reason]int {
	run.mu.Lock()
	defer run.mu.Unlock()

	held := make(map[Reason]int, len(run.granted))
	for r, granted := range run.granted {
		switch n := granted - run.released[r]; {
		case n < 0:
			run.t.Fatalf("the harness released %q %d times having been granted it %d: a release ran more than once",
				r, run.released[r], granted)
		case n > 0:
			held[r] = n
		}
	}
	if len(held) == 0 {
		return nil
	}
	return held
}

// counters is invariant 1, asserted as an equality at a point where the bubble
// is quiescent and the two accounts must therefore agree exactly. A hold the
// coordinator dropped and a hold it invented both fail here.
func (run *fuzzRun) counters(when string) {
	run.t.Helper()

	want := run.outstanding()
	got := run.coord.Holds()
	if !maps.Equal(got, want) {
		run.t.Fatalf("Holds() = %v %s, want %v: the coordinator's counters and the harness's disagree", got, when, want)
	}
	if len(want) == 0 && got != nil {
		run.t.Errorf("Holds() = %v (non-nil, empty) %s, want nil", got, when)
	}
	for r, n := range got {
		if n <= 0 {
			run.t.Errorf("Holds()[%q] = %d %s, want a positive count or no entry", r, n, when)
		}
	}
}

// runFuzzPlan executes one interleaving inside a synctest bubble.
func runFuzzPlan(t *testing.T, plan fuzzPlan) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := draintest.NewClock(epoch)
	run := newFuzzRun(t)
	coord, err := drain.New(fuzzGrace,
		drain.WithClock(clock),
		drain.WithPreDrainDelay(fuzzSettle),
		drain.WithDeadline(reasonGate, 0),
		drain.WithDeadline(reasonTurn, fuzzTurnDeadline),
		drain.WithOnQuiesce(func(drain.Cause) {}),
		drain.WithOnDeadline(run.onDeadline),
		// No WithIdleTimeout, deliberately: see the target's comment.
	)
	if err != nil {
		t.Fatalf("New(%v) = _, %v, want a valid coordinator", fuzzGrace, err)
	}
	run.coord = coord

	// One gate per phase, closed by the driver in order, and one channel closed
	// after the assertions so that a hold which must outlive the drain has
	// something to park on. A worker parked on either is durably blocked, which
	// is what keeps synctest.Wait meaningful.
	var gates [numFuzzPhases]chan struct{}
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	finished := make(chan struct{})

	// racing is the unordered gate: it is closed immediately before the advance
	// that runs the cut, with nothing in between, so the acquisitions parked on
	// it run concurrently with the close of admission rather than before it.
	//
	// Its closer is once-guarded because it has two callers. The advance that
	// runs the cut is skipped entirely when the drain is already over — a
	// context cancelled before Begin returns from Wait without any clock
	// movement at all — and a worker parked on a gate nobody closes is a
	// goroutine the bubble waits for forever.
	racing := make(chan struct{})
	closeRacing := sync.OnceFunc(func() { close(racing) })

	var workers sync.WaitGroup
	for _, hold := range plan.holds {
		workers.Add(1)
		go func() {
			defer workers.Done()

			// Exhaustive over the acquisition points rather than an
			// if-and-fallthrough, so that adding one without deciding how a
			// worker reaches it fails loudly here instead of indexing gates
			// with it and parking on whichever phase happens to share its
			// value.
			switch hold.acquireAt {
			case acquireRacingTheCut:
				<-racing
				run.churn(hold.reason)
				return
			case acquireBeforeBegin, acquireInSettle:
				// Both are gates, and both name the phase whose gate they are.
				<-gates[hold.acquireAt]
			default:
				t.Errorf("acquisition point %d has no worker path, want one of %d, %d, %d",
					hold.acquireAt, acquireBeforeBegin, acquireInSettle, acquireRacingTheCut)
				return
			}

			release, ok := run.acquire(hold.reason)
			if !ok {
				return
			}

			switch hold.release {
			case releaseNever:
				// After Wait has returned, where the release must still
				// decrement: Holds stays truthful for a final metrics scrape.
				<-finished
				release()
			case releaseInCallback:
				run.oweCallback(hold.reason, release)
			default:
				<-gates[hold.release]
				release()
			}
		}()
	}

	// Ordered so that the parked workers are released BEFORE they are waited
	// for; the reverse would park the driver behind a worker parked on it.
	defer func() {
		closeRacing()
		close(finished)
		workers.Wait()
	}()

	done := make(chan waitResult, 1)
	go func() {
		out, err := coord.Wait(ctx)
		done <- waitResult{out: out, err: err}
	}()
	synctest.Wait()

	// Phase 0: admission is open whatever the cause, so every acquisition here
	// is granted.
	close(gates[fuzzBeforeBegin])
	synctest.Wait()
	run.counters("before the drain started")

	// The drain starts, either because something called Begin or because Wait's
	// context was cancelled — which is itself a Begin, with CauseContext.
	if plan.cancelAt == fuzzBeforeBegin {
		cancel()
	} else {
		coord.Begin(plan.cause)
	}
	synctest.Wait()

	// Phase 1: inside the settle window, where a signal drain still grants and
	// every other cause has already refused.
	close(gates[fuzzInSettle])
	synctest.Wait()
	run.counters("inside the settle window")

	// The advances. Each is preceded by a synctest.Wait, so the coordinator has
	// armed everything it is going to arm before the clock moves past it, and
	// each is skipped once Wait has returned — a drain that is over has no
	// timers left to reach and advancing further would only move a clock nobody
	// reads.
	advance := func(d time.Duration, gate int, at string, unordered ...func()) {
		select {
		case res := <-done:
			done <- res
			return
		default:
		}
		if plan.cancelAt == gate {
			cancel()
			synctest.Wait()
		}
		// Closed here and NOT waited for: the workers this releases are meant
		// to run concurrently with the advance below. Every other gate in the
		// driver is followed by a synctest.Wait.
		for _, open := range unordered {
			open()
		}
		clock.Advance(d)
		synctest.Wait()
		run.counters(at)
	}

	// The settle window expires: admission closes and the cut snapshots what it
	// is owed. Then the wait phase, one advance to the ordinary deadline and a
	// last one that lands exactly on the ceiling. Each advance carries the
	// phase whose cancellation belongs immediately before it, so all four
	// cancellation points are reachable and none of them is noCancel.
	advance(fuzzSettle, fuzzInSettle, "at the admission cutoff", closeRacing)

	close(gates[fuzzInWait])
	synctest.Wait()
	run.counters("at the start of the wait phase")
	advance(fuzzTurnDeadline-fuzzSettle, fuzzInWait, "at the per-reason deadline")

	close(gates[fuzzLateInWait])
	synctest.Wait()
	run.counters("past the per-reason deadline")
	advance(fuzzGrace-fuzzTurnDeadline, fuzzLateInWait, "at the grace ceiling")

	// Invariant 2. The clock has reached the ceiling and no further, so a Wait
	// that has not returned by now never will: every goroutine in the bubble is
	// durably blocked and synctest fails the test with a dump naming this
	// receive and the phase the coordinator is stuck in.
	res := <-done
	synctest.Wait()
	run.counters("after Wait returned")
	checkOutcome(t, run, plan, res)
}

// checkOutcome asserts invariants 3 and 4 over one return.
func checkOutcome(t *testing.T, run *fuzzRun, plan fuzzPlan, res waitResult) {
	t.Helper()

	// Invariant 4, and more than the letter of it: the cause a plan produces is
	// known exactly. Begin runs before any cancellation except the one that
	// happens before it, and the first cause wins.
	wantCause := plan.cause
	if plan.cancelAt == fuzzBeforeBegin {
		wantCause = drain.CauseContext
	}
	if res.out.Cause != wantCause {
		t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, wantCause)
	}
	if res.out.Duration < 0 || res.out.Duration > fuzzGrace {
		t.Errorf("Outcome.Duration = %v, want between 0 and the %v ceiling", res.out.Duration, fuzzGrace)
	}

	// The error taxonomy: the only two failures Wait can return, and neither of
	// them may be invented. A cancellation the plan never issued would mean the
	// coordinator cancelled itself.
	var overrun *drain.GracePeriodExceededError
	switch {
	case res.err == nil:
	case errors.Is(res.err, context.Canceled):
		if plan.cancelAt == noCancel {
			t.Errorf("Wait() = _, %v, want no cancellation: this plan never cancelled the context", res.err)
		}
	case errors.As(res.err, &overrun):
		if overrun.Elapsed != res.out.Duration {
			t.Errorf("GracePeriodExceededError.Elapsed = %v but Outcome.Duration = %v; they are one number",
				overrun.Elapsed, res.out.Duration)
		}
		if overrun.Budget != fuzzGrace {
			t.Errorf("GracePeriodExceededError.Budget = %v, want %v", overrun.Budget, fuzzGrace)
		}
		if !sameMap(overrun.Abandoned, res.out.Abandoned) {
			t.Errorf("GracePeriodExceededError.Abandoned (%v) is not the same map as Outcome.Abandoned (%v); one snapshot, two names",
				overrun.Abandoned, res.out.Abandoned)
		}
	default:
		t.Errorf("Wait() = _, %v (%T), want nil, a cancellation, or a *drain.GracePeriodExceededError", res.err, res.err)
	}

	// Invariant 3. A clean return is a claim that nothing was left: a hold
	// still outstanding there is precisely the hold that was granted and never
	// waited for.
	held := run.outstanding()
	if res.err == nil {
		if len(held) != 0 {
			t.Errorf("Wait() returned no error with %v still held: a hold was granted and never waited for", held)
		}
		if res.out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v on a clean drain, want nil", res.out.Abandoned)
		}
	}

	// Everything still held must be accounted for. Abandoned is snapshotted
	// before the last callbacks are dispatched, so it may be LARGER than what
	// is held once a release issued from inside one of them has run; it may
	// never be smaller.
	for r, n := range held {
		if got := res.out.Abandoned[r]; got < n {
			t.Errorf("Outcome.Abandoned[%q] = %d with %d still held: a hold was silently dropped", r, got, n)
		}
	}

	// Nothing may be abandoned that the cut never saw. Admission closes at the
	// cut, so a reason outstanding at the return was outstanding at the cut,
	// and every reason the cut saw has an entry in Waited.
	for r, n := range res.out.Abandoned {
		if n <= 0 {
			t.Errorf("Outcome.Abandoned[%q] = %d, want a positive count or no entry", r, n)
		}
		if _, ok := res.out.Waited[r]; !ok {
			t.Errorf("Outcome.Abandoned has %q but Outcome.Waited does not: a hold outlived a cut that never saw it",
				r)
		}
	}

	// The deadline reported for a reason is the one the policy applies to it,
	// which is what makes a misspelled WithDeadline visible in telemetry.
	for r, waited := range res.out.Waited {
		if want := fuzzDeadline(r); waited != want {
			t.Errorf("Outcome.Waited[%q] = %v, want %v", r, waited, want)
		}
	}
}

// fuzzDeadline reports the deadline the target's policy applies to r,
// independently of the coordinator's own lookup.
func fuzzDeadline(r Reason) time.Duration {
	switch r {
	case reasonGate:
		return 0
	case reasonTurn:
		return fuzzTurnDeadline
	default:
		return fuzzGrace
	}
}

// sameMap reports whether two maps are the same instance. The spec's contract
// is identity — one snapshot under two names — not equality, and two maps that
// merely agree today can disagree tomorrow.
func sameMap(a, b map[Reason]int) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}
