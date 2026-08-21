package drain_test

// External, unlike holds_test.go and begin_test.go, and not by preference: this
// file drives the sequence through the deterministic clock, and draintest
// imports drain, so an internal test that imported it would be an import cycle.
// Everything Task 7 delivers is exported anyway — the phases are observable
// through Acquire, Draining, and the Outcome that Wait returns.
//
// The one assertion that cannot be made from out here is the atomicity of the
// admission cutoff and the hold snapshot, which needs the unexported cut itself;
// it lives in cutoff_test.go, which needs no clock and so has no cycle.

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/looprig/drain"
	"github.com/looprig/drain/draintest"
)

// The default policy every rig is built with. Individual tests override one or
// both through newRigWithPolicy — the ceiling tests need a grace period the
// sequence can actually reach, and the idle tests need an idle timeout.
const (
	testGrace  = 30 * time.Second
	testSettle = 5 * time.Second
)

// Reasons. Package-prefixed, per the vocabulary rule on Reason.
const (
	reasonTurn Reason = "drain_test/active_turn"
	reasonGate Reason = "drain_test/pending_gate"
	reasonProc Reason = "drain_test/subprocess"
)

// Reason is aliased so the reason constants above read as they would in a
// consumer, without qualifying every one of them.
type Reason = drain.Reason

// epoch is an arbitrary fixed start time. Nothing in drain depends on the wall
// clock; a fixed value only keeps failure messages readable.
var epoch = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

// phase names a point in the sequence that a test can order an advance against.
// A phase is identified by the set of timers the coordinator has armed when it
// is there, which is why rig.armedIn — and not the individual tests — owns the
// count.
type phase int

const (
	// phaseIdle is Wait BEFORE the sequence has started: no drain is running,
	// so the only timer that can be armed is the idle timer, and it is armed
	// only while the total is zero. A rig with no idle timeout never reaches
	// this phase, because nothing arms.
	phaseIdle phase = iota

	// phaseSettle is Wait inside the settle window: the grace ceiling and the
	// settle timer. The idle timer is stopped by Begin, and a stopped timer is
	// not an armed one, so it does not count here.
	phaseSettle

	// phaseWait is Wait inside the wait phase: the grace ceiling, and one
	// deadline timer for every reason the cut named whose deadline had not
	// already passed. The settle timer has fired by then. A reason whose
	// deadline is spent when the wait phase begins — a zero-deadline pending
	// gate, or any deadline shorter than the settle window — arms nothing, so
	// it is not counted.
	phaseWait
)

// waitResult is one return from Wait, carried off the goroutine that made it.
type waitResult struct {
	out drain.Outcome
	err error
}

// bubbled runs fn inside a testing/synctest bubble, handing it a context created
// inside that bubble. Everything the test touches — the rig, its clock, its
// goroutines — must be created inside fn: a channel the bubble does not own is
// never durably blocking, so a goroutine selecting on a context from outside
// would keep synctest.Wait from ever returning.
//
// Two things earn the bubble, and the second is why the wait-phase tests use it
// by default where Task 7's did not. It supplies the happens-before edge that
// rig.stillDraining needs to prove Wait has not returned. And it turns an
// ordering bug into an immediate failure: when every goroutine is durably
// blocked, synctest fails the test with a dump naming the timer that was never
// armed, instead of hanging to the binary's ten-minute -timeout. A wait phase
// whose deadlines are all driven by a fake clock has many ways to reach that
// state and no other way to report it.
//
// draintest's doc comment names synctest as the alternative available to tests
// inside drain, and declines to make it the exported mechanism, because
// requiring a consumer to run its shutdown wiring inside a bubble is a
// constraint this module has no standing to impose. That reasoning is about
// draintest's contract, not about this suite.
func bubbled(t *testing.T, fn func(t *testing.T, ctx context.Context)) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fn(t, ctx)
	})
}

// rig couples a coordinator with the fake clock driving it, and owns the one
// number a test must never guess: how many timers the coordinator has armed at
// each point in its sequence. Every ordering point in the suite goes through
// reach or advance.
//
// The coordinator is embedded, so a test calls r.Acquire, r.Begin and r.Draining
// exactly as a consumer would.
type rig struct {
	t      *testing.T
	clock  *draintest.Clock
	policy rigPolicy
	done   chan waitResult

	// waitTimers is how many per-reason deadline timers this test's holds
	// produce, declared through armsInWait, and idleTimers is whether the idle
	// timer is armed at the ordering point a test is about to take, declared
	// through armsIdle. Both are rig state rather than parameters of armedIn
	// because each is a property of the holds a test takes and means nothing in
	// the other's phase: threading two counts through reach and advance would
	// make every ordering point in the suite carry a number that is noise there.
	//
	// idleTimers defaults to one for a rig with an idle timeout, which is the
	// state at Wait entry with nothing outstanding. A test that enters Wait
	// holding something — the startup-hold pattern — says armsIdle(0), because
	// the idle timer arms on a nonzero→zero transition and there has not been
	// one.
	waitTimers int
	idleTimers int

	*drain.Coordinator
}

// rigPolicy is the part of a coordinator's policy the rig must know to own the
// timer count. It is a struct rather than a growing parameter list so that a
// test names the value it is changing.
//
// It is not optional state: the rig can only own the count if it knows the
// policy that produces the timers, so a WithPreDrainDelay, WithIdleTimeout or
// WithClock passed through opts instead would silently desynchronize armedIn
// from the coordinator.
type rigPolicy struct {
	grace  time.Duration
	settle time.Duration
	idle   time.Duration

	// start is where the fake clock begins, defaulting to epoch. Only a test
	// that compares the coordinator's time base against the real one needs to
	// set it — the callback context's deadline is the one value drain reports
	// in clock time that a caller may hold up against a wall-clock deadline of
	// its own.
	start time.Time

	// wrapClock decorates the rig's clock before the coordinator is given it,
	// for the one kind of test that cannot be written against the fake alone: a
	// test that must stop a specific timer from ever firing, to prove which of
	// two mechanisms produced an effect. It goes through the policy rather than
	// through opts for the same reason WithClock does — the rig owns the clock,
	// because a coordinator reading a clock the rig does not drive turns every
	// reach in the suite into a permanent block.
	//
	// A decorator that swallows an arm moves the count reach orders against, so
	// a test that uses one must declare the difference through armsInWait or
	// armsIdle. That is deliberate: the count stays a single declared number
	// rather than something armedIn tries to infer from a function it cannot
	// see inside.
	wrapClock func(*draintest.Clock) drain.Clock
}

// newRig builds a coordinator on a deterministic clock with the standard policy.
func newRig(t *testing.T, opts ...drain.Option) *rig {
	t.Helper()
	return newRigWithSettle(t, testSettle, opts...)
}

// newRigWithSettle is newRig with a chosen settle window.
func newRigWithSettle(t *testing.T, settle time.Duration, opts ...drain.Option) *rig {
	t.Helper()
	return newRigWithPolicy(t, rigPolicy{grace: testGrace, settle: settle}, opts...)
}

// newRigWithPolicy builds a coordinator on a deterministic clock with a policy
// the rig knows in full.
//
// The rig's own options are appended LAST, so that last-option-wins gives the
// rig the win rather than the caller. That is enforcement, not preference: an
// opts-supplied WithPreDrainDelay would only mis-time the settle window, but an
// opts-supplied WithClock would leave r.clock advancing a clock the coordinator
// never reads, so every reach in the suite would block on an arm that can never
// happen. Documenting the rule and then letting opts override it puts a hang one
// typo away.
func newRigWithPolicy(t *testing.T, policy rigPolicy, opts ...drain.Option) *rig {
	t.Helper()

	if policy.start.IsZero() {
		policy.start = epoch
	}
	clock := draintest.NewClock(policy.start)
	var injected drain.Clock = clock
	if policy.wrapClock != nil {
		injected = policy.wrapClock(clock)
	}
	all := make([]drain.Option, 0, len(opts)+3)
	all = append(all, opts...)
	all = append(all,
		drain.WithClock(injected),
		drain.WithPreDrainDelay(policy.settle),
		drain.WithIdleTimeout(policy.idle),
	)

	c, err := drain.New(policy.grace, all...)
	if err != nil {
		t.Fatalf("New(%v) = _, %v, want a valid coordinator", policy.grace, err)
	}

	r := &rig{t: t, clock: clock, policy: policy, done: make(chan waitResult, 1), Coordinator: c}
	if policy.idle > 0 {
		r.idleTimers = 1
	}
	return r
}

// armedIn reports how many timers the coordinator has armed once it has reached
// p. This is the single place in the suite that knows that number, and it is
// deliberately a function of the rig's policy rather than a literal.
//
// BlockUntil counts every timer on the clock, not the one a test is waiting for.
// A test that hard-coded 1 would return the moment the coordinator armed any
// timer and then advance while a second one was still unarmed — leaving that
// second timer waiting on a deadline the clock has already passed, so the test
// hangs to the binary's -timeout instead of failing. Every timer the coordinator
// arms is accounted for here and nowhere else: the grace ceiling, the settle
// window, the per-reason deadlines, and the idle timer.
func (r *rig) armedIn(p phase) int {
	switch p {
	case phaseIdle:
		// Nothing else can be armed before the sequence starts: the ceiling,
		// the settle window and every deadline belong to a drain that has not
		// begun.
		return r.idleTimers
	case phaseSettle:
		// The grace ceiling, armed at quiesce and stopped when Wait returns,
		// plus the settle timer. A non-positive settle window arms nothing at
		// all — the sequence dispatches straight to the cut rather than arming
		// a timer the fake clock would never fire — and neither does a window
		// whose remainder is already spent at Wait entry, which is why
		// TestSettleWindowAlreadyElapsedAtWaitEntry never reaches a phase.
		n := 1
		if r.policy.settle > 0 {
			n++
		}
		return n
	case phaseWait:
		// The ceiling again, plus one deadline timer per reason still owed a
		// deadline at the cut. The settle timer has fired by then.
		return 1 + r.waitTimers
	default:
		r.t.Fatalf("armedIn(%d): unknown phase", p)
		return 0
	}
}

// armsInWait declares how many per-reason deadline timers the coordinator arms
// when this test's drain enters the wait phase. Call it before the first
// reach(phaseWait); it is the one number a wait-phase test supplies, and it
// supplies it here rather than to a BlockUntil of its own.
//
// Count the reasons outstanding at the cut whose deadline has NOT already
// passed. A spent deadline arms nothing, so counting it blocks reach forever;
// under a synctest bubble that is an immediate failure rather than a hang, which
// is why the wait-phase tests run inside one.
func (r *rig) armsInWait(n int) {
	r.t.Helper()
	r.waitTimers = n
}

// armsIdle declares how many idle timers are armed at the next reach(phaseIdle)
// — one, or none. It defaults to one for a rig with an idle timeout, which is
// the state at Wait entry with nothing outstanding.
//
// A test that enters Wait already holding something says armsIdle(0): the idle
// timer arms at Wait entry only if the total is already zero, and otherwise on a
// nonzero→zero transition, and the startup-hold pattern is precisely the case
// where neither has happened yet. It is a knob of its own rather than part of
// waitTimers because the idle timer is the one timer that arms and disarms
// repeatedly WITHIN a test, as holds come and go.
func (r *rig) armsIdle(n int) {
	r.t.Helper()
	r.idleTimers = n
}

// start runs Wait on its own goroutine and keeps its return for result.
func (r *rig) start(ctx context.Context) {
	go func() {
		out, err := r.Wait(ctx)
		r.done <- waitResult{out: out, err: err}
	}()
}

// reach blocks until the coordinator has armed every timer it arms in p. It is
// the first of the plan's three rules — order the arm before the advance — and
// the reason no test in this suite writes BlockUntil itself.
//
// A phase whose count is zero orders NOTHING: BlockUntil(0) returns at once and
// the equality check below trivially holds. That is reach(phaseIdle) on a rig
// that has said armsIdle(0) — the startup-hold pattern, where no idle timer is
// armed because there has been no nonzero-to-zero transition yet — and it is
// correct there precisely because there is no arm to order against.
//
// The equality check rests on an invariant of this suite rather than of the
// coordinator: no timer arms or disarms concurrently with it. Every timer but
// the idle one belongs to a phase the rig is ordering against, and the idle
// timer — the one timer that arms and disarms repeatedly within a test — moves
// only inside Acquire and release, both of which this suite calls
// synchronously on the test's own goroutine. A test that took or dropped a
// hold from a goroutine of its own would break that and should order the
// arming itself.
func (r *rig) reach(p phase) {
	r.t.Helper()

	want := r.armedIn(p)
	r.clock.BlockUntil(want)

	// BlockUntil unblocks at "at least n", so two phases whose counts coincide
	// alias: a reach(phaseWait) expecting the ceiling plus one deadline would
	// otherwise be satisfied by the ceiling plus the settle timer and return
	// while the coordinator was still in settle. It cannot happen as the suite
	// is written — every phaseWait ordering point follows the advance that fired
	// the settle timer — but "cannot happen" is exactly the assumption that
	// reintroduces the race the rig exists to prevent, and its failure is
	// silent. So the count is checked for equality once the block releases,
	// which turns the aliasing into an assertion instead.
	if got := r.clock.Armed(); got != want {
		r.t.Fatalf("Armed() = %d on reaching phase %d, want exactly %d: "+
			"the ordering point does not match the coordinator's timers", got, p, want)
	}
}

// advance reaches p and then moves the clock. Only one goroutine ever advances,
// which is the plan's second rule: an Advance overtaken by a concurrent one is a
// complete no-op.
func (r *rig) advance(p phase, d time.Duration) {
	r.t.Helper()
	r.reach(p)
	r.clock.Advance(d)
}

// closeSettleWindow runs the settle window out, which is phase 3: the
// settle→wait boundary where admission closes.
//
// The cut runs inside the settle timer's callback, and Advance runs callbacks on
// its own goroutine, so admission is provably closed by the time this returns —
// a test can assert the refusal immediately instead of racing Wait's goroutine.
func (r *rig) closeSettleWindow() {
	r.t.Helper()
	r.advance(phaseSettle, r.policy.settle)
}

// grants reports whether Acquire still admits work, without leaving a hold
// behind. Acquire's ok is the only admission authority; Draining is advisory.
func (r *rig) grants(reason Reason) bool {
	r.t.Helper()
	release, ok := r.Acquire(reason)
	if release == nil {
		r.t.Fatalf("Acquire(%q) returned a nil release with ok == %t", reason, ok)
	}
	release()
	return ok
}

// mustAcquire takes a hold that the test's setup requires to be granted, and
// returns its release. A refusal here is a broken premise rather than the
// assertion under test, so it fails the test on the spot.
func (r *rig) mustAcquire(reason Reason) func() {
	r.t.Helper()

	release, ok := r.Acquire(reason)
	if !ok {
		r.t.Fatalf("Acquire(%q) refused, want granted: admission is open until the cut", reason)
	}
	return release
}

// stillDraining asserts that Wait has NOT returned. It is only sound inside a
// synctest bubble: proving an absence needs a happens-before edge, and
// synctest.Wait supplies it by returning only once every other goroutine is
// durably blocked, so a Wait that was going to return already has. Outside a
// bubble the same non-blocking receive passes for an implementation that simply
// had not been scheduled yet.
func (r *rig) stillDraining(why string) {
	r.t.Helper()

	synctest.Wait()
	select {
	case res := <-r.done:
		r.t.Fatalf("Wait() returned %+v, want it still draining: %s", res, why)
	default:
	}
}

// armed asserts how many PER-REASON DEADLINE timers the coordinator currently
// has armed, during a running sequence. Every caller has already ordered itself
// against the arming through reach; this reports what happened to those timers
// afterwards, which is how a deadline's arithmetic is observed.
//
// The grace ceiling is added here rather than at each call site, which is the
// whole reason the count is expressed this way: a timer the coordinator holds
// for the length of the sequence is not a fact about any one test's deadlines,
// and folding it into twelve literals would mean twelve edits the next time the
// sequence gains one. Use allStopped for the assertion after Wait has returned,
// where the ceiling is stopped along with everything else.
func (r *rig) armed(wantDeadlines int, why string) {
	r.t.Helper()

	want := wantDeadlines + 1 // the grace ceiling, armed for the whole sequence
	if got := r.clock.Armed(); got != want {
		r.t.Fatalf("Armed() = %d, want %d (%d deadline timer(s) plus the grace ceiling): %s",
			got, want, wantDeadlines, why)
	}
}

// allStopped asserts that the coordinator left no timer armed. It is the
// after-the-fact half of armed: a drain that has returned must have stopped its
// ceiling and every deadline that never came due, rather than leaving them to
// fire against a spent coordinator.
func (r *rig) allStopped(why string) {
	r.t.Helper()

	if got := r.clock.Armed(); got != 0 {
		r.t.Fatalf("Armed() = %d after the drain, want 0: %s", got, why)
	}
}

// result blocks for Wait's return. It has no timeout of its own, following
// draintest.BlockUntil: the failure it leaves is a -timeout goroutine dump
// naming the phase that never completed, which says more than a deadline error
// would.
func (r *rig) result() waitResult {
	r.t.Helper()
	return <-r.done
}

// exceeded is result with the assertion an overrun makes: a valid Outcome AND a
// *GracePeriodExceededError, because the caller still has to exit cleanly and
// still wants the telemetry. It returns both, since the error's fields are the
// point of most of the tests that use it.
func (r *rig) exceeded(wantCause drain.Cause) (drain.Outcome, *drain.GracePeriodExceededError) {
	r.t.Helper()

	res := r.result()
	var overrun *drain.GracePeriodExceededError
	if !errors.As(res.err, &overrun) {
		r.t.Fatalf("Wait() = _, %v, want a *drain.GracePeriodExceededError", res.err)
	}
	if res.out.Cause != wantCause {
		r.t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, wantCause)
	}
	if res.out.Duration != overrun.Elapsed {
		r.t.Errorf("Outcome.Duration = %v but the error reports Elapsed = %v; they are one number",
			res.out.Duration, overrun.Elapsed)
	}
	if overrun.Budget != r.policy.grace {
		r.t.Errorf("GracePeriodExceededError.Budget = %v, want %v", overrun.Budget, r.policy.grace)
	}
	return res.out, overrun
}

// cancelled is result with the assertion the cancellation path makes: a valid
// Outcome AND ctx.Err(), because a drain that was told to stop mid-flight is
// the one whose telemetry an operator most needs — work was abandoned without
// any deadline having been reached.
//
// The cause is asserted rather than assumed to be CauseContext: a cancellation
// implies Begin, so it records CauseContext only when nothing else had.
func (r *rig) cancelled(wantCause drain.Cause) drain.Outcome {
	r.t.Helper()

	res := r.result()
	if !errors.Is(res.err, context.Canceled) {
		r.t.Fatalf("Wait() = _, %v, want context.Canceled", res.err)
	}
	if res.out.Cause != wantCause {
		r.t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, wantCause)
	}
	return res.out
}

// completed is result with the assertion every clean drain makes.
func (r *rig) completed(wantCause drain.Cause) drain.Outcome {
	r.t.Helper()

	res := r.result()
	if res.err != nil {
		r.t.Fatalf("Wait() = _, %v, want no error on a clean drain", res.err)
	}
	if res.out.Cause != wantCause {
		r.t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, wantCause)
	}
	return res.out
}

// TestSequenceOrderingSignal is the regression test for the defect the spec
// revision fixed: an implementation that closes admission at quiesce passes a
// naive ordering test and fails this one.
//
// The settle window exists because endpoint withdrawal is asynchronous —
// requests were already routed here when the signal arrived, and more arrive for
// a second or two. If admission closed at quiesce the window would protect
// nothing and merely delay exit, turning a graceful shutdown into a burst of
// 503s. So for CauseSignal, and only for CauseSignal, Acquire keeps granting
// through settle and starts refusing exactly at the boundary.
func TestSequenceOrderingSignal(t *testing.T) {
	t.Parallel()

	quiesced := make(chan drain.Cause, 1)
	r := newRig(t, drain.WithOnQuiesce(func(cause drain.Cause) { quiesced <- cause }))
	r.start(t.Context())

	r.Begin(drain.CauseSignal)

	// Phase 1: quiesce. The callback runs on a coordinator-owned goroutine, so
	// the readiness probe can start failing while the sequence continues.
	if got := <-quiesced; got != drain.CauseSignal {
		t.Errorf("OnQuiesce saw cause %v, want %v", got, drain.CauseSignal)
	}
	if !r.Draining() {
		t.Error("Draining() = false during the settle window, want true")
	}

	// Phase 2: settle, with admission still OPEN. This is the assertion.
	r.reach(phaseSettle)
	release, ok := r.Acquire(reasonTurn)
	if !ok {
		t.Fatal("Acquire refused inside the settle window of a signal drain, want granted: " +
			"admission closes at the settle boundary, not at quiesce")
	}

	// Phase 3: the boundary closes admission.
	r.closeSettleWindow()
	if r.grants(reasonGate) {
		t.Error("Acquire granted after the settle window elapsed, want refused")
	}

	// Phase 4: the hold taken during settle was admitted, so it is owed, and
	// Wait returns only once it releases. That the wait actually blocks is
	// asserted deterministically in TestWaitBlocksUntilOutstandingHoldsRelease
	// — a non-blocking receive here would race Wait's goroutine and never fire.
	release()
	out := r.completed(drain.CauseSignal)
	if got := r.Holds(); got != nil {
		t.Errorf("Holds() = %v after the drain, want nil", got)
	}
	if out.Duration != testSettle {
		t.Errorf("Outcome.Duration = %v, want %v", out.Duration, testSettle)
	}
}

// TestSequenceOrderingIdle is the other side of the asymmetry. Nothing external
// triggered an idle drain, so there is no in-flight routing to protect and
// admitting work would land it on a process that has already decided to die.
// Admission closes at Begin, before any of the settle window has elapsed.
//
// The settle window still runs: for an idle drain it is the readiness-probe
// propagation window, which is longer rather than shorter. It is a quiet window,
// not an open one.
func TestSequenceOrderingIdle(t *testing.T) {
	t.Parallel()

	r := newRig(t)
	r.start(t.Context())

	r.Begin(drain.CauseIdle)
	if r.grants(reasonTurn) {
		t.Error("Acquire granted immediately after Begin(CauseIdle), want refused")
	}

	r.reach(phaseSettle)
	if r.grants(reasonTurn) {
		t.Error("Acquire granted inside the settle window of an idle drain, want refused")
	}

	r.closeSettleWindow()
	if r.grants(reasonTurn) {
		t.Error("Acquire granted after the settle window of an idle drain, want refused")
	}
	r.completed(drain.CauseIdle)
}

// TestSequenceOrderingExplicit covers the remaining causes that close admission
// at Begin, the normalized one included: an out-of-range cause is recorded as
// CauseExplicit and takes the fail-secure admission path rather than inheriting
// CauseSignal's open window.
func TestSequenceOrderingExplicit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		begin drain.Cause
		want  drain.Cause
	}{
		{name: "explicit", begin: drain.CauseExplicit, want: drain.CauseExplicit},
		{name: "context", begin: drain.CauseContext, want: drain.CauseContext},
		{name: "unknown is normalized", begin: drain.CauseUnknown, want: drain.CauseExplicit},
		{name: "out of range is normalized", begin: drain.Cause(99), want: drain.CauseExplicit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := newRig(t)
			r.start(t.Context())

			r.Begin(tc.begin)
			if r.grants(reasonTurn) {
				t.Errorf("Acquire granted after Begin(%v), want refused: admission closes at Begin", tc.begin)
			}

			r.reach(phaseSettle)
			if r.grants(reasonTurn) {
				t.Errorf("Acquire granted inside the settle window after Begin(%v), want refused", tc.begin)
			}

			r.closeSettleWindow()
			r.completed(tc.want)
		})
	}
}

// TestOnQuiesceRunsOnceWithCause covers the callback contract of phase 1: once,
// with the cause the sequence recorded, on a goroutine the coordinator owns.
func TestOnQuiesceRunsOnceWithCause(t *testing.T) {
	t.Parallel()

	t.Run("once, with the recorded cause", func(t *testing.T) {
		t.Parallel()

		for _, cause := range []drain.Cause{drain.CauseSignal, drain.CauseIdle, drain.CauseExplicit, drain.CauseContext} {
			t.Run(cause.String(), func(t *testing.T) {
				t.Parallel()

				// Buffered for more than one call, so a second invocation is a
				// failed assertion rather than a blocked callback goroutine.
				quiesced := make(chan drain.Cause, 4)
				r := newRig(t, drain.WithOnQuiesce(func(c drain.Cause) { quiesced <- c }))
				r.start(t.Context())

				r.Begin(cause)
				if got := <-quiesced; got != cause {
					t.Errorf("OnQuiesce saw cause %v, want %v", got, cause)
				}
				r.closeSettleWindow()
				r.completed(cause)

				// Begin again after the sequence has run: a no-op, callback
				// included.
				r.Begin(drain.CauseSignal)
				select {
				case extra := <-quiesced:
					t.Errorf("OnQuiesce ran a second time, with cause %v", extra)
				default:
				}
			})
		}
	})

	// The callback here never returns, and the drain has nothing outstanding, so
	// Wait must return at the settle boundary with err == nil.
	//
	// Both halves of that are the contract rather than an accident of this
	// task's implementation. An OnQuiesce run on Wait's own goroutine hangs
	// here. So does one the wait phase treats as an exit gate — and that is the
	// assertion worth being explicit about, because "wait for OnQuiesce" is the
	// plausible reading of "the ceiling covers phase 1". It is not: being
	// covered by the ceiling and being waited for are different things, and only
	// the first applies. OnQuiesce's job is to fail a readiness probe, which the
	// closed admission flag has already made redundant, so a process with
	// nothing outstanding must not be held for the grace period on its account.
	//
	// What this test does NOT show is that the ceiling covers phase 1 at all: it
	// passes just as well against a coordinator that never waits on OnQuiesce
	// AND has no ceiling. TestHungOnQuiesceDoesNotBlockCeiling is the one that
	// needs a hold that never releases, and it is the one that proves it.
	t.Run("on a coordinator-owned goroutine", func(t *testing.T) {
		t.Parallel()

		entered := make(chan struct{})
		block := make(chan struct{})
		t.Cleanup(func() { close(block) })

		r := newRig(t, drain.WithOnQuiesce(func(drain.Cause) {
			close(entered)
			<-block
		}))
		r.start(t.Context())

		r.Begin(drain.CauseSignal)
		<-entered
		r.closeSettleWindow()
		r.completed(drain.CauseSignal)
	})
}

// TestWaitReturnsWhenNoHolds is the empty-process path: nothing is outstanding
// at the cut, so the wait phase has nothing to wait for and the sequence ends at
// the settle boundary.
func TestWaitReturnsWhenNoHolds(t *testing.T) {
	t.Parallel()

	t.Run("nothing was ever acquired", func(t *testing.T) {
		t.Parallel()

		r := newRig(t)
		r.start(t.Context())
		r.Begin(drain.CauseSignal)
		r.closeSettleWindow()

		out := r.completed(drain.CauseSignal)
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v, want nil", out.Abandoned)
		}
	})

	t.Run("every hold released before the cut", func(t *testing.T) {
		t.Parallel()

		r := newRig(t)
		release, ok := r.Acquire(reasonTurn)
		if !ok {
			t.Fatal("Acquire refused before the drain began, want granted")
		}
		r.start(t.Context())
		r.Begin(drain.CauseSignal)
		release()

		r.closeSettleWindow()
		r.completed(drain.CauseSignal)
	})
}

// TestWaitProceedsImmediatelyIfBeginCalledFirst pins the half of Begin's
// contract that Wait implements: Begin never runs the sequence on its caller's
// goroutine, so the sequence has to be waiting inside Wait — and a Wait entered
// after the fact must pick it up rather than block for a second Begin that is
// never coming.
func TestWaitProceedsImmediatelyIfBeginCalledFirst(t *testing.T) {
	t.Parallel()

	quiesced := make(chan drain.Cause, 1)
	r := newRig(t, drain.WithOnQuiesce(func(c drain.Cause) { quiesced <- c }))

	r.Begin(drain.CauseSignal)
	if got := r.clock.Armed(); got != 0 {
		t.Fatalf("Armed() = %d after Begin alone, want 0: the sequence runs inside Wait", got)
	}

	r.start(t.Context())

	// No second Begin: the sequence must reach quiesce and arm the settle timer
	// on the strength of the Begin that already happened.
	if got := <-quiesced; got != drain.CauseSignal {
		t.Errorf("OnQuiesce saw cause %v, want %v", got, drain.CauseSignal)
	}
	r.closeSettleWindow()
	r.completed(drain.CauseSignal)
}

// TestWaitIsSingleCall covers both shapes of a second call. A coordinator runs
// its sequence once and is permanently spent afterwards, so the second caller
// gets ErrAlreadyRunning and a zero Outcome — never a second sequence, and never
// a share of the first one's telemetry.
func TestWaitIsSingleCall(t *testing.T) {
	t.Parallel()

	assertRefused := func(t *testing.T, out drain.Outcome, err error) {
		t.Helper()
		if !errors.Is(err, drain.ErrAlreadyRunning) {
			t.Errorf("second Wait() = _, %v, want ErrAlreadyRunning", err)
		}
		// DeepEqual rather than ==: Outcome carries maps and is not comparable.
		if !reflect.DeepEqual(out, drain.Outcome{}) {
			t.Errorf("second Wait() = %+v, want the zero Outcome", out)
		}
	}

	t.Run("concurrent with a running Wait", func(t *testing.T) {
		t.Parallel()

		r := newRig(t)
		r.start(t.Context())
		r.Begin(drain.CauseSignal)

		// The armed settle timer proves the first Wait has claimed the
		// coordinator; without it the second call could win the claim and the
		// first would be the one refused.
		r.reach(phaseSettle)

		out, err := r.Wait(t.Context())
		assertRefused(t, out, err)

		r.closeSettleWindow()
		r.completed(drain.CauseSignal)
	})

	t.Run("after the first Wait returned", func(t *testing.T) {
		t.Parallel()

		r := newRig(t)
		r.start(t.Context())
		r.Begin(drain.CauseSignal)
		r.closeSettleWindow()
		r.completed(drain.CauseSignal)

		for range 2 {
			out, err := r.Wait(t.Context())
			assertRefused(t, out, err)
		}
	})
}

// TestSpentCoordinatorRefusesAcquire pins the other half of "permanently spent".
// Admission is a one-way latch: nothing reopens it, and a release handed out
// before the drain still decrements afterwards so a final metrics scrape stays
// truthful.
func TestSpentCoordinatorRefusesAcquire(t *testing.T) {
	t.Parallel()

	r := newRig(t)
	survivor, ok := r.Acquire(reasonGate)
	if !ok {
		t.Fatal("Acquire refused before the drain began, want granted")
	}

	r.start(t.Context())
	r.Begin(drain.CauseSignal)
	r.closeSettleWindow()
	survivor()
	r.completed(drain.CauseSignal)

	for _, reason := range []Reason{reasonTurn, reasonGate, drain.ReasonUnspecified} {
		if r.grants(reason) {
			t.Errorf("Acquire(%q) granted on a spent coordinator, want refused", reason)
		}
	}
	if got := r.Holds(); got != nil {
		t.Errorf("Holds() = %v on a spent coordinator, want nil: a refused Acquire must not count", got)
	}
	if !r.Draining() {
		t.Error("Draining() = false on a spent coordinator, want true")
	}
}

// TestOutcomeDurationSpansBeginToReturn pins the epoch of the one number that
// says how long the process took to die. It is measured from Begin, not from
// Wait entry: a signal that arrives while the process is still starting up has
// already spent part of the platform's kill budget by the time Wait sees it.
func TestOutcomeDurationSpansBeginToReturn(t *testing.T) {
	t.Parallel()

	t.Run("Begin inside Wait", func(t *testing.T) {
		t.Parallel()

		r := newRig(t)
		r.start(t.Context())
		r.Begin(drain.CauseSignal)
		r.closeSettleWindow()

		if got := r.completed(drain.CauseSignal).Duration; got != testSettle {
			t.Errorf("Outcome.Duration = %v, want %v", got, testSettle)
		}
	})

	t.Run("Begin before Wait entry", func(t *testing.T) {
		t.Parallel()

		const elapsed = 2 * time.Second

		r := newRig(t)
		r.Begin(drain.CauseSignal)
		// Time passes between Begin and Wait entry. Nothing is armed yet, so no
		// ordering is needed for this advance.
		r.clock.Advance(elapsed)
		r.start(t.Context())

		// The settle window is measured from Begin too, so only its remainder
		// is left to run.
		r.advance(phaseSettle, testSettle-elapsed)

		if got := r.completed(drain.CauseSignal).Duration; got != testSettle {
			t.Errorf("Outcome.Duration = %v, want %v — the epoch is Begin, not Wait entry", got, testSettle)
		}
	})
}

// TestZeroSettleWindowCutsWithoutArmingATimer is the plan's third rule applied
// to phase 2: a non-positive duration never arms a timer, because draintest
// fires such a timer only on an explicit Advance and the sequence has no reason
// to make one. Getting this wrong hangs rather than fails, which is why it has
// its own case rather than riding on a settle-window test.
func TestZeroSettleWindowCutsWithoutArmingATimer(t *testing.T) {
	t.Parallel()

	r := newRigWithSettle(t, 0)
	r.start(t.Context())
	r.Begin(drain.CauseSignal)

	// No advance: with no window to wait out, the sequence must reach the cut
	// and return on its own.
	out := r.completed(drain.CauseSignal)
	if out.Duration != 0 {
		t.Errorf("Outcome.Duration = %v, want 0", out.Duration)
	}
	r.allStopped("a zero settle window must not arm a timer, and the ceiling is stopped on the way out")
	if r.grants(reasonTurn) {
		t.Error("Acquire granted after a zero-length settle window, want refused")
	}
}

// TestSettleWindowAlreadyElapsedAtWaitEntry is the OTHER half of the plan's
// third rule, and it is a different bug from the one above rather than a second
// phrasing of it.
//
// TestZeroSettleWindowCutsWithoutArmingATimer covers a window configured at
// zero. This covers a window configured positive whose remainder is already
// spent, because Begin preceded Wait entry by more than the window — a signal
// that arrived while the process was still starting up. The remainder is then
// NEGATIVE, and a guard written against the policy (preDrainDelay <= 0) instead
// of against the remainder (remaining <= 0) passes the zero case and arms a
// timer for a deadline in the past here. The fake clock fires such a timer only
// on an explicit Advance, which the sequence has no reason to make, so the
// mutation hangs rather than fails — inside the bubble, immediately and with a
// goroutine dump.
func TestSettleWindowAlreadyElapsedAtWaitEntry(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const elapsed = testSettle + 2*time.Second

		r := newRig(t)
		r.Begin(drain.CauseSignal)
		// Nothing is armed before Wait entry, so this advance needs no ordering.
		r.clock.Advance(elapsed)
		r.start(ctx)

		// No further advance: the window is spent, so the sequence must cut and
		// return on its own.
		out := r.completed(drain.CauseSignal)
		r.allStopped("a settle window already spent at Wait entry must not arm a timer")
		if out.Duration != elapsed {
			t.Errorf("Outcome.Duration = %v, want %v", out.Duration, elapsed)
		}
		if r.grants(reasonTurn) {
			t.Error("Acquire granted after a spent settle window, want refused")
		}
	})
}

// TestWaitBlocksUntilOutstandingHoldsRelease is the positive statement of phase
// 4: a hold that was admitted before the cutoff is owed, and the sequence does
// not end while it is outstanding.
//
// It runs inside a bubble because the claim is that Wait has NOT returned, and
// proving an absence needs a happens-before edge rather than a hopeful
// non-blocking receive: an implementation that skipped the wait phase entirely
// passed ten consecutive runs of the naive version, because Wait's goroutine had
// not been scheduled yet at the instant the test looked. synctest.Wait supplies
// the edge — it returns only once every other goroutine in the bubble is durably
// blocked, so a Wait that was going to return has already returned by then. That
// is what rig.stillDraining wraps, and by now most of the suite is bubbled for
// the same reason.
func TestWaitBlocksUntilOutstandingHoldsRelease(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newRig(t)
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		r.reach(phaseSettle)
		release := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.stillDraining("a hold admitted during the settle window is owed")
		if got := r.Holds()[reasonTurn]; got != 1 {
			t.Errorf("Holds()[%q] = %d during the wait phase, want 1", reasonTurn, got)
		}

		release()
		r.completed(drain.CauseSignal)
	})
}
