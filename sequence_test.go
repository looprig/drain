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

// The policy every rig in this file is built with. The grace period is a
// ceiling nothing in Task 7 can reach — the ceiling itself is Task 10 — and the
// settle window is the only one that matters here.
const (
	testGrace  = 30 * time.Second
	testSettle = 5 * time.Second
)

// Reasons. Package-prefixed, per the vocabulary rule on Reason.
const (
	reasonTurn Reason = "drain_test/active_turn"
	reasonGate Reason = "drain_test/pending_gate"
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
	// phaseSettle is Wait inside the settle window: the settle timer is armed
	// and, until Task 10, nothing else is.
	phaseSettle phase = iota
)

// waitResult is one return from Wait, carried off the goroutine that made it.
type waitResult struct {
	out drain.Outcome
	err error
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
	settle time.Duration
	done   chan waitResult

	*drain.Coordinator
}

// newRig builds a coordinator on a deterministic clock with the standard settle
// window.
func newRig(t *testing.T, opts ...drain.Option) *rig {
	t.Helper()
	return newRigWithSettle(t, testSettle, opts...)
}

// newRigWithSettle is newRig with a chosen settle window. The window is a rig
// parameter rather than one of opts on purpose: the rig can only own the timer
// count if it knows the policy that produces the timers, so a WithPreDrainDelay
// passed through opts would silently desynchronize armedIn from the coordinator.
func newRigWithSettle(t *testing.T, settle time.Duration, opts ...drain.Option) *rig {
	t.Helper()

	clock := draintest.NewClock(epoch)
	all := make([]drain.Option, 0, len(opts)+2)
	all = append(all, drain.WithClock(clock), drain.WithPreDrainDelay(settle))
	all = append(all, opts...)

	c, err := drain.New(testGrace, all...)
	if err != nil {
		t.Fatalf("New(%v) = _, %v, want a valid coordinator", testGrace, err)
	}
	return &rig{t: t, clock: clock, settle: settle, done: make(chan waitResult, 1), Coordinator: c}
}

// armedIn reports how many timers the coordinator has armed once it has reached
// p. This is the single place in the suite that knows that number, and it is
// deliberately a function of the rig's policy rather than a literal.
//
// BlockUntil counts every timer on the clock, not the one a test is waiting for.
// A test that hard-coded 1 would return the moment the coordinator armed any
// timer and then advance while a second one was still unarmed — leaving that
// second timer waiting on a deadline the clock has already passed, so the test
// hangs to the binary's -timeout instead of failing. Later tasks add their
// timers here and nowhere else: the grace ceiling (Task 10), the per-reason
// deadline timers (Task 8), the idle timer (Task 11).
func (r *rig) armedIn(p phase) int {
	n := 0
	// The settle timer. A non-positive window arms nothing at all — the
	// sequence dispatches straight to the cut rather than arming a timer the
	// fake clock would never fire — so the count is zero there.
	if p == phaseSettle && r.settle > 0 {
		n++
	}
	return n
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
func (r *rig) reach(p phase) {
	r.t.Helper()
	r.clock.BlockUntil(r.armedIn(p))
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
	r.advance(phaseSettle, r.settle)
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

// result blocks for Wait's return. It has no timeout of its own, following
// draintest.BlockUntil: the failure it leaves is a -timeout goroutine dump
// naming the phase that never completed, which says more than a deadline error
// would.
func (r *rig) result() waitResult {
	r.t.Helper()
	return <-r.done
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

	// The callback here never returns. An implementation that ran OnQuiesce on
	// Wait's own goroutine — or that waited for it before returning — fails this
	// as a hang, which is what Task 10's ceiling would otherwise have no way to
	// bound.
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
	if got := r.clock.Armed(); got != 0 {
		t.Errorf("Armed() = %d, want 0: a zero settle window must not arm a timer", got)
	}
	if r.grants(reasonTurn) {
		t.Error("Acquire granted after a zero-length settle window, want refused")
	}
}

// TestWaitBlocksUntilOutstandingHoldsRelease is the positive statement of phase
// 4: a hold that was admitted before the cutoff is owed, and the sequence does
// not end while it is outstanding.
//
// It is the one test in this suite that runs inside a testing/synctest bubble,
// and it needs to be. The claim is that Wait has NOT returned, and proving an
// absence needs a happens-before edge rather than a hopeful non-blocking
// receive: an implementation that skipped the wait phase entirely passed ten
// consecutive runs of the naive version, because Wait's goroutine had not been
// scheduled yet at the instant the test looked. synctest.Wait supplies the edge
// — it returns only once every other goroutine in the bubble is durably blocked,
// so a Wait that was going to return has already returned by then.
//
// draintest's own doc comment names synctest as the alternative available to
// tests inside drain. It is not this suite's default mechanism for the reason
// given there: draintest is exported for drain's consumers, and requiring them
// to run their shutdown wiring inside a bubble is a constraint the module has no
// standing to impose.
func TestWaitBlocksUntilOutstandingHoldsRelease(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Derived inside the bubble on purpose: a context created outside it
		// has a Done channel the bubble does not own, and a goroutine selecting
		// on one is not durably blocked, so synctest.Wait would never return.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r := newRig(t)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		r.reach(phaseSettle)
		release, ok := r.Acquire(reasonTurn)
		if !ok {
			t.Fatal("Acquire refused inside the settle window of a signal drain, want granted")
		}
		r.closeSettleWindow()

		// Every other goroutine — Wait's included — is now durably blocked.
		synctest.Wait()
		select {
		case res := <-r.done:
			t.Fatalf("Wait() returned %+v while a hold admitted during settle was still outstanding", res)
		default:
		}
		if got := r.Holds()[reasonTurn]; got != 1 {
			t.Errorf("Holds()[%q] = %d during the wait phase, want 1", reasonTurn, got)
		}

		release()
		r.completed(drain.CauseSignal)
	})
}
