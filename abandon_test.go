package drain_test

// Phase 5: what happens when a deadline passes with holds still outstanding.
// drain reports it and keeps waiting — it never abandons anything itself,
// because it has no idea what the work is. Three properties carry the phase, and
// each has a plausible wrong implementation that this file exists to reject:
//
//   - Skip if the count reached zero. Do not tell an owner to interrupt work
//     that already finished.
//   - Keep waiting after the callback fires. Marking the holds abandoned and
//     moving on breaks interrupt-then-drain and makes Abandoned over-report work
//     the owner successfully abandoned.
//   - release must be callable from inside OnDeadline. That is the dominant real
//     shape — the owner interrupts work whose deferred release then runs on the
//     callback's own goroutine — and it deadlocks instantly if any callback is
//     invoked under the lock.
//
// Like deadline_test.go, every test runs inside a synctest bubble: the claims
// here are about callbacks that must NOT happen and about a Wait that must NOT
// return, and both need the happens-before edge that only the bubble supplies.

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/looprig/drain"
)

// deadlineCall is one OnDeadline invocation, carried off the coordinator-owned
// goroutine that made it.
type deadlineCall struct {
	reason      Reason
	outstanding int
}

// recorder collects OnDeadline invocations. Its channel is buffered well past
// any test's expectations so that a callback never blocks: an unexpected extra
// invocation must show up as a failed assertion, not as a wedged goroutine.
type recorder struct {
	t     *testing.T
	calls chan deadlineCall
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()
	return &recorder{t: t, calls: make(chan deadlineCall, 8)}
}

// on is the callback to hand to WithOnDeadline. The ctx is unused here; what it
// is derived from is the grace ceiling's business (Task 10).
func (rec *recorder) on(_ context.Context, r Reason, outstanding int) {
	rec.calls <- deadlineCall{reason: r, outstanding: outstanding}
}

// next blocks for the next invocation and asserts its reason and count.
func (rec *recorder) next(reason Reason, outstanding int) {
	rec.t.Helper()

	got := <-rec.calls
	if got.reason != reason || got.outstanding != outstanding {
		rec.t.Errorf("OnDeadline(%q, %d), want OnDeadline(%q, %d)",
			got.reason, got.outstanding, reason, outstanding)
	}
}

// took blocks for n invocations and returns them keyed by reason, failing if
// any reason is reported twice.
//
// It is next for the case where several reasons are reported at the SAME
// instant — the ceiling's sweep and the cancellation sweep both dispatch every
// outstanding reason at once, on a goroutine each, so the order the calls
// arrive in is a race by design and only the set of them is a property.
func (rec *recorder) took(n int) map[Reason]int {
	rec.t.Helper()

	got := make(map[Reason]int, n)
	for range n {
		call := <-rec.calls
		if _, duplicate := got[call.reason]; duplicate {
			rec.t.Errorf("OnDeadline fired twice for %q, want once per reason", call.reason)
		}
		got[call.reason] = call.outstanding
	}
	return got
}

// none asserts that no invocation has happened. It is only sound inside a
// bubble: synctest.Wait returns once every other goroutine is durably blocked,
// so a callback that was going to be dispatched already has been.
func (rec *recorder) none(why string) {
	rec.t.Helper()

	synctest.Wait()
	select {
	case call := <-rec.calls:
		rec.t.Fatalf("OnDeadline(%q, %d) fired, want no call at all: %s", call.reason, call.outstanding, why)
	default:
	}
}

// TestOnDeadlineFiresPerOverrunReason is the phase's baseline: one call per
// reason that outlived its own deadline, at that deadline and not at another's.
func TestOnDeadlineFiresPerOverrunReason(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			turnDeadline = 10 * time.Second
			gateDeadline = 20 * time.Second
		)

		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonTurn, turnDeadline),
			drain.WithDeadline(reasonGate, gateDeadline),
			drain.WithOnDeadline(rec.on),
		)
		r.armsInWait(2)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		gate := r.mustAcquire(reasonGate)
		r.closeSettleWindow()

		r.advance(phaseWait, turnDeadline-testSettle)
		rec.next(reasonTurn, 1)
		rec.none("the gate's deadline is another " + (gateDeadline - turnDeadline).String() + " away")
		r.armed(1, "only the reason that overran is spent; the other is still armed")

		r.clock.Advance(gateDeadline - turnDeadline)
		rec.next(reasonGate, 1)
		r.armed(0, "both deadlines have now passed")

		turn()
		gate()
		out := r.completed(drain.CauseSignal)
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v, want nil: both holds released before Wait returned", out.Abandoned)
		}
	})
}

// TestOnDeadlineSkippedIfCountReachedZero rejects the first wrong
// implementation. A deadline is a question about work still in flight, so when
// it expires the coordinator re-reads the count under the lock and skips the
// callback ENTIRELY — not a call with zero — if the work already finished.
// Telling an owner to interrupt a turn that has completed is worse than
// useless: the owner's abandon path is a real action, and OnDeadline is the only
// thing that triggers it.
//
// The second reason exists to hold the wait open. Without it the last release
// ends the drain, the timer is stopped on the way out, and the test would prove
// nothing about the re-check.
func TestOnDeadlineSkippedIfCountReachedZero(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			turnDeadline = 10 * time.Second
			finished     = 2 * time.Second
		)

		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonTurn, turnDeadline),
			drain.WithOnDeadline(rec.on),
		)
		r.armsInWait(2)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		gate := r.mustAcquire(reasonGate)
		r.closeSettleWindow()

		// The turn finishes with time to spare. The fake clock is what makes
		// the interleaving deterministic: this release lands strictly before the
		// expiry, so the re-check under the lock is guaranteed to see the zero
		// that a naive implementation would have missed by reading the cut's
		// snapshot instead.
		r.advance(phaseWait, finished)
		turn()

		r.clock.Advance(turnDeadline - testSettle - finished)
		rec.none("the turn's count reached zero before its deadline expired")

		gate()
		r.completed(drain.CauseSignal)
	})
}

// TestKeepsWaitingAfterOnDeadline rejects the second wrong implementation. The
// callback is a report, not a verdict: drain keeps waiting for those holds until
// they release or the ceiling expires.
//
// That is what makes interrupt-then-drain work — the owner needs the interrupted
// work to unwind, and unwinding takes time it can only have if the drain is
// still waiting — and it is what keeps Abandoned from over-reporting work the
// owner successfully abandoned.
func TestKeepsWaitingAfterOnDeadline(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			deadline  = 10 * time.Second
			unwinding = 3 * time.Second
		)

		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithOnDeadline(rec.on),
		)
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.advance(phaseWait, deadline-testSettle)
		rec.next(reasonTurn, 1)
		r.stillDraining("a deadline is a report, not a verdict: the hold is still outstanding")

		// Time passes while the owner unwinds the interrupted work.
		r.clock.Advance(unwinding)
		r.stillDraining("drain waits for an abandoned hold until it releases or the ceiling expires")

		turn()
		out := r.completed(drain.CauseSignal)
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v, want nil: the hold released before Wait returned, so nothing was abandoned", out.Abandoned)
		}
		if want := deadline + unwinding; out.Duration != want {
			t.Errorf("Outcome.Duration = %v, want %v", out.Duration, want)
		}
	})
}

// TestNeverReleasedHoldAppearsInAbandoned is the other side of keep-waiting: a
// hold that never releases is reported, so the two together make Abandoned mean
// exactly "still outstanding when the process gave up".
//
// It is also the documented pending-gate signature. A session blocked on a human
// gets a zero deadline — do not wait for a person, signal immediately so the
// session can be restored elsewhere — so its deadline is spent before the
// sequence starts and it appears in Abandoned on every drain by construction.
//
// The grace ceiling that would otherwise end this drain is Task 10, so the exit
// here is cancellation. That branch of Wait predates this task and had no
// coverage of its own; it is exercised rather than assumed.
func TestNeverReleasedHoldAppearsInAbandoned(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, parent context.Context) {
		ctx, cancel := context.WithCancel(parent)
		defer cancel()

		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonGate, 0),
			drain.WithOnDeadline(rec.on),
		)
		r.armsInWait(0)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		// Acquired and never released: the human never answers.
		r.mustAcquire(reasonGate)
		r.closeSettleWindow()

		rec.next(reasonGate, 1)
		r.stillDraining("a zero deadline abandons the hold, it does not end the wait")
		r.armed(0, "a zero deadline is dispatched directly, never armed")

		cancel()
		res := r.result()
		if !errors.Is(res.err, context.Canceled) {
			t.Errorf("Wait() = _, %v, want context.Canceled", res.err)
		}
		if res.out.Cause != drain.CauseSignal {
			t.Errorf("Outcome.Cause = %v, want %v: cancellation does not overwrite the first cause",
				res.out.Cause, drain.CauseSignal)
		}
		if got := res.out.Abandoned[reasonGate]; got != 1 {
			t.Errorf("Outcome.Abandoned = %v, want %d hold of %q: a hold outstanding at Wait's return is abandoned",
				res.out.Abandoned, 1, reasonGate)
		}
	})
}

// TestReleaseFromInsideOnDeadline is the dominant real shape of abandonment and
// the reason no user callback may ever run under the lock: the owner interrupts
// the work, and the work's own deferred release() then runs on the callback's
// goroutine, before the callback has returned.
//
// A coordinator that invoked OnDeadline under c.mu deadlocks here on the spot,
// with releaseHold waiting for a lock its own caller holds. Nothing else in the
// suite would catch it: every other callback path is fine under a lock.
func TestReleaseFromInsideOnDeadline(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const deadline = 10 * time.Second

		// The release travels by channel rather than by closure capture so that
		// the handoff to the callback's goroutine carries a happens-before edge
		// -race can see.
		releases := make(chan func(), 1)
		released := make(chan Reason, 1)
		onDeadline := func(_ context.Context, r Reason, _ int) {
			(<-releases)()
			released <- r
		}

		r := newRig(t,
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithOnDeadline(onDeadline),
		)
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		releases <- r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.advance(phaseWait, deadline-testSettle)
		if got := <-released; got != reasonTurn {
			t.Errorf("OnDeadline released %q, want %q", got, reasonTurn)
		}

		out := r.completed(drain.CauseSignal)
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v, want nil: a hold released from inside OnDeadline is not abandoned", out.Abandoned)
		}
	})
}

// TestOnDeadlineCountIsAdvisorySnapshot pins where the count comes from: a read
// under the lock at invocation, not the snapshot phase 3 took at the cut. The
// two differ by every release in between, and the cut's number is the plausible
// wrong one — it is already in hand when the deadline is armed.
//
// The value is advisory by contract, since a release can land while the callback
// is being dispatched. Advisory means "may be stale by the time you read it",
// not "may be a different number than the coordinator ever held".
func TestOnDeadlineCountIsAdvisorySnapshot(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			deadline = 10 * time.Second
			finished = 2 * time.Second
		)

		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithOnDeadline(rec.on),
		)
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		// Two holds on one reason: two counts and two independent releases.
		first := r.mustAcquire(reasonTurn)
		second := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.advance(phaseWait, finished)
		first()

		r.clock.Advance(deadline - testSettle - finished)
		rec.next(reasonTurn, 1)

		second()
		r.completed(drain.CauseSignal)
	})
}

// TestOnDeadlineRunsConcurrentlyForDifferentReasons pins the goroutine contract:
// one goroutine per reason, so a slow owner does not delay another reason's
// notification or the drain around it.
//
// Each callback blocks until the other has been entered, which a serial
// dispatcher cannot satisfy: the first would wait for a second that is queued
// behind it. Inside the bubble that is an immediate deadlock failure naming both
// goroutines rather than a hang.
func TestOnDeadlineRunsConcurrentlyForDifferentReasons(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const deadline = 10 * time.Second

		entered := make(chan Reason, 2)
		proceed := make(chan struct{})
		onDeadline := func(_ context.Context, r Reason, _ int) {
			entered <- r
			<-proceed
		}

		r := newRig(t,
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithDeadline(reasonGate, deadline),
			drain.WithOnDeadline(onDeadline),
		)
		r.armsInWait(2)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		gate := r.mustAcquire(reasonGate)
		r.closeSettleWindow()

		r.advance(phaseWait, deadline-testSettle)

		// Both callbacks are in flight at once, each holding the other's exit.
		if got, want := []Reason{<-entered, <-entered}, []Reason{reasonGate, reasonTurn}; !sameReasons(got, want) {
			t.Errorf("OnDeadline entered for %v, want one call each for %v", got, want)
		}
		close(proceed)

		turn()
		gate()
		r.completed(drain.CauseSignal)
	})
}

// sameReasons reports whether two reason sets match regardless of order. The
// callbacks race by design, so the order they are entered in is not a property
// worth asserting.
func sameReasons(got, want []Reason) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[Reason]int, len(want))
	for _, r := range want {
		counts[r]++
	}
	for _, r := range got {
		counts[r]--
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}
