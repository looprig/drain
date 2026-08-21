package drain_test

// Phase 4: which deadline applies to which reason, and from when. Every test
// here runs inside a synctest bubble through the rig's bubbled helper, because
// every claim it makes is about a timer that a fake clock will only fire if the
// coordinator armed it for the right instant — and the failure mode of getting
// that wrong is a wait for an Advance nobody makes. Inside the bubble that is an
// immediate failure with a goroutine dump; outside it, ten minutes of silence.
//
// Phase 5 is Task 9, so a deadline that passes is not yet observable through
// OnDeadline. What is observable, and is what these tests assert, is the
// schedule itself: how many timers the wait phase arms, and the instant each one
// comes due.

import (
	"context"
	"testing"
	"time"

	"github.com/looprig/drain"
)

// TestWaitsForOutstandingHolds is phase 4's baseline: every reason the cut named
// is owed, and the wait ends on the LAST release rather than the first. Both
// reasons here are unconfigured, so both are owed the full grace period and
// neither deadline is anywhere near.
func TestWaitsForOutstandingHolds(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newRig(t)
		r.armsInWait(2)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		gate := r.mustAcquire(reasonGate)
		r.closeSettleWindow()

		r.reach(phaseWait)
		if got := r.Holds(); len(got) != 2 {
			t.Fatalf("Holds() = %v in the wait phase, want both reasons outstanding", got)
		}

		turn()
		r.stillDraining("a hold of " + string(reasonGate) + " is still outstanding")

		gate()
		out := r.completed(drain.CauseSignal)
		if out.Duration != testSettle {
			t.Errorf("Outcome.Duration = %v, want %v: the wait ended at the last release", out.Duration, testSettle)
		}
	})
}

// TestUnconfiguredReasonGetsFullGracePeriod pins the unknown-key policy: the
// cautious default is the one you get by doing nothing. It is also the property
// that makes a misspelled WithDeadline survivable — the real reason keeps
// waiting instead of being abandoned at whatever the typo configured — and the
// one Outcome.Waited will expose in telemetry.
//
// The full grace period is measured from Begin like every other deadline, so the
// settle window has already spent part of it by the time the timer is armed.
func TestUnconfiguredReasonGetsFullGracePeriod(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newRig(t)
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		hold := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.advance(phaseWait, testGrace-testSettle-time.Nanosecond)
		r.armed(1, "an unconfigured reason waits the full grace period, not a shorter default")

		// The arm was ordered by the advance above and the wait phase arms
		// nothing further, so this last step needs no ordering of its own — and
		// could not be given one, since reaching the phase again would block on
		// a timer that has by then fired.
		r.clock.Advance(time.Nanosecond)
		r.armed(0, "the deadline of an unconfigured reason is the grace period, measured from Begin")

		hold()
		out := r.completed(drain.CauseSignal)
		if out.Duration != testGrace {
			t.Errorf("Outcome.Duration = %v, want %v", out.Duration, testGrace)
		}
	})
}

// TestDeadlineEpochIsBeginNotPhaseThree pins the first of the spec's two
// arithmetic claims. Deadlines are measured from Begin and overlap the settle
// window: a hold admitted three seconds into a five-second settle, under a
// ten-second deadline, has five seconds left in the wait phase, not ten.
//
// The budget belongs to the process, not to the request. That is what makes the
// ceiling arithmetic hold — a settle-relative epoch would let a hold admitted at
// the end of the window run past the grace period the platform is counting down
// against, however carefully New validated the deadline.
func TestDeadlineEpochIsBeginNotPhaseThree(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			admitted = 3 * time.Second
			deadline = 10 * time.Second
		)

		r := newRig(t, drain.WithDeadline(reasonTurn, deadline))
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		// Admitted mid-settle, which only a signal drain allows: admission stays
		// open until the boundary because the requests already routed here are
		// still arriving.
		r.advance(phaseSettle, admitted)
		hold := r.mustAcquire(reasonTurn)
		r.advance(phaseSettle, testSettle-admitted)

		r.advance(phaseWait, deadline-testSettle-time.Nanosecond)
		r.armed(1, "the deadline has not arrived yet")

		r.clock.Advance(time.Nanosecond)
		r.armed(0, "the deadline epoch is Begin, so it expires at Begin+"+deadline.String()+
			"; a settle-relative epoch would leave it armed until Begin+"+(testSettle+deadline).String())

		hold()
		out := r.completed(drain.CauseSignal)
		if out.Duration != deadline {
			t.Errorf("Outcome.Duration = %v, want %v", out.Duration, deadline)
		}
	})
}

// TestReasonsWaitConcurrentlyNotSerially pins the second arithmetic claim: the
// worst case across reasons is max, not sum. Three reasons at ten seconds each
// is a ten-second wait.
//
// Serial waiting is the plausible wrong implementation — a loop that waits out
// one reason's deadline before arming the next reads naturally and is thirty
// seconds here, which would blow through a Kubernetes grace period that New
// validated each deadline against individually.
func TestReasonsWaitConcurrentlyNotSerially(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const deadline = 10 * time.Second

		reasons := []Reason{reasonTurn, reasonGate, reasonProc}
		opts := make([]drain.Option, 0, len(reasons))
		for _, reason := range reasons {
			opts = append(opts, drain.WithDeadline(reason, deadline))
		}

		r := newRig(t, opts...)
		r.armsInWait(len(reasons))
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		releases := make([]func(), 0, len(reasons))
		for _, reason := range reasons {
			releases = append(releases, r.mustAcquire(reason))
		}
		r.closeSettleWindow()

		// Every deadline is armed at once. A serial implementation arms one,
		// which blocks the reach below rather than reaching this assertion.
		r.reach(phaseWait)
		r.armed(len(reasons), "every reason's deadline runs concurrently, so all are armed at wait-phase entry")

		r.clock.Advance(deadline - testSettle)
		r.armed(0, "all three deadlines are the same instant, so one advance expires all of them")

		for _, release := range releases {
			release()
		}
		out := r.completed(drain.CauseSignal)
		if out.Duration != deadline {
			t.Errorf("Outcome.Duration = %v, want %v: %d reasons at %v each is a %v wait, not %v",
				out.Duration, deadline, len(reasons), deadline, deadline, time.Duration(len(reasons))*deadline)
		}
	})
}

// TestWaitReturnsEarlyWhenAllHoldsRelease is the common case, and the one the
// grace period exists to make rare: the work finishes, the wait ends there, and
// no deadline is ever reached. It also pins that the armed deadlines are stopped
// on the way out rather than left to fire against a spent coordinator.
func TestWaitReturnsEarlyWhenAllHoldsRelease(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			deadline = 10 * time.Second
			worked   = 2 * time.Second
		)

		r := newRig(t, drain.WithDeadline(reasonTurn, deadline))
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		hold := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.advance(phaseWait, worked)
		hold()

		out := r.completed(drain.CauseSignal)
		if out.Duration != testSettle+worked {
			t.Errorf("Outcome.Duration = %v, want %v: the wait ends at the last release", out.Duration, testSettle+worked)
		}
		r.armed(0, "a deadline that never came due must be stopped when the wait ends")
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v, want nil: nothing outlived its deadline", out.Abandoned)
		}
	})
}

// TestSpentDeadlineDispatchesWithoutArmingATimer is the plan's third rule at the
// point where it stops being an edge case. A reason's remaining budget at
// wait-phase entry is deadline - elapsedSinceBegin, and it is routinely
// non-positive: a zero-deadline pending gate is spent before the sequence starts
// and any deadline shorter than the settle window is spent by the cut.
//
// Both subtests are here because they are different arithmetic, and the review
// of phase 2 proved the point — a guard mutated from the remainder to the
// configured policy kept the zero case passing while arming a timer for a
// negative duration in the other. draintest fires such a timer only on an
// explicit Advance, which the sequence has no reason to make, so the wrong guard
// hangs instead of failing.
func TestSpentDeadlineDispatchesWithoutArmingATimer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		deadline time.Duration
	}{
		// The pending-gate case: do not wait for a human. Signal immediately so
		// the owner can let the session be restored elsewhere later.
		{name: "configured zero", deadline: 0},
		// Positive, but shorter than the settle window it overlaps, so the
		// remainder at wait-phase entry is negative rather than zero.
		{name: "elapsed during settle", deadline: testSettle - 2*time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bubbled(t, func(t *testing.T, ctx context.Context) {
				r := newRig(t, drain.WithDeadline(reasonGate, tc.deadline))
				r.armsInWait(0)
				r.start(ctx)
				r.Begin(drain.CauseSignal)

				gate := r.mustAcquire(reasonGate)
				r.closeSettleWindow()

				// Nothing is armed and nothing will be, so the ordering edge
				// has to come from the bubble rather than from an arm.
				r.stillDraining("the hold is outstanding, and a passed deadline does not end the wait")
				r.armed(0, "a deadline already spent at wait-phase entry is dispatched directly, never armed")

				gate()
				out := r.completed(drain.CauseSignal)
				if out.Duration != testSettle {
					t.Errorf("Outcome.Duration = %v, want %v", out.Duration, testSettle)
				}
			})
		})
	}
}
