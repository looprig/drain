package drain_test

// The grace ceiling: a hard bound over the WHOLE sequence, phases 1 to 6,
// measured from Begin and including time spent inside user callbacks.
//
// It exists because SIGKILL does not negotiate. Every other mechanism in this
// module is a policy the owner can influence — a deadline it configured, a
// callback it wrote, a hold it forgot to release — and each of them is a way for
// the process to still be here when the platform's countdown reaches zero. The
// ceiling is the one thing in the sequence that is not up for discussion, which
// is why the abandon phase cannot be allowed to run on Wait's own goroutine:
// user code that ran there could hang the mechanism meant to bound it.
//
// So the exit condition is "every fired OnDeadline has returned AND the total
// reached zero, or grace expired — whichever is first", and the tests here are
// the ones that separate the two halves of it. Note what is deliberately absent
// from that condition: OnQuiesce. It is covered by the ceiling and it is not
// waited for, and the difference between those is the subject of
// TestHungOnQuiesceDoesNotBlockCeiling.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/drain"
)

// hungCallback returns a callback body that never returns on its own, a channel
// closed as it is entered, and the release that lets it finish.
//
// The release must run as a defer INSIDE the bubble rather than as a t.Cleanup:
// a cleanup runs after synctest.Test has already waited for every goroutine in
// the bubble to exit, so a callback still parked at that point is a bubble that
// never ends — and a bubble deadlock panics the whole test binary, taking every
// other failure in the package with it. Production has no such obligation, which
// is the point being tested: there the goroutine is leaked deliberately, because
// the process is exiting and the ceiling has already done its job.
func hungCallback() (entered <-chan struct{}, body, release func()) {
	in, hold := make(chan struct{}), make(chan struct{})
	return in, func() {
		close(in)
		<-hold
	}, func() { close(hold) }
}

// TestHungOnDeadlineDoesNotBlockCeiling is the ceiling doing the job the
// goroutine ownership rule exists to make possible. The owner is told to abandon
// its work and never comes back; the hold it was told about releases anyway, so
// the total reaches zero — and the drain still must not end there, because a
// callback the coordinator started is still running on a goroutine the process
// is about to exit out from under.
//
// The state it lands in is the one the spec calls out explicitly: grace expires
// with nothing outstanding, so Wait returns a *GracePeriodExceededError whose
// Abandoned is nil. Nothing was abandoned. The budget was still spent.
func TestHungOnDeadlineDoesNotBlockCeiling(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const deadline = 10 * time.Second

		entered, body, release := hungCallback()
		defer release()

		r := newRig(t,
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithOnDeadline(func(context.Context, Reason, int) { body() }),
		)
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.advance(phaseWait, deadline-testSettle)
		<-entered

		// The work unwinds and the hold releases: the total is zero and the
		// drain still is not over. A coordinator whose exit condition was the
		// total alone returns here, twenty seconds early, while its own
		// callback goroutine is mid-flight.
		turn()
		r.stillDraining("an OnDeadline the coordinator started has not returned")
		if got := r.Holds(); got != nil {
			t.Errorf("Holds() = %v, want nil: the abandoned hold released", got)
		}

		r.clock.Advance(testGrace - deadline)
		out, overrun := r.exceeded(drain.CauseSignal)
		if out.Duration != testGrace {
			t.Errorf("Outcome.Duration = %v, want %v: the ceiling ends the wait", out.Duration, testGrace)
		}
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v, want nil: every hold released before the ceiling", out.Abandoned)
		}
		if overrun.Abandoned != nil {
			t.Errorf("GracePeriodExceededError.Abandoned = %v, want nil: grace can expire with "+
				"zero holds outstanding, because the exit condition is callbacks-returned AND total-zero",
				overrun.Abandoned)
		}
		r.allStopped("the ceiling and every deadline are stopped on the way out")
	})
}

// TestHungOnQuiesceDoesNotBlockCeiling is the regression test for an OnQuiesce
// run on Wait's own goroutine, and it is written with a hold that NEVER releases
// for a reason worth stating plainly.
//
// The spec says OnQuiesce completion is not an exit gate: a process with nothing
// outstanding exits promptly even if its OnQuiesce is wedged forever, because
// the callback's only job — failing a readiness probe — was made redundant by
// the admission cutoff before it could matter. So the zero-hold version of this
// test is vacuous. It passes against a coordinator that never waits on
// OnQuiesce and has no ceiling at all, which is precisely the implementation
// that would leave phase 1 uncovered.
//
// With a hold that never releases there is something for the ceiling to be the
// answer to. Wait returns at Begin+gracePeriod on the hold's account, with the
// quiesce callback still parked inside it — covered by the ceiling, not waited
// for by it.
func TestHungOnQuiesceDoesNotBlockCeiling(t *testing.T) {
	t.Parallel()

	t.Run("the wait ends at the ceiling, on the hold's account", func(t *testing.T) {
		t.Parallel()
		bubbled(t, hungQuiesceReachesTheCeiling)
	})

	// The other side of "covered by the ceiling is not waited for", and the one
	// the ceiling itself cannot show. Here the holds DO release, well inside the
	// budget, so a coordinator that treated OnQuiesce as an exit gate would look
	// correct in every ceiling test in this file and simply hold the process for
	// the remaining grace period — waiting on a callback whose only effect was
	// to stop traffic the admission cutoff is already refusing.
	t.Run("a wedged OnQuiesce does not extend a drain whose holds released", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			const worked = 2 * time.Second

			entered, body, release := hungCallback()
			defer release()

			r := newRig(t, drain.WithOnQuiesce(func(drain.Cause) { body() }))
			r.armsInWait(1)
			r.start(ctx)
			r.Begin(drain.CauseSignal)

			<-entered
			turn := r.mustAcquire(reasonTurn)
			r.closeSettleWindow()

			r.advance(phaseWait, worked)
			turn()

			out := r.completed(drain.CauseSignal)
			if want := testSettle + worked; out.Duration != want {
				t.Errorf("Outcome.Duration = %v, want %v: the wait ends at the last release, "+
					"not at the ceiling and not when OnQuiesce returns", out.Duration, want)
			}
		})
	})
}

// hungQuiesceReachesTheCeiling is the first subtest's body, lifted out only
// because bubbled takes exactly this shape.
func hungQuiesceReachesTheCeiling(t *testing.T, ctx context.Context) {
	entered, body, release := hungCallback()
	defer release()
	returned := make(chan struct{})

	r := newRig(t, drain.WithOnQuiesce(func(drain.Cause) {
		body()
		close(returned)
	}))
	r.armsInWait(1)
	r.start(ctx)
	r.Begin(drain.CauseSignal)

	<-entered
	gate := r.mustAcquire(reasonGate)
	defer gate()
	r.closeSettleWindow()

	r.advance(phaseWait, testGrace-testSettle)
	out, overrun := r.exceeded(drain.CauseSignal)

	select {
	case <-returned:
		t.Fatal("OnQuiesce returned; this test proves nothing unless it is still wedged")
	default:
	}
	if out.Duration != testGrace {
		t.Errorf("Outcome.Duration = %v, want %v", out.Duration, testGrace)
	}
	if got := overrun.Abandoned[reasonGate]; got != 1 {
		t.Errorf("Abandoned = %v, want %d hold of %q: the wait ended on the hold's account",
			overrun.Abandoned, 1, reasonGate)
	}
}

// TestCeilingCoversPhaseOne pins the arithmetic that the phrase "phases 1 to 6"
// stands for: the ceiling is armed before the first user callback is started and
// measured from Begin, so quiesce and the settle window spend it like everything
// else does.
//
// The plausible wrong implementation arms the ceiling where the waiting starts,
// at the settle boundary. It passes every hung-callback test in this file and
// gives the process gracePeriod + preDrainDelay, which is the number the
// platform is not counting down against.
func TestCeilingCoversPhaseOne(t *testing.T) {
	t.Parallel()

	// A grace period the sequence can actually reach with room to see the
	// settle window inside it rather than beside it.
	const (
		grace  = 10 * time.Second
		settle = 5 * time.Second
	)

	t.Run("the settle window spends the budget", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			r := newRigWithPolicy(t, rigPolicy{grace: grace, settle: settle})
			r.armsInWait(1)
			r.start(ctx)
			r.Begin(drain.CauseSignal)

			gate := r.mustAcquire(reasonGate)
			defer gate()
			r.closeSettleWindow()

			// Half the budget is already gone, spent on a window that protects
			// in-flight routing rather than on waiting for work.
			r.advance(phaseWait, grace-settle)
			out, _ := r.exceeded(drain.CauseSignal)
			if out.Duration != grace {
				t.Errorf("Outcome.Duration = %v, want %v: a ceiling armed at the settle boundary "+
					"instead of at quiesce would return at %v", out.Duration, grace, settle+grace)
			}
		})
	})

	// The same epoch seen from the other end. A signal that arrives while the
	// process is still starting up can spend the entire budget before Wait is
	// ever entered, and the remainder is then negative rather than zero — which
	// is the plan's third rule at the point where it applies to the ceiling: a
	// non-positive remainder is dispatched directly, because a fake clock fires
	// a timer armed for the past only on an Advance the sequence has no reason
	// to make. The wrong guard hangs here instead of failing.
	t.Run("already spent before Wait was entered", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			const late = grace + 2*time.Second

			r := newRigWithPolicy(t, rigPolicy{grace: grace, settle: settle})
			gate := r.mustAcquire(reasonGate)
			defer gate()

			r.Begin(drain.CauseSignal)
			// Nothing is armed before Wait entry, so this advance needs no
			// ordering of its own.
			r.clock.Advance(late)
			r.start(ctx)

			// No further advance: the budget is gone, so Wait must return on
			// its own.
			out, overrun := r.exceeded(drain.CauseSignal)
			if out.Duration != late {
				t.Errorf("Outcome.Duration = %v, want %v", out.Duration, late)
			}
			if got := overrun.Abandoned[reasonGate]; got != 1 {
				t.Errorf("Abandoned = %v, want %d hold of %q", overrun.Abandoned, 1, reasonGate)
			}
			r.allStopped("a ceiling already spent at Wait entry must not arm a timer")
		})
	})
}

// TestGracePeriodExceededReturnsValidOutcome is the whole point of returning an
// error and an Outcome together. A process that overran its budget is the one
// that most needs to say what it was waiting for, and it is about to be killed,
// so the telemetry cannot be conditional on a clean exit.
//
// It also pins keep-waiting-after-abandon against the ceiling: the report at the
// deadline does not end the wait, and the ceiling does.
func TestGracePeriodExceededReturnsValidOutcome(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const deadline = 10 * time.Second

		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithOnDeadline(rec.on),
		)
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		defer turn()
		r.closeSettleWindow()

		r.advance(phaseWait, deadline-testSettle)
		rec.next(reasonTurn, 1)
		r.stillDraining("a deadline is a report, not a verdict; the ceiling is the verdict")

		r.clock.Advance(testGrace - deadline)
		out, overrun := r.exceeded(drain.CauseSignal)

		rec.none("the reason was already told at its own deadline; the ceiling does not tell it twice")
		if out.Cause != drain.CauseSignal {
			t.Errorf("Outcome.Cause = %v, want %v", out.Cause, drain.CauseSignal)
		}
		if out.Duration != testGrace {
			t.Errorf("Outcome.Duration = %v, want %v", out.Duration, testGrace)
		}
		if got := out.Abandoned[reasonTurn]; got != 1 {
			t.Errorf("Outcome.Abandoned = %v, want %d hold of %q", out.Abandoned, 1, reasonTurn)
		}
		if overrun.Elapsed != testGrace || overrun.Budget != testGrace {
			t.Errorf("GracePeriodExceededError{Elapsed: %v, Budget: %v}, want both %v",
				overrun.Elapsed, overrun.Budget, testGrace)
		}
		r.allStopped("the ceiling and every deadline are stopped on the way out")
	})
}

// TestErrorAbandonedIsSameMapAsOutcome pins "one snapshot, two names" by
// IDENTITY rather than by equality. Two separate reads of the counters can be
// deep-equal on the run that a test happens to observe and disagree on the run
// that matters, because a release landing between them changes one and not the
// other — and the two numbers are then reported side by side to the same
// operator, from the same return.
//
// Mutating through one alias and observing the other is the only assertion that
// distinguishes the two implementations. It is also the documented licence: the
// coordinator keeps no reference after Wait returns, so a caller may edit the
// map it was handed.
func TestErrorAbandonedIsSameMapAsOutcome(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newRig(t)
		r.armsInWait(2)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		defer turn()
		gate := r.mustAcquire(reasonGate)
		defer gate()
		r.closeSettleWindow()

		r.advance(phaseWait, testGrace-testSettle)
		out, overrun := r.exceeded(drain.CauseSignal)

		if out.Abandoned == nil {
			t.Fatal("Outcome.Abandoned = nil, want two reasons outstanding at the ceiling")
		}

		// Through the Outcome, out the error.
		out.Abandoned[reasonTurn] = 99
		if got := overrun.Abandoned[reasonTurn]; got != 99 {
			t.Errorf("after Outcome.Abandoned[%q] = 99, error's Abandoned[%q] = %d, want 99: "+
				"the two must be one map, not two equal maps", reasonTurn, reasonTurn, got)
		}

		// And back the other way, including a key that never existed, which no
		// amount of copying could make appear in both.
		overrun.Abandoned[reasonProc] = 7
		if got := out.Abandoned[reasonProc]; got != 7 {
			t.Errorf("after error's Abandoned[%q] = 7, Outcome.Abandoned[%q] = %d, want 7",
				reasonProc, reasonProc, got)
		}
		delete(overrun.Abandoned, reasonGate)
		if _, ok := out.Abandoned[reasonGate]; ok {
			t.Errorf("Outcome.Abandoned still has %q after it was deleted through the error", reasonGate)
		}

		// The coordinator retains no reference, so none of that reached it.
		if got := r.Holds()[reasonTurn]; got != 1 {
			t.Errorf("Holds()[%q] = %d after mutating the snapshot, want 1: Holds returns a copy",
				reasonTurn, got)
		}
	})
}

// TestOnDeadlineFiresOnceWhenDeadlineEqualsCeiling covers the collision the
// unknown-key policy guarantees will happen. A reason nobody configured waits
// the full grace period, so its deadline timer and the ceiling are armed for the
// SAME INSTANT, and the ceiling's own sweep — which owes a report to every
// reason still outstanding that has not had one — arrives at the same reason
// from the other direction.
//
// Two reports would tell an owner to interrupt work it has already been told to
// interrupt, which for the abandon path is a real second action rather than a
// duplicated log line. Which of the two paths wins is not a property worth
// depending on; that exactly one of them does is.
func TestOnDeadlineFiresOnceWhenDeadlineEqualsCeiling(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		rec := newRecorder(t)
		r := newRig(t, drain.WithOnDeadline(rec.on))
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		// Unconfigured, so its deadline IS the grace period.
		turn := r.mustAcquire(reasonTurn)
		defer turn()
		r.closeSettleWindow()

		r.advance(phaseWait, testGrace-testSettle)
		out, _ := r.exceeded(drain.CauseSignal)

		rec.next(reasonTurn, 1)
		rec.none("the deadline timer and the ceiling sweep are the same instant, and one report is owed")
		if got := out.Abandoned[reasonTurn]; got != 1 {
			t.Errorf("Outcome.Abandoned = %v, want %d hold of %q", out.Abandoned, 1, reasonTurn)
		}
	})
}

// TestCallbackContextExpiresAtTheCeiling covers the ctx OnDeadline is handed. It
// is derived from Wait's, so a cancelled Wait cancels it, and its deadline is
// grace expiry, so an owner that asks how long it has to unwind gets the truth
// rather than "no deadline".
//
// The expiry is driven by the injected Clock rather than by context.WithDeadline
// — which is why the first subtest can assert a deadline in a fake time base at
// all, and why a consumer's deterministic test sees a callback ctx that expires
// when the simulated drain says it does instead of at some unrelated wall-clock
// instant.
func TestCallbackContextExpiresAtTheCeiling(t *testing.T) {
	t.Parallel()

	const deadline = 10 * time.Second

	t.Run("the deadline is grace expiry", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			seen := make(chan context.Context, 1)
			r := newRig(t,
				drain.WithDeadline(reasonTurn, deadline),
				drain.WithOnDeadline(func(cbctx context.Context, _ Reason, _ int) { seen <- cbctx }),
			)
			r.armsInWait(1)
			r.start(ctx)
			r.Begin(drain.CauseSignal)

			turn := r.mustAcquire(reasonTurn)
			defer turn()
			r.closeSettleWindow()

			r.advance(phaseWait, deadline-testSettle)
			cbctx := <-seen

			got, ok := cbctx.Deadline()
			if want := epoch.Add(testGrace); !ok || !got.Equal(want) {
				t.Errorf("callback ctx Deadline() = %v, %t, want %v, true", got, ok, want)
			}
			if err := cbctx.Err(); err != nil {
				t.Errorf("callback ctx Err() = %v at invocation, want nil: %v of the budget is left",
					err, testGrace-deadline)
			}

			r.clock.Advance(testGrace - deadline)
			r.exceeded(drain.CauseSignal)
			if err := cbctx.Err(); err == nil {
				t.Error("callback ctx Err() = nil after the ceiling expired, want non-nil")
			}

			// Err() cannot say WHY, and it never will on this context: the
			// ceiling is driven by the injected Clock, so the cancellation
			// cannot go through context.WithDeadline and Err() is
			// context.Canceled here exactly as it is when Wait's own ctx is
			// cancelled. A context that reports a Deadline invites the check
			// that then reads wrong — errors.Is(ctx.Err(), DeadlineExceeded) —
			// and the two endings need different responses from the owner, so
			// the discrimination is carried as the cancellation's cause.
			if cause := context.Cause(cbctx); !errors.Is(cause, context.DeadlineExceeded) {
				t.Errorf("context.Cause(callback ctx) = %v after the ceiling expired, want context.DeadlineExceeded: "+
					"the owner cannot tell an overrun from a cancelled Wait through Err() alone", cause)
			}
		})
	})

	// A derived context may never outlive its parent's deadline, so the shorter
	// of the two is what Deadline reports. This is the only test in the suite
	// that puts the two time bases side by side, which is what it is for: the
	// clock starts at the real now, so a wall-clock deadline shorter than the
	// grace period is expressible at all. No bubble, because the parent's timer
	// is a real one.
	t.Run("Wait's ctx wins when it expires first", func(t *testing.T) {
		t.Parallel()

		const budget = 3 * time.Second

		seen := make(chan context.Context, 1)
		r := newRigWithPolicy(t,
			rigPolicy{grace: testGrace, settle: testSettle, start: time.Now()},
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithOnDeadline(func(cbctx context.Context, _ Reason, _ int) { seen <- cbctx }),
		)
		r.armsInWait(1)

		// Real time, and shorter than the grace period. It never actually
		// fires: the fake clock reaches the reason's deadline first, in
		// microseconds.
		want := r.clock.Now().Add(budget)
		ctx, cancel := context.WithDeadline(context.Background(), want)
		defer cancel()

		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		defer turn()
		r.closeSettleWindow()

		r.advance(phaseWait, deadline-testSettle)

		got, ok := (<-seen).Deadline()
		if !ok || !got.Equal(want) {
			t.Errorf("callback ctx Deadline() = %v, %t, want %v, true: Wait's ctx expires %v before "+
				"the ceiling does", got, ok, want, testGrace-budget)
		}
	})
}
