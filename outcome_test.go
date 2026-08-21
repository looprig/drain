package drain_test

// Outcome's two maps: what the drain waited with, and what it walked away from.
//
// Waited exists for one failure mode, and it is worth stating before the tests
// that pin it. Reason is an open set of strings, so nothing checks that the
// reason a caller configured is a reason the process actually uses:
// WithDeadline("harness/pending_gaet", 0) leaves the real pending_gate at the
// full grace period, which is a drain that waits hours on a human — the exact
// failure the zero deadline was written to prevent. No constructor error is
// possible, because the typo is a perfectly good reason nobody happens to hold.
// Waited reports the deadline actually applied to every reason the drain saw
// outstanding, so the misconfiguration shows up in telemetry on the first drain
// instead of never.
//
// Both maps follow the same single-representation rule as Holds: nil when
// empty, never an empty non-nil map, so m == nil and len(m) == 0 can never
// disagree for a caller.
//
// One assertion about Waited cannot be made from out here — that the map is
// freshly allocated rather than the coordinator's own configuration with extra
// keys added — because the corruption it prevents is invisible through any
// exported surface. It lives in waited_test.go, inside the package.

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/looprig/drain"
)

// TestWaitedRecordsAppliedDeadlinePerReason is the baseline: the policy that
// governed each reason the drain waited for, reported whether or not that
// reason was ever abandoned. A clean drain has telemetry too.
func TestWaitedRecordsAppliedDeadlinePerReason(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			turnDeadline = 10 * time.Second
			gateDeadline = 20 * time.Second
		)

		r := newRig(t,
			drain.WithDeadline(reasonTurn, turnDeadline),
			drain.WithDeadline(reasonGate, gateDeadline),
		)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		gate := r.mustAcquire(reasonGate)
		r.closeSettleWindow()

		turn()
		gate()
		out := r.completed(drain.CauseSignal)

		want := map[Reason]time.Duration{reasonTurn: turnDeadline, reasonGate: gateDeadline}
		if !maps.Equal(out.Waited, want) {
			t.Errorf("Outcome.Waited = %v, want %v", out.Waited, want)
		}
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v, want nil: both holds released inside their deadlines", out.Abandoned)
		}
	})
}

// TestWaitedReportsGracePeriodForUnconfiguredReason is the typo detector, and
// it is written as the typo rather than as an abstract missing key: the drain is
// configured to abandon pending gates immediately, the option names a reason
// that does not exist, and the reason that does exist quietly inherits the full
// thirty-second grace period.
//
// Nothing else in the module can report this. New cannot reject the option —
// "harness/pending_gaet" is a valid reason with a valid deadline — and the
// drain itself behaves exactly as configured. Waited is the only place the
// discrepancy surfaces.
func TestWaitedReportsGracePeriodForUnconfiguredReason(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		// The reason the operator meant to configure is reasonGate. This is not.
		const misspelled Reason = "drain_test/pending_gaet"

		r := newRig(t, drain.WithDeadline(misspelled, 0))
		r.armsInWait(1)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		gate := r.mustAcquire(reasonGate)
		defer gate()
		r.closeSettleWindow()

		// The wait phase proves the harm before Waited reports it: a gate that
		// was meant to be abandoned at once is still being waited for at the
		// twenty-second mark, and would be for another ten.
		r.advance(phaseWait, testGrace-testSettle-10*time.Second)
		r.stillDraining("the misspelled option left the real gate at the full grace period")

		r.clock.Advance(10 * time.Second)
		out, _ := r.exceeded(drain.CauseSignal)

		if got := out.Waited[reasonGate]; got != testGrace {
			t.Errorf("Outcome.Waited[%q] = %v, want %v: an unconfigured reason waits the full grace period, "+
				"and reporting that is how a misspelled WithDeadline becomes visible", reasonGate, got, testGrace)
		}
		if _, ok := out.Waited[misspelled]; ok {
			t.Errorf("Outcome.Waited = %v, want no entry for %q: nothing was ever held under it",
				out.Waited, misspelled)
		}
	})
}

// TestWaitedOnlyIncludesReasonsSeenOutstanding scopes the map to the reasons
// the drain actually waited for. A configured reason nobody held has no entry,
// which is what makes the previous test's assertion mean something: a Waited
// built from the configuration would list every deadline whether or not it
// applied to anything, and the reason with the typo would sit in the telemetry
// looking exactly as legitimate as the rest.
func TestWaitedOnlyIncludesReasonsSeenOutstanding(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const (
			turnDeadline = 10 * time.Second
			procDeadline = 15 * time.Second
		)

		r := newRig(t,
			drain.WithDeadline(reasonTurn, turnDeadline),
			drain.WithDeadline(reasonProc, procDeadline),
		)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		// Only one of the two configured reasons is ever held, and one
		// unconfigured reason is held and released before the cut.
		r.mustAcquire(reasonGate)()
		turn := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		turn()
		out := r.completed(drain.CauseSignal)

		want := map[Reason]time.Duration{reasonTurn: turnDeadline}
		if !maps.Equal(out.Waited, want) {
			t.Errorf("Outcome.Waited = %v, want %v: only reasons outstanding at the admission cutoff, "+
				"neither a configured reason nobody held nor one that finished before the cut",
				out.Waited, want)
		}
	})
}

// TestAbandonedOmitsZeroCounts pins the other map's filter. A reason that was
// told to abandon and then finished unwinding is not abandoned work — that is
// interrupt-then-drain succeeding, and reporting it would make the number
// consumers alert on count the successes as well as the failures.
func TestAbandonedOmitsZeroCounts(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const deadline = 10 * time.Second

		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonTurn, deadline),
			drain.WithOnDeadline(rec.on),
		)
		r.armsInWait(2)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		gate := r.mustAcquire(reasonGate)
		defer gate()
		r.closeSettleWindow()

		// The turn is told to abandon and does; the gate never releases.
		r.advance(phaseWait, deadline-testSettle)
		rec.next(reasonTurn, 1)
		turn()

		r.clock.Advance(testGrace - deadline)
		out, overrun := r.exceeded(drain.CauseSignal)

		want := map[Reason]int{reasonGate: 1}
		if !maps.Equal(out.Abandoned, want) {
			t.Errorf("Outcome.Abandoned = %v, want %v: the interrupted turn unwound in time", out.Abandoned, want)
		}
		if _, ok := out.Abandoned[reasonTurn]; ok {
			t.Errorf("Outcome.Abandoned has an entry for %q, want it omitted rather than reported as zero", reasonTurn)
		}
		if !maps.Equal(overrun.Abandoned, want) {
			t.Errorf("GracePeriodExceededError.Abandoned = %v, want %v", overrun.Abandoned, want)
		}
	})
}

// TestAbandonedSnapshottedOnceAtReturn pins WHEN the counters are read, from
// both sides of Wait's return.
//
// The map is a snapshot, not a live view: a hold released after Wait returned
// still decrements, so Holds moves on and the Outcome does not. And the
// snapshot is taken BEFORE phase 5's sweep dispatches, because the expected
// shape of abandonment is an owner whose OnDeadline interrupts work whose
// deferred release then runs on the callback's own goroutine. Reading the
// counters after dispatching would make Abandoned depend on whether that
// goroutine got scheduled first — two numbers for one drain, reported to the
// same operator in the same return.
func TestAbandonedSnapshottedOnceAtReturn(t *testing.T) {
	t.Parallel()

	t.Run("a release after Wait returns does not change it", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			r := newRig(t)
			r.armsInWait(1)
			r.start(ctx)
			r.Begin(drain.CauseSignal)

			gate := r.mustAcquire(reasonGate)
			r.closeSettleWindow()

			r.advance(phaseWait, testGrace-testSettle)
			out, _ := r.exceeded(drain.CauseSignal)

			want := map[Reason]int{reasonGate: 1}
			if !maps.Equal(out.Abandoned, want) {
				t.Fatalf("Outcome.Abandoned = %v, want %v", out.Abandoned, want)
			}

			// The release is still valid and still decrements — Holds stays
			// truthful for a final metrics scrape — and the Outcome is a record
			// of the drain, not of the counters afterwards.
			gate()
			if got := r.Holds(); got != nil {
				t.Errorf("Holds() = %v after the release, want nil", got)
			}
			if !maps.Equal(out.Abandoned, want) {
				t.Errorf("Outcome.Abandoned = %v after a post-return release, want %v: it is a snapshot",
					out.Abandoned, want)
			}
		})
	})

	// The sweep on the cancellation path is the one place the ordering is
	// observable on its own: the wait phase's deadline timers are stopped on
	// the way out, so the sweep is the only thing dispatching, and a callback
	// that releases its hold is racing nothing but this snapshot. Taken first,
	// Abandoned is exactly what the sweep was told; taken after the dispatch,
	// it is a coin toss between 1 and absent.
	t.Run("it is taken before the abandon sweep dispatches", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			released := make(chan struct{})
			var gate func()

			r := newRig(t, drain.WithOnDeadline(func(context.Context, Reason, int) {
				// The dominant real shape: the owner interrupts the work and
				// its deferred release runs here, on the callback's goroutine.
				gate()
				close(released)
			}))
			r.start(ctx)
			r.Begin(drain.CauseSignal)

			gate = r.mustAcquire(reasonGate)
			r.reach(phaseSettle)

			cancel()
			out := r.cancelled(drain.CauseSignal)

			<-released
			want := map[Reason]int{reasonGate: 1}
			if !maps.Equal(out.Abandoned, want) {
				t.Errorf("Outcome.Abandoned = %v, want %v: the snapshot is what the sweep was told about, "+
					"not what the counters happened to say once its callbacks had run", out.Abandoned, want)
			}
			if got := r.Holds(); got != nil {
				t.Errorf("Holds() = %v, want nil: the callback released the hold", got)
			}
		})
	})
}

// TestCompletedDrainEmptyMapsAreNil pins the single-representation rule. A
// process that drained with nothing in flight gets nil for both maps, never an
// empty non-nil one, so a consumer's `if out.Abandoned == nil` and its
// `if len(out.Abandoned) == 0` can never disagree — and neither can two
// serializations of the same Outcome.
func TestCompletedDrainEmptyMapsAreNil(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newRig(t)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		r.closeSettleWindow()
		out := r.completed(drain.CauseSignal)

		if out.Waited != nil {
			t.Errorf("Outcome.Waited = %v (len %d), want nil: no reason was ever outstanding",
				out.Waited, len(out.Waited))
		}
		if out.Abandoned != nil {
			t.Errorf("Outcome.Abandoned = %v (len %d), want nil: nothing was abandoned",
				out.Abandoned, len(out.Abandoned))
		}
	})
}

// TestZeroDeadlineReasonAppearsInAbandonedWithWaitedZero is the pending-gate
// signature, and the reason the two maps are read together.
//
// A session blocked on a human is a hold — otherwise idle self-termination
// preferentially kills the sessions waiting on a person — with a zero deadline,
// because a drain that waits for a human answer stalls every rollout. So every
// drain with a pending gate reports it as abandoned, by construction. A
// consumer alerting on Abandoned > 0 would page on every human-gated rollout;
// Waited[r] == 0 is what identifies the expected abandonment so the alert can
// exclude it.
func TestZeroDeadlineReasonAppearsInAbandonedWithWaitedZero(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		rec := newRecorder(t)
		r := newRig(t,
			drain.WithDeadline(reasonGate, 0),
			drain.WithOnDeadline(rec.on),
		)
		// A spent deadline arms no timer: it is dispatched directly at the wait
		// phase's first instant, which is the whole point of a zero deadline.
		r.armsInWait(0)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		gate := r.mustAcquire(reasonGate)
		defer gate()
		r.closeSettleWindow()
		rec.next(reasonGate, 1)

		r.advance(phaseWait, testGrace-testSettle)
		out, overrun := r.exceeded(drain.CauseSignal)

		if got, ok := out.Waited[reasonGate]; !ok || got != 0 {
			t.Errorf("Outcome.Waited[%q] = %v, %t, want 0, true: a zero deadline is the reason's "+
				"applied policy, not a missing entry", reasonGate, got, ok)
		}
		want := map[Reason]int{reasonGate: 1}
		if !maps.Equal(out.Abandoned, want) {
			t.Errorf("Outcome.Abandoned = %v, want %v: a zero-deadline hold is abandoned by design",
				out.Abandoned, want)
		}
		if !maps.Equal(overrun.Abandoned, want) {
			t.Errorf("GracePeriodExceededError.Abandoned = %v, want %v", overrun.Abandoned, want)
		}
	})
}
