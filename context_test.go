package drain_test

// Context cancellation. Wait's ctx is not a second grace period and not a
// suggestion: it is the process being told that the drain itself is over, and
// every branch of that is defined.
//
//   - ctx.Done() IMPLIES Begin. A coordinator that quietly stopped waiting
//     while the process kept running would be worse than one that never
//     started, so a cancellation with no cause recorded records CauseContext.
//   - It skips whatever remains of settle and of the wait phase. There is no
//     in-flight routing left to protect and no deadline left worth honouring.
//   - It still fires OnDeadline once per reason with a nonzero count. This is
//     the half a naive implementation drops: a cancelled drain abandons work,
//     and the owner is owed the same report it would get at a deadline.
//   - The callback ctx is ALREADY DONE on this path, deliberately. The
//     invocation is a notification that abandonment happened, not a budget to
//     work within.
//   - Wait returns a valid Outcome together with ctx.Err().
//
// Every test runs inside a synctest bubble, like the rest of the sequence
// suite: the claims are about a Wait that must return without the clock moving
// and about callbacks that must NOT fire, and both need the happens-before edge
// the bubble supplies.

import (
	"context"
	"errors"
	"maps"
	"testing"
	"testing/synctest"
	"time"

	"github.com/looprig/drain"
)

// TestCancelBeforeBeginYieldsCauseContext covers the branch with no other
// cause: nothing signalled, nothing idled, nobody called Begin, and the ctx is
// done. The sequence starts anyway, because the alternative is a Wait that
// returns while the coordinator has recorded nothing at all.
func TestCancelBeforeBeginYieldsCauseContext(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		r := newRig(t)
		r.start(ctx)

		// Wait is parked on begun: no cause, no ceiling, no timers.
		synctest.Wait()
		if r.Draining() {
			t.Fatal("Draining() = true before anything began, want false")
		}

		cancel()
		out := r.cancelled(drain.CauseContext)

		if out.Duration != 0 {
			t.Errorf("Outcome.Duration = %v, want 0: the clock never moved", out.Duration)
		}
		r.allStopped("a cancelled drain stops its ceiling on the way out")
	})
}

// TestCancelImpliesBegin is the same branch seen from the coordinator's state
// rather than from the Outcome. A cancellation is a Begin, so the sequence is
// begun and admission is closed — permanently, since the coordinator is spent
// once Wait returns.
func TestCancelImpliesBegin(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		r := newRig(t)
		r.start(ctx)
		synctest.Wait()

		cancel()
		r.cancelled(drain.CauseContext)

		if !r.Draining() {
			t.Error("Draining() = false after a cancelled Wait, want true: ctx.Done() implies Begin")
		}
		if r.grants(reasonTurn) {
			t.Error("Acquire granted after a cancelled Wait, want refused")
		}
	})
}

// TestCancelMidSettleSkipsAhead cancels inside the settle window. The window is
// dataplane-propagation time for requests that were already routed here, and a
// process that has been told to stop has no reason to spend it: the cut happens
// at once, the report is made, and Wait returns without the clock moving.
func TestCancelMidSettleSkipsAhead(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		rec := newRecorder(t)
		r := newRig(t, drain.WithOnDeadline(rec.on))
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		r.reach(phaseSettle)
		turn := r.mustAcquire(reasonTurn)
		defer turn()

		cancel()
		out := r.cancelled(drain.CauseSignal)

		// The whole of "skips ahead": no part of the five-second window was
		// spent, and the hold that outlived the drain was still reported.
		if out.Duration != 0 {
			t.Errorf("Outcome.Duration = %v, want 0: cancellation skips the rest of the settle window", out.Duration)
		}
		rec.next(reasonTurn, 1)
		rec.none("one report per outstanding reason, and there was one reason")
		if r.grants(reasonGate) {
			t.Error("Acquire granted after a cancelled drain, want refused: the cut runs on this path too")
		}
		if got := out.Abandoned[reasonTurn]; got != 1 {
			t.Errorf("Outcome.Abandoned = %v, want %d hold of %q", out.Abandoned, 1, reasonTurn)
		}
		r.allStopped("the settle timer and the ceiling are both stopped on the way out")
	})
}

// TestCancelMidWaitSkipsAhead cancels with a deadline still comfortably in the
// future. The deadline is what drain would have waited for; the cancellation
// says there is no longer anything to wait with, so the reason is reported at
// once rather than at its own deadline.
func TestCancelMidWaitSkipsAhead(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		const deadline = 20 * time.Second

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

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

		r.reach(phaseWait)
		rec.none("the deadline is another " + (deadline - testSettle).String() + " away")
		r.stillDraining("a drain with an outstanding hold waits for it")

		cancel()
		out := r.cancelled(drain.CauseSignal)

		rec.next(reasonTurn, 1)
		if out.Duration != testSettle {
			t.Errorf("Outcome.Duration = %v, want %v: only the settle window elapsed", out.Duration, testSettle)
		}
		if got := out.Abandoned[reasonTurn]; got != 1 {
			t.Errorf("Outcome.Abandoned = %v, want %d hold of %q", out.Abandoned, 1, reasonTurn)
		}
		r.allStopped("the deadline that never came due is stopped rather than left armed")
	})
}

// TestCancelFiresOnDeadlinePerOutstandingReason pins the count: once per
// reason, and only for reasons that actually have work outstanding.
//
// The second subtest is the interaction with the once-per-reason guard the
// deadline path shares. A reason already told at its own deadline must not be
// told again by the cancellation sweep — the owner's abandon path is a real
// action, and telling it twice is telling it to interrupt work it has already
// interrupted.
func TestCancelFiresOnDeadlinePerOutstandingReason(t *testing.T) {
	t.Parallel()

	t.Run("one report per reason still outstanding", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			rec := newRecorder(t)
			r := newRig(t, drain.WithOnDeadline(rec.on))
			r.start(ctx)
			r.Begin(drain.CauseSignal)

			// Two reasons outstanding, one of them twice, and a third that
			// finished before the cancellation landed.
			turn := r.mustAcquire(reasonTurn)
			defer turn()
			gate1 := r.mustAcquire(reasonGate)
			defer gate1()
			gate2 := r.mustAcquire(reasonGate)
			defer gate2()
			r.mustAcquire(reasonProc)()

			cancel()
			out := r.cancelled(drain.CauseSignal)

			// One goroutine per reason, dispatched at the same instant, so the
			// set is the property and the order is not.
			want := map[Reason]int{reasonTurn: 1, reasonGate: 2}
			if got := rec.took(len(want)); !maps.Equal(got, want) {
				t.Errorf("OnDeadline calls = %v, want %v", got, want)
			}
			rec.none("the subprocess hold released before the cancellation, so there is nothing to report")
			if got := out.Abandoned[reasonProc]; got != 0 {
				t.Errorf("Outcome.Abandoned = %v, want no entry for the released reason %q", out.Abandoned, reasonProc)
			}
		})
	})

	t.Run("a reason already told at its deadline is not told twice", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			const deadline = 10 * time.Second

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			rec := newRecorder(t)
			r := newRig(t,
				drain.WithDeadline(reasonTurn, deadline),
				drain.WithOnDeadline(rec.on),
			)
			r.armsInWait(2)
			r.start(ctx)
			r.Begin(drain.CauseSignal)

			turn := r.mustAcquire(reasonTurn)
			defer turn()
			gate := r.mustAcquire(reasonGate)
			defer gate()
			r.closeSettleWindow()

			// The turn's deadline comes due; the gate's is the full grace
			// period and is still far off.
			r.advance(phaseWait, deadline-testSettle)
			rec.next(reasonTurn, 1)

			cancel()
			r.cancelled(drain.CauseSignal)

			rec.next(reasonGate, 1)
			rec.none("the turn was told at its own deadline; the cancellation sweep does not tell it again")
		})
	})
}

// TestCancelCallbackCtxIsAlreadyDone pins the property that reads like a bug
// and is not one. OnDeadline's ctx is derived from Wait's, so on this path it
// is dead before the callback starts, and that is the honest thing to hand
// over: the drain is over, there is no budget, and the call is a notification
// that abandonment happened rather than an invitation to do work.
//
// It is asserted at the invocation rather than afterwards, which is what makes
// it a property of the dispatch and not of the test's timing.
//
// The two subtests differ in how cancellation reaches the derived context: the
// context package cancels a *cancelCtx parent's children synchronously, while a
// caller's own Context implementation is adopted by a watchdog goroutine
// instead and propagates with no ordering guarantee at all.
//
// Neither subtest can distinguish that, and the limit is stated rather than
// papered over. drain cancels the callback context itself before the sweep, and
// with that line removed BOTH cases still pass, because propagation wins a race
// it is not required to win — the sweep has a cut, a dispatch and a goroutine
// start to get through first. The explicit cancellation is what turns the
// property from a scheduling accident into a guarantee; what these subtests pin
// is that the callback is handed a dead context and the ceiling as its
// deadline, on a stdlib context and on a foreign one alike.
func TestCancelCallbackCtxIsAlreadyDone(t *testing.T) {
	t.Parallel()

	t.Run("context.WithCancel", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			assertCallbackCtxDoneAtInvocation(t, ctx, cancel)
		})
	})

	t.Run("a context whose cancellation propagates asynchronously", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			async := newAsyncCancelCtx(ctx)
			defer async.cancel()
			assertCallbackCtxDoneAtInvocation(t, async, async.cancel)
		})
	})
}

// assertCallbackCtxDoneAtInvocation drains a coordinator with one hold
// outstanding, cancels ctx, and asserts what OnDeadline was handed.
func assertCallbackCtxDoneAtInvocation(t *testing.T, ctx context.Context, cancel func()) {
	t.Helper()

	type call struct {
		err      error
		cause    error
		deadline time.Time
	}
	seen := make(chan call, 4)

	r := newRig(t, drain.WithOnDeadline(func(cbctx context.Context, _ Reason, _ int) {
		deadline, _ := cbctx.Deadline()
		seen <- call{err: cbctx.Err(), cause: context.Cause(cbctx), deadline: deadline}
	}))
	r.start(ctx)
	r.Begin(drain.CauseSignal)

	turn := r.mustAcquire(reasonTurn)
	defer turn()

	cancel()
	r.cancelled(drain.CauseSignal)

	got := <-seen
	if !errors.Is(got.err, context.Canceled) {
		t.Errorf("callback ctx Err() = %v at invocation, want context.Canceled: "+
			"a cancelled drain hands over a notification, not a budget", got.err)
	}
	// The half Err() cannot express. Err() is context.Canceled at the ceiling
	// too — the ceiling is clock-driven, so it cannot go through
	// context.WithDeadline — and an owner has to tell "out of budget" from
	// "called off from outside", because the correct response differs.
	if !errors.Is(got.cause, context.Canceled) || errors.Is(got.cause, context.DeadlineExceeded) {
		t.Errorf("context.Cause(callback ctx) = %v, want context.Canceled: "+
			"the drain was called off, not out of budget", got.cause)
	}
	// Still the ceiling, because the ctx is the same one every other path
	// gets: cancelled early, not re-derived.
	if want := epoch.Add(testGrace); !got.deadline.Equal(want) {
		t.Errorf("callback ctx Deadline() = %v, want %v", got.deadline, want)
	}
}

// asyncCancelCtx is a Context that reports Done through a channel of its own.
// That is enough to keep the context package from adopting a context derived
// from it as a child: WithCancel compares the parent's Done channel with the
// *cancelCtx it finds through Value, they differ, and the derived context is
// left to a watchdog goroutine instead. Cancellation then reaches the derived
// context strictly after this one is done, with no ordering guarantee at all.
//
// Nothing exotic is required for a caller to be in that position — any context
// wrapper that overrides Done qualifies — which is why drain cancels the
// callback context itself on this path rather than inheriting the propagation.
type asyncCancelCtx struct {
	context.Context
	done chan struct{}
}

func newAsyncCancelCtx(parent context.Context) *asyncCancelCtx {
	return &asyncCancelCtx{Context: parent, done: make(chan struct{})}
}

func (c *asyncCancelCtx) Done() <-chan struct{} { return c.done }

func (c *asyncCancelCtx) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return c.Context.Err()
	}
}

// cancel is idempotent, so a test may both defer it and call it.
func (c *asyncCancelCtx) cancel() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

// TestCancelReturnsValidOutcomeWithCtxErr is the contract for the return pair.
// A cancelled drain is the case where telemetry matters most — work was
// abandoned without any deadline being reached — so the Outcome is complete and
// the error is ctx.Err() and nothing else.
func TestCancelReturnsValidOutcomeWithCtxErr(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		r := newRig(t)
		r.start(ctx)
		r.Begin(drain.CauseSignal)

		turn := r.mustAcquire(reasonTurn)
		defer turn()
		r.reach(phaseSettle)

		cancel()
		res := r.result()

		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("Wait() = _, %v, want context.Canceled", res.err)
		}
		// Not a grace overrun. The budget was never spent; the caller was told
		// to stop, and an error that claimed otherwise would page the wrong
		// person.
		var overrun *drain.GracePeriodExceededError
		if errors.As(res.err, &overrun) {
			t.Errorf("Wait() = _, %v, want ctx.Err() alone on the cancellation path", res.err)
		}
		if res.out.Cause != drain.CauseSignal {
			t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, drain.CauseSignal)
		}
		if got := res.out.Abandoned[reasonTurn]; got != 1 {
			t.Errorf("Outcome.Abandoned = %v, want %d hold of %q", res.out.Abandoned, 1, reasonTurn)
		}
		// Valid means complete. The cut runs on this path too, so the reason
		// that was outstanding when admission closed is reported with the
		// deadline it would have been given — a cancelled drain still tells the
		// operator what policy was in force when it walked away.
		want := map[Reason]time.Duration{reasonTurn: testGrace}
		if !maps.Equal(res.out.Waited, want) {
			t.Errorf("Outcome.Waited = %v, want %v", res.out.Waited, want)
		}
	})
}

// TestCancelAfterBeginPreservesOriginalCause covers the ordering the Outcome's
// determinism rests on. Cancellation implies Begin, and Begin's first cause
// wins, so a ctx that is cancelled after the sequence started reports why the
// process actually decided to die — never CauseContext, which would erase the
// signal or the idle timer that really caused it.
func TestCancelAfterBeginPreservesOriginalCause(t *testing.T) {
	t.Parallel()

	// Every cause a Begin can record. CauseContext is absent because it is what
	// this test asserts is NOT recorded.
	for _, cause := range []drain.Cause{drain.CauseSignal, drain.CauseIdle, drain.CauseExplicit} {
		t.Run(cause.String(), func(t *testing.T) {
			t.Parallel()

			bubbled(t, func(t *testing.T, ctx context.Context) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				r := newRig(t)
				r.start(ctx)

				// Before Begin, so the hold is granted whatever the cause does
				// to admission: only a signal keeps it open through settle.
				turn := r.mustAcquire(reasonTurn)
				defer turn()

				r.Begin(cause)
				r.reach(phaseSettle)

				cancel()
				out := r.cancelled(cause)

				if out.Cause == drain.CauseContext {
					t.Error("Outcome.Cause = CauseContext, want the cause Begin recorded first")
				}
			})
		})
	}
}
