package drain_test

// Idle self-termination, and the generation counter that makes it safe.
//
// The mechanism is small and the race it closes is not obvious, so it is worth
// restating what these tests are actually defending. Timer.Stop returns false
// once the callback has fired or is already in flight; the runtime has committed
// to running it and there is no taking it back. So stopping the idle timer on a
// new Acquire does NOT close the race — a hold granted at T + idleTimeout − ε can
// still be followed by the callback running. Stop is an optimization. The
// correctness mechanism is that the callback re-validates under the lock: the
// total must still be zero AND the generation it was armed in must still be
// current.
//
// Which is why no test here proves anything by stopping a timer. draintest's
// FireStopped exists for exactly this: it runs a stopped-but-unfired callback,
// forcing the interleaving a real timer permits and a fake clock otherwise
// cannot reach.
//
// Two things about that forcing, both deliberate:
//
//   - FireStopped fires EVERY stopped, unfired timer, not the one a test had in
//     mind, and a coordinator that re-arms on each hold transition leaves one
//     behind on every transition. That is not worked around here and draintest
//     gains no FireStoppedFunc(pred), because the assertion these tests make —
//     the coordinator does not begin draining — is one every stale timer must
//     satisfy too. Firing the debris makes the test stronger rather than
//     ambiguous. The count is asserted exactly only where the arming history is
//     exactly known, and as a lower bound otherwise.
//   - In the fake, Stop() returned TRUE and the callback runs anyway, where a
//     real timer would have returned false. The fake reproduces the effect the
//     coordinator must survive, not the return value that announces it. So no
//     test here depends on what Stop returned — and neither may the
//     coordinator, which is the point: the fake could not catch an
//     implementation that branched on it.

import (
	"context"
	"testing"
	"time"

	"github.com/looprig/drain"
)

// testIdle is the idle timeout every rig in this file is built with. It is well
// short of the grace period so that a drain the idle timer starts still has a
// whole sequence left to run.
const testIdle = 20 * time.Second

// reasonStartup is the startup hold: taken in main and released when the process
// is ready to serve. See TestStartupHoldPreventsIdleSelfKill.
const reasonStartup Reason = "drain_test/startup"

// newIdleRig builds a rig with the standard policy plus an idle timeout.
func newIdleRig(t *testing.T, opts ...drain.Option) *rig {
	t.Helper()
	return newRigWithPolicy(t, rigPolicy{grace: testGrace, settle: testSettle, idle: testIdle}, opts...)
}

// TestIdleTimeoutBeginsDrain is the feature in one test: nothing outstanding for
// IdleTimeout and the process starts draining itself, with the cause that says
// so.
//
// The rest of the sequence is unchanged, which is the design claim — idle is a
// second trigger for the same machinery, not a second machinery. Admission
// closes at Begin, as it does for every self-inflicted cause: nothing external
// is routing work here, so the settle window is a quiet window rather than an
// open one.
func TestIdleTimeoutBeginsDrain(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newIdleRig(t)
		r.start(ctx)

		r.advance(phaseIdle, testIdle)

		if !r.Draining() {
			t.Fatal("Draining() = false after the idle timeout elapsed with no holds, want true")
		}
		// Safe to probe here and nowhere else in this file: admission is closed,
		// so the probe is refused and leaves no hold behind. Before Begin the
		// same call would be a zero→nonzero→zero round trip that bumps the
		// generation and re-arms the idle timer under the test's feet.
		if r.grants(reasonTurn) {
			t.Error("Acquire granted after an idle drain began, want refused: admission closes at Begin")
		}

		r.closeSettleWindow()
		out := r.completed(drain.CauseIdle)
		if out.Duration != testSettle {
			t.Errorf("Outcome.Duration = %v, want %v: the epoch is Begin, which the idle timer is",
				out.Duration, testSettle)
		}
	})
}

// TestBeginDisarmsTheIdleTimer pins the tidying half of Begin. Once the sequence
// has started there is nothing left for the idle timer to decide — its callback
// would find the drain already under way and no-op — so this is not correctness,
// and it is asserted anyway: a ten-minute idle timeout left armed is a timer the
// runtime keeps alive, holding the coordinator, for ten minutes after the
// process has decided to exit.
//
// The assertion has to come from the wait phase rather than from the settle
// boundary. BlockUntil releases at "at least n", so a count taken while the
// coordinator is still arming could see the right number for the wrong reason;
// stillDraining's synctest edge is what makes the count final.
func TestBeginDisarmsTheIdleTimer(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newIdleRig(t)
		r.armsInWait(1)
		r.start(ctx)

		// The idle timer must be stopped by Begin and not by an Acquire, so
		// nothing is acquired until the drain has started. A signal drain keeps
		// admission open through settle, which is what makes that possible.
		r.reach(phaseIdle)
		r.Begin(drain.CauseSignal)

		r.reach(phaseSettle)
		turn := r.mustAcquire(reasonTurn)
		r.closeSettleWindow()

		r.stillDraining("the hold admitted during settle is outstanding")
		r.armed(1, "Begin stops the idle timer, so only the reason's deadline is left beside the ceiling")

		turn()
		r.completed(drain.CauseSignal)
	})
}

// TestIdleTimerArmedAtWaitEntryWhenNoHolds pins where the timer comes from when
// no hold has ever been taken. There is no nonzero→zero transition in a process
// that has never held anything, so a coordinator that armed only on that
// transition would never arm at all — an idle timeout that silently does nothing
// for the emptiest process there is.
func TestIdleTimerArmedAtWaitEntryWhenNoHolds(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newIdleRig(t)
		r.start(ctx)

		r.reach(phaseIdle)
		r.stillDraining("the idle timeout has not elapsed")
		if r.Draining() {
			t.Fatal("Draining() = true at Wait entry, want false")
		}

		r.clock.Advance(testIdle - time.Nanosecond)
		if r.Draining() {
			t.Fatalf("Draining() = true after %v of a %v idle timeout, want false",
				testIdle-time.Nanosecond, testIdle)
		}

		r.clock.Advance(time.Nanosecond)
		if !r.Draining() {
			t.Fatalf("Draining() = false after exactly %v, want true", testIdle)
		}

		r.closeSettleWindow()
		r.completed(drain.CauseIdle)
	})
}

// TestIdleEpochIsWaitEntryNotNew is the cold-start half of the same question.
// Construction happens early in main; serving starts later. A timer measured
// from New spends the interval between the two, and a process that took longer
// to start than its idle timeout would begin draining the instant it was ready.
//
// The wrong implementation fails this as a bubble deadlock rather than an
// assertion: it arms at New, the advance below fires it before Wait is ever
// entered, and the reach that follows waits for an arm that has already
// happened.
func TestIdleEpochIsWaitEntryNotNew(t *testing.T) {
	t.Parallel()

	// The other way a coordinator can arm too early, and the one the Wait-entry
	// latch is actually load-bearing for. The timer arms on a nonzero→zero
	// transition, and main can produce one before it ever calls Wait: take a
	// startup hold, finish initialising, release it, and only then enter Wait.
	// A coordinator that armed on that release would start the idle clock at a
	// point of its own choosing inside main.
	t.Run("a hold released before Wait entry does not start the clock", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			r := newIdleRig(t)

			// Acquired and released entirely before Wait: a nonzero→zero
			// transition with no Wait to arm for.
			r.mustAcquire(reasonStartup)()
			if got := r.clock.Armed(); got != 0 {
				t.Fatalf("Armed() = %d before Wait was entered, want 0", got)
			}

			r.clock.Advance(2 * testIdle)
			r.start(ctx)

			r.reach(phaseIdle)
			if r.Draining() {
				t.Fatalf("Draining() = true at Wait entry after a hold released %v earlier, "+
					"want false", 2*testIdle)
			}

			r.clock.Advance(testIdle)
			if !r.Draining() {
				t.Fatalf("Draining() = false %v after Wait entry, want true", testIdle)
			}
			r.closeSettleWindow()
			r.completed(drain.CauseIdle)
		})
	})

	t.Run("a slow start does not spend the timeout", func(t *testing.T) {
		t.Parallel()
		bubbled(t, idleEpochSlowStart)
	})
}

// idleEpochSlowStart is the subtest above's body, lifted out only because
// bubbled takes exactly this shape.
func idleEpochSlowStart(t *testing.T, ctx context.Context) {
	r := newIdleRig(t)

	// A slow start: longer than the whole idle timeout passes between New
	// and Wait entry. Nothing is armed, so this advance needs no ordering.
	r.clock.Advance(2 * testIdle)
	r.start(ctx)

	r.reach(phaseIdle)
	if r.Draining() {
		t.Fatalf("Draining() = true at Wait entry after %v of startup, want false: "+
			"the idle clock's epoch is Wait entry, not New", 2*testIdle)
	}

	r.clock.Advance(testIdle - time.Nanosecond)
	if r.Draining() {
		t.Fatal("Draining() = true a nanosecond before the timeout measured from Wait entry")
	}

	r.clock.Advance(time.Nanosecond)
	if !r.Draining() {
		t.Fatalf("Draining() = false %v after Wait entry, want true", testIdle)
	}

	r.closeSettleWindow()
	r.completed(drain.CauseIdle)
}

// TestAcquireBeforeIdleTimeoutPreventsDrain is the ordinary, non-racing case:
// work arrives before the timeout expires, so the process is not idle and does
// not drain. The hold lands a nanosecond before the timer was due, which is the
// tightest the fake clock can express without forcing the callback outright.
//
// The second half matters as much as the first. Releasing puts the process back
// to idle, and the timeout is then measured AFRESH from that release rather than
// resumed from wherever the stopped timer had got to.
func TestAcquireBeforeIdleTimeoutPreventsDrain(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newIdleRig(t)
		r.start(ctx)

		r.advance(phaseIdle, testIdle-time.Nanosecond)
		turn := r.mustAcquire(reasonTurn)

		// The instant the timer would have fired.
		r.armsIdle(0)
		r.clock.Advance(time.Nanosecond)
		r.reach(phaseIdle)
		if r.Draining() {
			t.Fatal("Draining() = true with a hold outstanding, want false")
		}
		r.stillDraining("a hold granted before the timeout is work, and work is not idleness")

		// Ready again, and the clock starts over.
		turn()
		r.armsIdle(1)
		r.advance(phaseIdle, testIdle-time.Nanosecond)
		if r.Draining() {
			t.Fatal("Draining() = true a nanosecond early, want the timeout measured from the release")
		}

		r.clock.Advance(time.Nanosecond)
		if !r.Draining() {
			t.Fatalf("Draining() = false %v after the last release, want true", testIdle)
		}
		r.closeSettleWindow()
		r.completed(drain.CauseIdle)
	})
}

// TestForcedLostIdleRaceNoOps is the race the generation counter exists for,
// forced rather than waited for. The hold is granted and the idle callback runs
// ANYWAY — the interleaving a real Timer.Stop admits when the runtime has
// already committed to the callback, and the one a test can never produce by
// stopping a timer, because stopping is precisely the insufficient fix.
//
// A coordinator that trusted Stop kills a process here at the moment it accepted
// work.
func TestForcedLostIdleRaceNoOps(t *testing.T) {
	t.Parallel()

	t.Run("one timer, exact count", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			r := newIdleRig(t)
			r.start(ctx)

			r.reach(phaseIdle)
			turn := r.mustAcquire(reasonTurn)

			// Exactly one timer has ever been armed on this coordinator, and
			// the Acquire above stopped it, so the count is knowable. Assert it:
			// a forcing that forced nothing would leave every assertion below
			// true for the wrong reason.
			if forced := r.clock.FireStopped(); forced != 1 {
				t.Fatalf("FireStopped() = %d, want 1: the idle timer armed at Wait entry and "+
					"stopped by the Acquire is the only stopped timer on this clock", forced)
			}

			r.stillDraining("the idle callback ran with a hold outstanding and must have no-oped")
			if r.Draining() {
				t.Fatal("Draining() = true after a lost idle race, want false: " +
					"the callback must re-validate the counters, not trust its own scheduling")
			}

			r.Begin(drain.CauseExplicit)
			turn()
			r.closeSettleWindow()
			r.completed(drain.CauseExplicit)
		})
	})

	// The same forcing on a coordinator that has churned holds, so the clock is
	// littered with stopped timers from earlier transitions. Every one of them
	// is fired, and every one of them must no-op — a strictly stronger claim
	// than the subtest above makes, which is why the debris is not filtered out.
	t.Run("with debris from earlier transitions", func(t *testing.T) {
		t.Parallel()

		bubbled(t, func(t *testing.T, ctx context.Context) {
			const churns = 3

			r := newIdleRig(t)
			r.start(ctx)
			r.reach(phaseIdle)

			for range churns {
				r.mustAcquire(reasonProc)()
			}
			turn := r.mustAcquire(reasonTurn)

			if forced := r.clock.FireStopped(); forced < 1 {
				t.Fatalf("FireStopped() = %d, want at least 1", forced)
			}
			r.stillDraining("every stale idle callback must no-op, not only the newest")
			if r.Draining() {
				t.Fatal("Draining() = true after firing every stopped idle timer, want false")
			}

			r.Begin(drain.CauseExplicit)
			turn()
			r.closeSettleWindow()
			r.completed(drain.CauseExplicit)
		})
	})
}

// TestLostIdleRaceThenSignalYieldsCauseSignal is the visible consequence the
// spec names, and it is the assertion that separates "the idle callback no-oped"
// from "the idle callback quietly recorded a cause and let a later Begin look
// like a no-op".
//
// Begin records the FIRST cause that actually transitions. If the lost idle race
// had transitioned, the SIGTERM that follows would be a no-op and the process
// would report CauseIdle for a drain the platform started — telemetry that sends
// an operator looking for an idle process during a rollout.
func TestLostIdleRaceThenSignalYieldsCauseSignal(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newIdleRig(t)
		r.armsInWait(1)
		r.start(ctx)

		r.reach(phaseIdle)
		turn := r.mustAcquire(reasonTurn)
		if forced := r.clock.FireStopped(); forced != 1 {
			t.Fatalf("FireStopped() = %d, want 1", forced)
		}
		if r.Draining() {
			t.Fatal("Draining() = true after a lost idle race, want false")
		}

		// The real thing arrives.
		r.Begin(drain.CauseSignal)
		if !r.grants(reasonGate) {
			t.Error("Acquire refused inside the settle window of a signal drain, want granted: " +
				"a lost idle race must leave the coordinator in the state it was in")
		}

		r.closeSettleWindow()
		turn()
		r.completed(drain.CauseSignal)
	})
}

// TestGenerationBumpsOnZeroToNonzeroTransition isolates the generation clause
// from the total-is-zero clause, which is the only way to show the generation is
// doing work at all.
//
// The timer armed at Wait entry is forced AFTER a hold has been acquired and
// released again. The total is back to zero, so "is the total still zero" says
// yes and would begin a drain. The process is not idle, though — it served
// something between the arming and the firing, and the release already armed a
// fresh timer for the full timeout. Only the generation mismatch distinguishes
// the two, and a coordinator that bumped nothing on the zero→nonzero transition
// kills a process that is actively serving.
func TestGenerationBumpsOnZeroToNonzeroTransition(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		r := newIdleRig(t)
		r.start(ctx)
		r.reach(phaseIdle)

		// Work arrives and finishes: zero→nonzero→zero. The first transition
		// bumps the generation and stops the timer; the second arms a new one.
		r.mustAcquire(reasonTurn)()

		if forced := r.clock.FireStopped(); forced != 1 {
			t.Fatalf("FireStopped() = %d, want 1: the timer armed at Wait entry is stopped and "+
				"the one the release armed is not", forced)
		}
		if got := r.Holds(); got != nil {
			t.Fatalf("Holds() = %v, want nil: the total is zero, so only the generation can "+
				"reject this callback", got)
		}
		r.stillDraining("the stale callback was armed in a generation the Acquire retired")
		if r.Draining() {
			t.Fatal("Draining() = true after a stale idle callback ran against a zero total, " +
				"want false: the generation is the check that catches this one")
		}

		// The timer the release armed is current, and still works.
		r.advance(phaseIdle, testIdle)
		if !r.Draining() {
			t.Fatalf("Draining() = false %v after the release, want true: rejecting the stale "+
				"callback must not disarm the mechanism", testIdle)
		}
		r.closeSettleWindow()
		r.completed(drain.CauseIdle)
	})
}

// TestStartupHoldPreventsIdleSelfKill is the regression test for the cold-start
// self-kill loop, and it is a test of the documented pattern rather than of a
// guard inside the module.
//
// The failure it prevents: IdleTimeout 20s, the first request 30s away, and a
// pod that kills itself before serving anything — after which the autoscaler
// starts another one to do the same thing. drain cannot tell "not started yet"
// from "finished everything" on its own; nothing outstanding looks the same
// either way. So the owner says which it is, by acquiring a hold in main and
// releasing it when the process is ready to serve.
//
// Wait entry alone is not enough, because a process can enter Wait early. What
// makes the pattern work is that the idle timer arms at Wait entry only if the
// total is ALREADY zero, and otherwise on a nonzero→zero transition — so the
// startup hold's release is what starts the clock.
func TestStartupHoldPreventsIdleSelfKill(t *testing.T) {
	t.Parallel()

	bubbled(t, func(t *testing.T, ctx context.Context) {
		// Thirty seconds of starting up against a twenty-second idle timeout:
		// the interval that kills the pod without this hold.
		const startup = 30 * time.Second

		r := newIdleRig(t)
		ready := r.mustAcquire(reasonStartup)

		r.armsIdle(0)
		r.start(ctx)
		r.stillDraining("Wait has been entered, but the process is not serving yet")
		r.reach(phaseIdle)

		r.clock.Advance(startup)
		if r.Draining() {
			t.Fatalf("Draining() = true %v into startup with a %v idle timeout, want false: "+
				"the startup hold is what stops a pod killing itself before it serves anything",
				startup, testIdle)
		}

		// Ready to serve. The idle clock starts here, and not before.
		ready()
		r.armsIdle(1)
		r.advance(phaseIdle, testIdle-time.Nanosecond)
		if r.Draining() {
			t.Fatal("Draining() = true a nanosecond early: the timeout is measured from the " +
				"release of the startup hold, not from Wait entry")
		}

		r.clock.Advance(time.Nanosecond)
		if !r.Draining() {
			t.Fatalf("Draining() = false %v after the process became ready, want true: the "+
				"startup hold must delay the idle timer, not disable it", testIdle)
		}
		r.closeSettleWindow()
		r.completed(drain.CauseIdle)
	})
}
