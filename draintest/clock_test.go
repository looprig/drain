package draintest_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/drain"
	"github.com/looprig/drain/draintest"
)

// Compile-time proof that the fake is usable everywhere drain takes a Clock.
var _ drain.Clock = (*draintest.Clock)(nil)

// epoch is an arbitrary fixed start time. Nothing depends on its value; a fake
// clock's whole point is that only Advance moves it.
var epoch = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

// counter records callback invocations. Callbacks run on whichever goroutine
// called Advance or FireStopped, which under the concurrency test is not the
// test's own, so the count is atomic.
type counter struct{ n atomic.Int64 }

func (c *counter) inc()       { c.n.Add(1) }
func (c *counter) get() int64 { return c.n.Load() }

func TestFakeClockAfterFuncFiresOnAdvance(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		after    time.Duration
		advances []time.Duration
	}{
		{"exactly at the deadline", time.Second, []time.Duration{time.Second}},
		{"past the deadline", time.Second, []time.Duration{2 * time.Second}},
		{"cumulative advances reach the deadline", 3 * time.Second, []time.Duration{time.Second, time.Second, time.Second}},
		{"zero duration is due immediately", 0, []time.Duration{0}},
		{"negative duration is due immediately", -time.Second, []time.Duration{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clock := draintest.NewClock(epoch)
			var fired counter
			clock.AfterFunc(tc.after, fired.inc)

			for _, d := range tc.advances {
				clock.Advance(d)
			}
			if got := fired.get(); got != 1 {
				t.Fatalf("callback ran %d times after advancing to the deadline, want 1", got)
			}

			// A fired timer is spent: further advances must not re-run it.
			clock.Advance(time.Hour)
			if got := fired.get(); got != 1 {
				t.Errorf("callback ran %d times after a further advance, want 1", got)
			}
		})
	}
}

func TestFakeClockDoesNotFireBeforeDeadline(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		after time.Duration
		short time.Duration // strictly less than after
	}{
		{"no time has passed", 10 * time.Second, 0},
		{"one nanosecond short", 10 * time.Second, 10*time.Second - time.Nanosecond},
		{"most of the way there", 10 * time.Second, 9 * time.Second},
		{"finest granularity", time.Nanosecond, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clock := draintest.NewClock(epoch)
			var fired counter
			clock.AfterFunc(tc.after, fired.inc)

			clock.Advance(tc.short)
			if got := fired.get(); got != 0 {
				t.Fatalf("callback ran %d times after advancing %v of %v, want 0", got, tc.short, tc.after)
			}

			// The timer is still armed, not lost: the remainder fires it.
			clock.Advance(tc.after - tc.short)
			if got := fired.get(); got != 1 {
				t.Errorf("callback ran %d times after reaching the deadline, want 1", got)
			}
		})
	}
}

func TestFakeClockStopPreventsFiring(t *testing.T) {
	t.Parallel()

	clock := draintest.NewClock(epoch)
	var fired counter
	timer := clock.AfterFunc(time.Second, fired.inc)

	if !timer.Stop() {
		t.Fatal("Stop() on an armed, unfired timer = false, want true")
	}
	clock.Advance(time.Hour)
	if got := fired.get(); got != 0 {
		t.Errorf("callback ran %d times after a successful Stop, want 0", got)
	}
}

// TestFakeClockStopAfterFiringReturnsFalse matches real time.Timer semantics
// exactly: Stop reports whether it stopped the timer *before* the callback ran,
// so once the callback has fired — or has already been committed to — it is
// false. A fake that returned true here would hide the lost-race window the
// coordinator's idle generation counter exists to close.
func TestFakeClockStopAfterFiringReturnsFalse(t *testing.T) {
	t.Parallel()

	t.Run("after the callback fired", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter
		timer := clock.AfterFunc(time.Second, fired.inc)

		clock.Advance(time.Second)
		if got := fired.get(); got != 1 {
			t.Fatalf("callback ran %d times, want 1", got)
		}
		if timer.Stop() {
			t.Error("Stop() after the callback fired = true, want false")
		}
	})

	t.Run("after a successful Stop", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		timer := clock.AfterFunc(time.Second, func() {})

		if !timer.Stop() {
			t.Fatal("first Stop() = false, want true")
		}
		if timer.Stop() {
			t.Error("second Stop() = true, want false")
		}
	})

	// FireStopped is where the fake and time.Timer part company, deliberately:
	// this Stop returns true and the callback is then forced to run anyway,
	// where the real lost race is announced by Stop returning false. What must
	// still hold is the part the coordinator depends on — a spent timer is spent
	// — so the Stop after the forced firing is false.
	t.Run("after FireStopped ran the callback", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter
		timer := clock.AfterFunc(time.Second, fired.inc)

		if !timer.Stop() {
			t.Fatal("Stop() = false, want true")
		}
		if got := clock.FireStopped(); got != 1 {
			t.Fatalf("FireStopped() = %d, want 1", got)
		}
		if timer.Stop() {
			t.Error("Stop() after FireStopped = true, want false")
		}
		if got := fired.get(); got != 1 {
			t.Errorf("callback ran %d times, want 1", got)
		}
	})
}

func TestFakeClockAdvanceFiresInDeadlineOrder(t *testing.T) {
	t.Parallel()

	t.Run("earliest deadline first", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var (
			mu    sync.Mutex
			order []string
			seen  = map[string]time.Time{}
		)
		record := func(name string) func() {
			return func() {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, name)
				seen[name] = clock.Now()
			}
		}

		// Armed out of deadline order on purpose.
		clock.AfterFunc(3*time.Second, record("third"))
		clock.AfterFunc(time.Second, record("first"))
		clock.AfterFunc(2*time.Second, record("second"))

		clock.Advance(5 * time.Second)

		want := []string{"first", "second", "third"}
		if len(order) != len(want) {
			t.Fatalf("fired %v, want %v", order, want)
		}
		for i, name := range want {
			if order[i] != name {
				t.Fatalf("fired %v, want %v", order, want)
			}
		}

		// Each callback observes the clock at its own deadline, not at the end of
		// the advance, so a callback that reads Now sees what a real one would.
		for name, offset := range map[string]time.Duration{"first": time.Second, "second": 2 * time.Second, "third": 3 * time.Second} {
			if got, want := seen[name], epoch.Add(offset); !got.Equal(want) {
				t.Errorf("%s callback saw Now() = %v, want %v", name, got, want)
			}
		}
		if got, want := clock.Now(), epoch.Add(5*time.Second); !got.Equal(want) {
			t.Errorf("Now() after Advance = %v, want %v", got, want)
		}
	})

	t.Run("equal deadlines fire in arming order", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var (
			mu    sync.Mutex
			order []string
		)
		record := func(name string) func() {
			return func() {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, name)
			}
		}

		clock.AfterFunc(time.Second, record("a"))
		clock.AfterFunc(time.Second, record("b"))
		clock.AfterFunc(time.Second, record("c"))

		clock.Advance(time.Second)

		if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
			t.Errorf("fired %v, want [a b c]", order)
		}
	})
}

// TestFakeClockConcurrentAdvanceAndAfterFunc hammers the fake the way the
// coordinator's own tests will: holds arm and stop timers from many goroutines
// while the sequence advances time. Run with -race. The invariants are that no
// callback runs twice, and that a timer whose Stop returned true never runs at
// all — the same guarantee real time.Timer gives.
func TestFakeClockConcurrentAdvanceAndAfterFunc(t *testing.T) {
	t.Parallel()

	const (
		armers    = 32
		advancers = 8
		steps     = 25
	)

	clock := draintest.NewClock(epoch)

	kept := make([]*counter, armers)
	stopped := make([]*counter, armers)
	stopOK := make([]bool, armers)

	var wg sync.WaitGroup
	for i := range armers {
		kept[i] = &counter{}
		stopped[i] = &counter{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// One timer that is left to fire, and one raced against Stop.
			clock.AfterFunc(time.Duration(i%5+1)*time.Millisecond, kept[i].inc)
			timer := clock.AfterFunc(time.Duration(i%3+1)*time.Millisecond, stopped[i].inc)
			stopOK[i] = timer.Stop()
		}()
	}
	for range advancers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range steps {
				clock.Advance(time.Millisecond)
				_ = clock.Now()
			}
		}()
	}
	wg.Wait()

	// A single final advance from this goroutine settles every timer that the
	// concurrent advances did not reach. Only this advance may be reasoned
	// about: the concurrent ones overtake each other, so the elapsed time they
	// produced is a maximum and not a sum, and nothing here asserts on Now.
	clock.Advance(time.Hour)

	var won, lost int
	for i := range armers {
		if got := kept[i].get(); got != 1 {
			t.Errorf("timer %d: callback ran %d times, want exactly 1", i, got)
		}
		got := stopped[i].get()
		switch {
		case stopOK[i]:
			won++
			if got != 0 {
				t.Errorf("timer %d: callback ran %d times after Stop() reported true, want 0", i, got)
			}
		default:
			lost++
			if got != 1 {
				t.Errorf("timer %d: Stop() reported false but the callback ran %d times, want 1", i, got)
			}
		}
	}

	// Which branch each Stop takes is up to the scheduler, so report the split.
	// A run that lands entirely in one bucket still passes every assertion above
	// while proving only half of what this test is for; say so rather than let
	// it degrade quietly.
	t.Logf("Stop() won %d of %d races and lost %d", won, armers, lost)
	if won == 0 || lost == 0 {
		t.Skipf("only one Stop branch was exercised (won %d, lost %d): this run proves half the invariant", won, lost)
	}
}

// TestFakeClockFireStoppedConcurrent pins the invariant that made the lazy
// fired-marking a bug: two concurrent FireStopped calls must partition the
// stopped timers between them, never both run the same callback. Marking a
// timer just before calling it — with the lock dropped for the call — let a
// second caller re-snapshot everything the first had not reached yet.
func TestFakeClockFireStoppedConcurrent(t *testing.T) {
	t.Parallel()

	const (
		timers = 64
		firers = 8
	)

	clock := draintest.NewClock(epoch)
	fired := make([]*counter, timers)
	for i := range timers {
		fired[i] = &counter{}
		timer := clock.AfterFunc(time.Duration(i+1)*time.Second, fired[i].inc)
		if !timer.Stop() {
			t.Fatalf("timer %d: Stop() = false, want true", i)
		}
	}

	var (
		wg     sync.WaitGroup
		forced atomic.Int64
	)
	for range firers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			forced.Add(int64(clock.FireStopped()))
		}()
	}
	wg.Wait()

	// The returned counts are exact, so they partition the set: every timer is
	// claimed by exactly one caller.
	if got := forced.Load(); got != timers {
		t.Errorf("FireStopped counts summed to %d, want %d", got, timers)
	}
	for i := range timers {
		if got := fired[i].get(); got != 1 {
			t.Errorf("timer %d: callback ran %d times, want exactly 1", i, got)
		}
	}
}

// TestFakeClockArmed covers the count that BlockUntil waits on. "Armed" is
// specifically "waiting for its deadline": neither a fired timer nor a stopped
// one is still going to happen.
func TestFakeClockArmed(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		arm  func(c *draintest.Clock)
		want int
	}{
		{"a fresh clock has none", func(*draintest.Clock) {}, 0},
		{"one armed timer", func(c *draintest.Clock) { c.AfterFunc(time.Second, func() {}) }, 1},
		{"several armed timers", func(c *draintest.Clock) {
			for range 3 {
				c.AfterFunc(time.Second, func() {})
			}
		}, 3},
		{"a stopped timer is not armed", func(c *draintest.Clock) {
			c.AfterFunc(time.Second, func() {}).Stop()
		}, 0},
		{"a fired timer is not armed", func(c *draintest.Clock) {
			c.AfterFunc(time.Second, func() {})
			c.Advance(time.Second)
		}, 0},
		{"only the unfired remainder counts", func(c *draintest.Clock) {
			c.AfterFunc(time.Second, func() {})
			c.AfterFunc(time.Hour, func() {})
			c.AfterFunc(2*time.Hour, func() {})
			c.Advance(time.Second)
		}, 2},
		{"a timer armed by a callback counts", func(c *draintest.Clock) {
			c.AfterFunc(time.Second, func() { c.AfterFunc(time.Hour, func() {}) })
			c.Advance(time.Second)
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clock := draintest.NewClock(epoch)
			tc.arm(clock)
			if got := clock.Armed(); got != tc.want {
				t.Errorf("Armed() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestFakeClockBlockUntilOrdersTheArm is the reason BlockUntil exists: without
// it the advance can beat the arm, the timer is left waiting on a deadline the
// clock has already passed, and the test hangs to its own timeout rather than
// failing.
func TestFakeClockBlockUntilOrdersTheArm(t *testing.T) {
	t.Parallel()

	t.Run("returns immediately when enough timers are already armed", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		clock.BlockUntil(0) // never blocks: zero timers is always satisfied

		clock.AfterFunc(time.Second, func() {})
		clock.BlockUntil(1)
	})

	t.Run("waits for a timer armed on another goroutine", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter

		armed := make(chan struct{})
		go func() {
			<-armed
			clock.AfterFunc(time.Second, fired.inc)
		}()

		close(armed)
		clock.BlockUntil(1)
		clock.Advance(time.Second)

		if got := fired.get(); got != 1 {
			t.Errorf("callback ran %d times, want 1 — the advance beat the arm", got)
		}
	})

	t.Run("counts arms across goroutines", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter
		for range 4 {
			go clock.AfterFunc(time.Second, fired.inc)
		}

		clock.BlockUntil(4)
		if got := clock.Armed(); got < 4 {
			t.Fatalf("Armed() = %d after BlockUntil(4), want at least 4", got)
		}
		clock.Advance(time.Second)
		if got := fired.get(); got != 4 {
			t.Errorf("callbacks ran %d times, want 4", got)
		}
	})

	t.Run("a stopped timer does not satisfy the wait", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		clock.AfterFunc(time.Second, func() {}).Stop()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := clock.BlockUntilContext(ctx, 1); !errors.Is(err, context.Canceled) {
			t.Errorf("BlockUntilContext with only a stopped timer = %v, want context.Canceled", err)
		}
	})
}

// TestFakeClockBlockUntilContextRespectsCancellation covers the bounded variant:
// a test that would rather fail in milliseconds than hang to the binary's
// -timeout gets an error instead of a wait.
func TestFakeClockBlockUntilContextRespectsCancellation(t *testing.T) {
	t.Parallel()

	t.Run("nothing armed", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if err := clock.BlockUntilContext(ctx, 1); !errors.Is(err, context.Canceled) {
			t.Errorf("BlockUntilContext() = %v, want context.Canceled", err)
		}
	})

	t.Run("cancelled while waiting", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		ctx, cancel := context.WithCancel(t.Context())
		go cancel()

		if err := clock.BlockUntilContext(ctx, 1); !errors.Is(err, context.Canceled) {
			t.Errorf("BlockUntilContext() = %v, want context.Canceled", err)
		}
	})

	t.Run("already satisfied ignores a done context", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		clock.AfterFunc(time.Second, func() {})

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := clock.BlockUntilContext(ctx, 1); err != nil {
			t.Errorf("BlockUntilContext() = %v, want nil — the condition already held", err)
		}
	})
}

// TestFakeClockPrunesFiredTimers pins the compaction that keeps a coordinator's
// timer churn linear. It is invisible through the drain.Clock contract by
// design, so the observable claims are the ones a prune could plausibly break:
// a fired timer stays spent, and a stopped one survives to be forced.
func TestFakeClockPrunesFiredTimers(t *testing.T) {
	t.Parallel()

	clock := draintest.NewClock(epoch)
	var (
		churn  counter
		forced counter
	)

	// A coordinator re-arming an idle timer on every hold transition: arm, fire,
	// arm, stop, repeat. Only the fired half is prunable.
	for range 50 {
		clock.AfterFunc(time.Millisecond, churn.inc)
		clock.Advance(time.Millisecond)
		clock.AfterFunc(time.Hour, forced.inc).Stop()
	}

	if got := churn.get(); got != 50 {
		t.Errorf("armed-and-fired callbacks ran %d times, want 50", got)
	}
	if got := clock.Armed(); got != 0 {
		t.Errorf("Armed() = %d after every timer fired or stopped, want 0", got)
	}
	// The stopped timers were kept, which is the half of the prune that matters:
	// forcing them is what FireStopped is for.
	if got := clock.FireStopped(); got != 50 {
		t.Errorf("FireStopped() = %d, want 50 — stopped timers must survive the prune", got)
	}
	if got := forced.get(); got != 50 {
		t.Errorf("forced callbacks ran %d times, want 50", got)
	}
	// Nothing is left to fire or force, in either direction.
	if got := clock.FireStopped(); got != 0 {
		t.Errorf("second FireStopped() = %d, want 0", got)
	}
	clock.Advance(2 * time.Hour)
	if got, want := churn.get()+forced.get(), int64(100); got != want {
		t.Errorf("callbacks ran %d times in total after a final advance, want %d", got, want)
	}
}

// TestFakeClockFireStoppedForcesTheLostStopRace covers the capability that has
// no equivalent in Advance: running the callback of a timer that was already
// stopped. Real time.Timer permits exactly this interleaving — the runtime is
// already committed to the callback when Stop lands, which is why Stop returns
// false in that window — and the coordinator's idle path must no-op when it
// happens. Only the coordinator holds its idle Timer, so the test reaches it
// through the clock rather than through the timer value.
func TestFakeClockFireStoppedForcesTheLostStopRace(t *testing.T) {
	t.Parallel()

	t.Run("runs the callback of a stopped timer without advancing time", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter
		timer := clock.AfterFunc(time.Hour, fired.inc)

		if !timer.Stop() {
			t.Fatal("Stop() = false, want true")
		}
		if got := clock.FireStopped(); got != 1 {
			t.Fatalf("FireStopped() = %d, want 1", got)
		}
		if got := fired.get(); got != 1 {
			t.Errorf("callback ran %d times, want 1", got)
		}
		if got := clock.Now(); !got.Equal(epoch) {
			t.Errorf("Now() = %v, want %v — FireStopped must not move the clock", got, epoch)
		}
	})

	t.Run("no-op when no timer was stopped", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter
		clock.AfterFunc(time.Second, fired.inc)

		if got := clock.FireStopped(); got != 0 {
			t.Errorf("FireStopped() with nothing stopped = %d, want 0", got)
		}
		if got := fired.get(); got != 0 {
			t.Errorf("callback ran %d times, want 0", got)
		}
	})

	t.Run("does not run an already-fired timer", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter
		timer := clock.AfterFunc(time.Second, fired.inc)

		clock.Advance(time.Second)
		if timer.Stop() {
			t.Fatal("Stop() after firing = true, want false")
		}
		if got := clock.FireStopped(); got != 0 {
			t.Errorf("FireStopped() = %d, want 0 — the timer already fired", got)
		}
		if got := fired.get(); got != 1 {
			t.Errorf("callback ran %d times, want 1", got)
		}
	})

	t.Run("a forced timer is spent", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var fired counter
		timer := clock.AfterFunc(time.Second, fired.inc)
		timer.Stop()

		if got := clock.FireStopped(); got != 1 {
			t.Fatalf("FireStopped() = %d, want 1", got)
		}
		if got := clock.FireStopped(); got != 0 {
			t.Errorf("second FireStopped() = %d, want 0", got)
		}
		clock.Advance(time.Hour)
		if got := fired.get(); got != 1 {
			t.Errorf("callback ran %d times, want 1", got)
		}
	})

	t.Run("forces stopped timers in deadline order", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		var (
			mu    sync.Mutex
			order []string
		)
		record := func(name string) func() {
			return func() {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, name)
			}
		}

		late := clock.AfterFunc(2*time.Second, record("late"))
		early := clock.AfterFunc(time.Second, record("early"))
		late.Stop()
		early.Stop()

		if got := clock.FireStopped(); got != 2 {
			t.Fatalf("FireStopped() = %d, want 2", got)
		}
		if len(order) != 2 || order[0] != "early" || order[1] != "late" {
			t.Errorf("fired %v, want [early late]", order)
		}
	})
}

// TestFakeClockNowMovesOnlyOnAdvance is the property the whole fake exists for:
// no test in drain or its consumers sleeps in real time.
func TestFakeClockNowMovesOnlyOnAdvance(t *testing.T) {
	t.Parallel()

	clock := draintest.NewClock(epoch)
	if got := clock.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() = %v, want the start time %v", got, epoch)
	}
	clock.Advance(90 * time.Minute)
	if got, want := clock.Now(), epoch.Add(90*time.Minute); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v", got, want)
	}
	clock.Advance(0)
	if got, want := clock.Now(), epoch.Add(90*time.Minute); !got.Equal(want) {
		t.Errorf("Now() after Advance(0) = %v, want %v", got, want)
	}
}

// TestFakeClockAdvanceRejectsNegative pins the loud failure: a fake clock that
// silently ran backwards would produce a passing test of an impossible world.
func TestFakeClockAdvanceRejectsNegative(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("Advance(-1s) did not panic, want a panic")
		}
	}()
	draintest.NewClock(epoch).Advance(-time.Second)
}

// TestFakeClockAdvanceRunsTimersChainedFromCallbacks pins the half of Advance's
// contract that says "a callback that arms a further timer due inside the same
// window runs in this same Advance". Nothing exercised it: the arming case in
// TestFakeClockArmed chains an hour-long timer against a one-second advance, so
// the chained timer is correctly left armed and the chaining itself never runs.
//
// Two drain mechanisms are about to depend on this. Task 8 arms one deadline
// timer per outstanding reason from inside the wait phase, and Task 11 re-arms
// the idle timer from inside the callback that just fired — a re-arm with a
// non-positive remaining budget is the zero-duration case below. An Advance that
// stopped at the timers armed when it was entered would leave both waiting for
// an advance the coordinator has no reason to make, which is a hang rather than
// a failure.
func TestFakeClockAdvanceRunsTimersChainedFromCallbacks(t *testing.T) {
	t.Parallel()

	// chain records firing order and the time each callback observed, which is
	// the second half of the claim: a chained callback must see its own
	// deadline, not the end of the advance.
	type chain struct {
		mu    sync.Mutex
		order []string
		seen  map[string]time.Time
	}
	record := func(clock *draintest.Clock, c *chain, name string) func() {
		return func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.order = append(c.order, name)
			c.seen[name] = clock.Now()
		}
	}
	newChain := func() *chain { return &chain{seen: map[string]time.Time{}} }

	assertOrder := func(t *testing.T, got, want []string) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Fatalf("callbacks fired %v, want %v", got, want)
		}
	}

	t.Run("a chained timer due inside the window runs in the same Advance", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		c := newChain()

		// first fires at +1s and arms second for +1s more, which is +2s: still
		// inside the three-second window this Advance covers.
		clock.AfterFunc(time.Second, func() {
			record(clock, c, "first")()
			clock.AfterFunc(time.Second, record(clock, c, "second"))
		})

		clock.Advance(3 * time.Second)

		assertOrder(t, c.order, []string{"first", "second"})
		if got, want := c.seen["second"], epoch.Add(2*time.Second); !got.Equal(want) {
			t.Errorf("chained callback saw Now() = %v, want its own deadline %v", got, want)
		}
		if got, want := clock.Now(), epoch.Add(3*time.Second); !got.Equal(want) {
			t.Errorf("Now() after Advance = %v, want %v", got, want)
		}
		if got := clock.Armed(); got != 0 {
			t.Errorf("Armed() = %d after the window covered both timers, want 0", got)
		}
	})

	t.Run("a chained timer due past the window is left armed", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		c := newChain()

		// The boundary case for the rule above: the chained timer is armed at
		// +1s for +5s, so it is due at +6s and this three-second window must not
		// reach it. It must be armed, not lost.
		clock.AfterFunc(time.Second, func() {
			record(clock, c, "first")()
			clock.AfterFunc(5*time.Second, record(clock, c, "late"))
		})

		clock.Advance(3 * time.Second)

		assertOrder(t, c.order, []string{"first"})
		if got := clock.Armed(); got != 1 {
			t.Fatalf("Armed() = %d after the chained timer's deadline fell outside the window, want 1", got)
		}

		clock.Advance(3 * time.Second)
		assertOrder(t, c.order, []string{"first", "late"})
		if got, want := c.seen["late"], epoch.Add(6*time.Second); !got.Equal(want) {
			t.Errorf("chained callback saw Now() = %v, want its own deadline %v", got, want)
		}
	})

	t.Run("a zero-duration timer armed from a callback runs in the same Advance", func(t *testing.T) {
		t.Parallel()

		clock := draintest.NewClock(epoch)
		c := newChain()

		clock.AfterFunc(time.Second, func() {
			record(clock, c, "first")()
			clock.AfterFunc(0, record(clock, c, "immediate"))
		})

		clock.Advance(time.Second)

		assertOrder(t, c.order, []string{"first", "immediate"})
		// The advance stops exactly at the parent's deadline, so the chained
		// timer is due at the instant the clock has already reached. It must run
		// anyway: this is the shape of a re-armed timer whose remaining budget
		// has already elapsed.
		if got, want := c.seen["immediate"], epoch.Add(time.Second); !got.Equal(want) {
			t.Errorf("chained callback saw Now() = %v, want %v", got, want)
		}
		if got := clock.Armed(); got != 0 {
			t.Errorf("Armed() = %d, want 0", got)
		}
	})

	t.Run("a chain of zero-duration timers drains in one Advance", func(t *testing.T) {
		t.Parallel()

		const depth = 8
		clock := draintest.NewClock(epoch)
		c := newChain()

		// Each link arms the next with d == 0, which is what a coordinator does
		// when it re-arms a timer whose budget is already spent. A loop that ran
		// only the timers armed on entry would fire one link and hang on the
		// rest.
		var arm func(n int)
		arm = func(n int) {
			clock.AfterFunc(0, func() {
				record(clock, c, "link"+strconv.Itoa(n))()
				if n+1 < depth {
					arm(n + 1)
				}
			})
		}
		arm(0)

		clock.Advance(0)

		want := make([]string, depth)
		for i := range want {
			want[i] = "link" + strconv.Itoa(i)
		}
		assertOrder(t, c.order, want)
		if got := clock.Now(); !got.Equal(epoch) {
			t.Errorf("Now() = %v after Advance(0), want %v", got, epoch)
		}
		if got := clock.Armed(); got != 0 {
			t.Errorf("Armed() = %d after the chain drained, want 0", got)
		}
	})
}
