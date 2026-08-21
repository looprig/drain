package draintest_test

import (
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
	// concurrent advances did not reach.
	clock.Advance(time.Hour)

	for i := range armers {
		if got := kept[i].get(); got != 1 {
			t.Errorf("timer %d: callback ran %d times, want exactly 1", i, got)
		}
		got := stopped[i].get()
		switch {
		case stopOK[i] && got != 0:
			t.Errorf("timer %d: callback ran %d times after Stop() reported true, want 0", i, got)
		case !stopOK[i] && got != 1:
			t.Errorf("timer %d: Stop() reported false but the callback ran %d times, want 1", i, got)
		}
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
