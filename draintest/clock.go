package draintest

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/looprig/drain"
)

// Clock is a deterministic drain.Clock. Time starts where NewClock puts it and
// moves only when Advance is called, so a test drives a five-second settle
// window or a ten-minute idle timeout in microseconds and never flakes on the
// scheduler.
//
// A Clock is safe for concurrent use: timers may be armed and stopped from any
// goroutine while another advances time, which is exactly how a coordinator's
// tests exercise it. Callbacks run on the goroutine that called Advance or
// FireStopped, with the clock's lock released, so a callback may itself call
// Now, AfterFunc, or Timer.Stop.
//
// One caveat for concurrent use: Advance releases the lock while a callback
// runs, so advances that overlap a callback may share elapsed time rather than
// strictly summing. A test that asserts on an exact Now must advance from one
// goroutine.
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64 // arming order, the tie-break for equal deadlines
	timers []*fakeTimer
}

// NewClock returns a Clock reading start. The value is arbitrary — nothing in
// drain depends on the wall time — but passing a fixed one keeps failure
// messages readable.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now reports the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc arms f to run once when the clock reaches now+d. A non-positive d
// is due immediately, which here means "on the next Advance, including
// Advance(0)" rather than "on some other goroutine, shortly": the fake never
// runs a callback that the test did not ask for.
func (c *Clock) AfterFunc(d time.Duration, f func()) drain.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &fakeTimer{clock: c, deadline: c.now.Add(d), seq: c.seq, f: f}
	c.seq++
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock forward by d, running every timer that becomes due in
// deadline order (equal deadlines in arming order). Each callback observes Now
// at its own deadline, not at the end of the advance, so a callback that reads
// the clock sees what a real one would; Now reaches now+d once they have all
// run. A callback that arms a further timer due inside the same window runs in
// this same Advance.
//
// Advance panics on a negative d. A clock that silently ran backwards would let
// a test pass against an impossible world.
func (c *Clock) Advance(d time.Duration) {
	if d < 0 {
		panic("draintest: Clock.Advance called with a negative duration")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.now.Add(d)
	for {
		t := c.earliestDueLocked(target)
		if t == nil {
			break
		}
		if t.deadline.After(c.now) {
			c.now = t.deadline
		}
		t.fired = true
		c.runUnlocked(t.f)
	}
	if target.After(c.now) {
		c.now = target
	}
}

// FireStopped runs the callbacks of the timers that have been stopped without
// having fired — those stopped as of this call, in deadline order — and reports
// how many it ran. It does not move the clock.
//
// It exists because Advance cannot reach that state, and one drain mechanism is
// built entirely around it. A real time.Timer permits an interleaving where the
// runtime has already committed to the callback when Stop lands: Stop returns
// false and the callback runs anyway. The coordinator's idle path must be inert
// when that happens — that is what the idle generation counter is for — and a
// test can only force the interleaving if it can say "run that callback now,
// despite the Stop". Advance would refuse: a stopped timer is never due.
//
// A forced timer is spent afterwards exactly as a fired one is, so a later
// Advance will not run it again and Stop reports false.
//
// The count is returned so a test can assert it forced what it meant to. A
// race-forcing test that silently forced nothing would pass while proving
// nothing at all.
func (c *Clock) FireStopped() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	forced := make([]*fakeTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if t.stopped && !t.fired {
			forced = append(forced, t)
		}
	}
	slices.SortFunc(forced, byDeadline)

	for _, t := range forced {
		t.fired = true
		c.runUnlocked(t.f)
	}
	return len(forced)
}

// earliestDueLocked returns the timer that should fire next at or before target,
// or nil when none is due. Stopped and already-fired timers are never due.
func (c *Clock) earliestDueLocked(target time.Time) *fakeTimer {
	var next *fakeTimer
	for _, t := range c.timers {
		if t.fired || t.stopped || t.deadline.After(target) {
			continue
		}
		if next == nil || byDeadline(t, next) < 0 {
			next = t
		}
	}
	return next
}

// runUnlocked calls f with the lock released and reacquires it afterwards, so a
// callback can use the clock without deadlocking. The lock is reacquired even
// if f panics, which keeps the caller's deferred unlock balanced.
func (c *Clock) runUnlocked(f func()) {
	c.mu.Unlock()
	defer c.mu.Lock()
	f()
}

// byDeadline orders timers by deadline, then by arming order.
func byDeadline(a, b *fakeTimer) int {
	if d := a.deadline.Compare(b.deadline); d != 0 {
		return d
	}
	return cmp.Compare(a.seq, b.seq)
}

// fakeTimer is one armed callback. It is unexported because the drain.Timer
// contract is all a coordinator may use, and because the only capability beyond
// that contract — FireStopped — belongs on the Clock: the code under test owns
// its Timer values, so a test can reach them only through the clock it injected.
type fakeTimer struct {
	clock    *Clock
	deadline time.Time
	seq      uint64
	f        func()
	fired    bool // guarded by clock.mu
	stopped  bool // guarded by clock.mu
}

// Stop matches time.Timer.Stop: it reports whether it cancelled the callback
// before it ran. Once the callback has fired — including a firing forced by
// FireStopped, which stands in for "already in flight" — Stop is false, and so
// is a second Stop on a timer this call already cancelled.
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()

	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	return true
}
