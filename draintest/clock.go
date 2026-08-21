package draintest

import (
	"cmp"
	"context"
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
// Advance from one goroutine. That is a rule, not a preference: an Advance
// computes its target when it is entered and moves the clock at the end only
// if that target is still ahead, so an advance overtaken by a concurrent one
// is a complete no-op — not merely an advance that shared elapsed time. Two
// concurrent Advance(time.Second) calls can leave the clock one second later,
// never two. Concurrent advances remain memory-safe and never corrupt the timer
// set, which is why this package's own concurrency test does it on purpose, but
// a test that asserts on Now, or on which timers fired, must advance from a
// single goroutine.
//
// The other ordering hazard is the arming race, and Armed and BlockUntil exist
// for it: the code under test usually arms its timer on a goroutine the test
// just started, so an Advance issued straight afterwards can win and leave the
// timer armed forever. Block for the arm first.
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64 // arming order, the tie-break for equal deadlines
	timers []*fakeTimer
	armed  chan struct{} // closed by the next arming; nil when nobody waits
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
//
// That difference from time.AfterFunc, which really does dispatch a
// non-positive d almost at once, is a constraint on the code under test as much
// as on the test: code that must act on a zero or already-elapsed deadline has
// to special-case d <= 0 and dispatch directly rather than arm a timer for it.
// Arming instead turns the zero-deadline case — a pending gate that must be
// abandoned immediately — into a wait for an Advance that the coordinator's own
// sequence has no reason to make, so the drain hangs rather than failing.
func (c *Clock) AfterFunc(d time.Duration, f func()) drain.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &fakeTimer{clock: c, deadline: c.now.Add(d), seq: c.seq, f: f}
	c.seq++
	c.timers = append(c.timers, t)
	c.notifyArmedLocked()
	return t
}

// Armed reports the number of timers armed and not yet fired or stopped.
func (c *Clock) Armed() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.armedCountLocked()
}

// BlockUntil blocks until at least n timers are armed, and is how a test orders
// "the code under test armed its timer" against "the test advanced the clock".
// Without it the usual shape
//
//	go coord.Wait(ctx)
//	clock.BlockUntil(1)
//	clock.Advance(gracePeriod)
//
// is a race whose losing side is silent: the advance runs first, the timer is
// armed afterwards for a deadline the clock has already passed, and the test
// hangs to its own timeout instead of failing.
//
// BlockUntil has no timeout of its own and relies on the test binary's
// -timeout, which is deliberate. Failing usefully on a timeout needs a
// *testing.T, and taking one here would mean importing testing from a library
// package, which registers the testing flags into every consumer binary that
// links draintest. The hang it leaves is self-describing in a way the one it
// replaces is not: the timeout panic dumps a goroutine sitting in
// Clock.BlockUntil, naming exactly the arm that never happened. Use
// BlockUntilContext where a bounded wait is wanted instead.
//
// A note for future readers, so it is not relitigated: testing/synctest would
// order the arm too, and for a test inside drain it is a fine alternative — see
// this package's doc comment. It is not the mechanism here because draintest is
// exported for drain's consumers, and requiring a consumer to run its own
// shutdown wiring under synctest.Test is a constraint this module has no
// standing to impose.
func (c *Clock) BlockUntil(n int) { _ = c.BlockUntilContext(context.Background(), n) }

// BlockUntilContext is BlockUntil bounded by ctx. It returns nil once at least
// n timers are armed and ctx.Err() if ctx is done first, so a test that would
// rather fail in a second than hang to the binary's timeout can say so.
func (c *Clock) BlockUntilContext(ctx context.Context, n int) error {
	for {
		c.mu.Lock()
		if c.armedCountLocked() >= n {
			c.mu.Unlock()
			return nil
		}
		armed := c.armedSignalLocked()
		c.mu.Unlock()

		select {
		case <-armed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
	c.pruneLocked()

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
// runtime has already committed to the callback when Stop lands, and the
// coordinator's idle path must be inert when that happens — that is what the
// idle generation counter is for — so a test needs to say "run that callback
// now, despite the Stop". Advance would refuse: a stopped timer is never due.
//
// Two deliberate divergences from a real timer, both simplifications that make
// the forcing possible at all:
//
//   - In the real interleaving Stop returns false, because the runtime has
//     already committed to the callback. Here Stop returned true and the
//     callback runs anyway. The fake reproduces the effect the coordinator must
//     survive, not the return value that announces it, and a test that wants
//     the honest Stop() == false ordering can simply Advance past the deadline
//     and then Stop.
//   - A forced callback observes a Now strictly before its own deadline, which
//     cannot happen in the real world. It is harmless for the idle-generation
//     path, which reads counters and not the clock, but do not force a callback
//     whose behaviour depends on the time it reads.
//
// A forced timer is spent afterwards exactly as a fired one is, so a later
// Advance will not run it again and Stop reports false.
//
// The count is returned so a test can assert it forced what it meant to, and it
// is exact for this call: every timer counted is one this call ran, even when
// another goroutine calls FireStopped concurrently. It is not, however, a count
// of the timers the test had in mind. FireStopped fires every stopped, unfired
// timer, and a coordinator that re-arms an idle timer on each hold transition
// leaves a stopped one behind on every transition, so a test that has churned
// holds should assert >= 1 rather than == 1.
func (c *Clock) FireStopped() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked()

	forced := make([]*fakeTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if t.stopped && !t.fired {
			// Claim the timer under the lock, before any callback runs and
			// drops it. Marking lazily, just before each call, would let a
			// concurrent FireStopped snapshot everything this one has not
			// reached yet and run those callbacks a second time.
			t.fired = true
			forced = append(forced, t)
		}
	}
	slices.SortFunc(forced, byDeadline)

	for _, t := range forced {
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

// pruneLocked drops the timers that have fired. Nothing can reach a fired timer
// again — Stop reads the timer's own flag, not this slice — and without the
// prune the slice only grows, which matters because every firing costs a full
// scan of it: a coordinator that re-arms a timer on each hold transition turns
// a linear test into a quadratic one.
//
// Stopped timers are kept. They are unfired, and running them is precisely what
// FireStopped is for; a prune that dropped them would quietly disarm the one
// interleaving the fake exists to force. They are released once FireStopped has
// run them, or when the clock itself is dropped — so a test that stops timers by
// the thousand and never forces them keeps paying the scan for them.
func (c *Clock) pruneLocked() {
	c.timers = slices.DeleteFunc(c.timers, func(t *fakeTimer) bool { return t.fired })
}

// armedCountLocked counts the timers that are still waiting for their deadline.
func (c *Clock) armedCountLocked() int {
	n := 0
	for _, t := range c.timers {
		if !t.fired && !t.stopped {
			n++
		}
	}
	return n
}

// armedSignalLocked returns a channel the next arming closes. It is created on
// demand so that AfterFunc allocates nothing when no one is blocked.
func (c *Clock) armedSignalLocked() chan struct{} {
	if c.armed == nil {
		c.armed = make(chan struct{})
	}
	return c.armed
}

// notifyArmedLocked wakes every BlockUntil waiter to re-check its condition.
func (c *Clock) notifyArmedLocked() {
	if c.armed != nil {
		close(c.armed)
		c.armed = nil
	}
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
