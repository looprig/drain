package drain

import "time"

// Clock is the coordinator's entire dependency on time. It is deliberately the
// smallest surface that expresses what the sequence needs — read the current
// time, and run something later — so a test double is a few dozen lines rather
// than a clock library. drain/draintest ships a deterministic implementation;
// no test in this module or its consumers sleeps in real time.
type Clock interface {
	// Now reports the current time. The coordinator uses it to measure elapsed
	// time against the grace ceiling.
	Now() time.Time

	// AfterFunc arranges for f to run once d has elapsed and returns a Timer
	// that can cancel it. A non-positive d is due immediately.
	//
	// An implementation MUST NOT invoke f while holding a lock that AfterFunc,
	// Stop, or Now also acquires. The coordinator calls all three from inside
	// callbacks — the idle callback re-arms, the settle callback reads the
	// counters, the ceiling callback cancels — and it calls AfterFunc and Stop
	// while holding its own mutex, so the module's only lock order is
	// coordinator then clock, with no reverse edge. A Clock that ran f under
	// its own lock would close that cycle and deadlock the coordinator on the
	// first hold transition, with nothing in the resulting goroutine dump to
	// name the implementation as the cause.
	//
	// How an implementation satisfies that is its own business: time.AfterFunc
	// runs f on a new goroutine, and draintest runs it on the goroutine that
	// advanced the clock with its own lock released. Neither is required.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is the subset of *time.Timer the coordinator uses: it arms a callback
// once and may cancel it. There is no Reset, because every timer the sequence
// arms is armed for one deadline and then replaced.
type Timer interface {
	// Stop cancels the timer and reports whether it did so before the callback
	// ran. It returns false once the callback has fired or is already in flight,
	// which is why Stop is only ever an optimization: the callback itself must
	// re-validate that it still has work to do.
	Stop() bool
}

// systemClock is the production Clock: the time package itself. It carries no
// state, so it is a value rather than a pointer and every Coordinator shares
// one. A coordinator constructed without WithClock gets this.
type systemClock struct{}

// Now reports the current wall-clock time.
func (systemClock) Now() time.Time { return time.Now() }

// AfterFunc arms f on the runtime timer. A non-positive d fires it almost
// immediately, which is where the real clock and a fake part company: a fake
// cannot run a callback the test did not ask for, so code that must act on an
// already-elapsed deadline dispatches it directly rather than arming a timer.
func (systemClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// *time.Timer satisfies Timer, so a production Clock returns time.AfterFunc's
// result directly with no adapter.
var (
	_ Timer = (*time.Timer)(nil)
	_ Clock = systemClock{}
)
