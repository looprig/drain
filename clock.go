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

	// AfterFunc runs f on its own goroutine after d has elapsed and returns a
	// Timer that can cancel it. A non-positive d is due immediately.
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

// *time.Timer satisfies Timer, so a production Clock returns time.AfterFunc's
// result directly with no adapter.
var _ Timer = (*time.Timer)(nil)
