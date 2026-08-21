package drain

import "time"

// Outcome is the telemetry record of one sequence. It is always returned, and on
// every path where a sequence actually ran it is valid and populated —
// including alongside a non-nil error, so a caller that is being killed on a
// deadline still gets to say what it was waiting for. The single exception is
// ErrAlreadyRunning, which yields the zero Outcome because the call it came from
// ran no sequence.
type Outcome struct {
	// Cause is the first cause recorded by Begin.
	Cause Cause

	// Duration spans Begin to Wait's return.
	Duration time.Duration

	// Waited records, for every reason outstanding when admission closed, the
	// deadline drain applied to it: the configured per-reason deadline, or the
	// grace period for a reason with no configuration. A reason showing the full
	// grace period that was meant to be configured is a misspelled WithDeadline,
	// which nothing else in the module can report — the typo is a valid reason
	// with a valid deadline that simply nobody holds.
	//
	// The admission cutoff is the scope, so a reason acquired and released
	// entirely inside a signal drain's open settle window has no entry: no
	// deadline was ever applied to it. A configured reason nobody held has no
	// entry either.
	//
	// Waited is nil when no reason was outstanding at the cutoff; it is never an
	// empty non-nil map. The map is the caller's own — the coordinator keeps no
	// reference and never hands out its configuration.
	Waited map[Reason]time.Duration

	// Abandoned is the per-reason hold count still outstanding at Wait's return,
	// snapshotted once and taken before the last abandon callbacks are
	// dispatched, so it is exactly the work those callbacks were told about
	// rather than whatever the counters said once they had run. Reasons at zero
	// are omitted.
	//
	// A zero-deadline reason is the one most likely to show up here, and it is
	// not a failure when it does: WithDeadline(r, 0) means "abandon at the cut",
	// so a hold nobody releases is reported here on every drain it was held
	// for. It is NOT unconditional — a zero deadline abandons but does not end
	// the wait, so an owner whose OnDeadline interrupts the work releases the
	// hold and the reason is absent — but an alert on Abandoned > 0 should
	// exclude the reasons Waited[r] == 0 identifies, because for those this is
	// the configured outcome rather than an overrun.
	//
	// Abandoned is nil when every count is zero; it is never an empty non-nil
	// map. When Wait also returns a *GracePeriodExceededError, that error's
	// Abandoned field is this same map — one snapshot, two names — and the
	// coordinator retains no reference to it, so mutating it is safe and is
	// visible through both.
	Abandoned map[Reason]int
}
