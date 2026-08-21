package drain

import "time"

// Outcome is the telemetry record of one sequence. It is always valid and always
// returned, including alongside a non-nil error, so a caller that is being
// killed on a deadline still gets to say what it was waiting for.
type Outcome struct {
	// Cause is the first cause recorded by Begin.
	Cause Cause

	// Duration spans Begin to Wait's return.
	Duration time.Duration

	// Waited records, for every reason seen outstanding during the sequence, the
	// deadline drain applied to it: the configured per-reason deadline, or the
	// grace period for a reason with no configuration. A reason showing the full
	// grace period that was meant to be configured is a misspelled WithDeadline.
	Waited map[Reason]time.Duration

	// Abandoned is the per-reason hold count still outstanding at Wait's return,
	// snapshotted once, omitting reasons at zero. Reasons configured with a zero
	// deadline appear here on every drain by design; Waited[r] == 0 identifies
	// them, and an alert on Abandoned > 0 should exclude them.
	Abandoned map[Reason]int
}
