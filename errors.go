package drain

import (
	"errors"
	"strconv"
	"time"
)

// This file implements the error taxonomy documented on the package (see
// doc.go). Two rules govern the messages, and the second is a rule for future
// code, not a description of current behaviour:
//
//   - Every Error() is prefixed "drain: " and names its subject.
//   - When an Error() renders a string subject — a field name today, a Reason
//     when one is ever rendered — it quotes it with strconv.Quote, so an
//     untrusted value cannot inject newlines or control bytes into a log line.
//     No Error() in this package renders a Reason yet; the rule binds the one
//     that first does.

// InvalidPolicyError reports a coordinator policy that cannot be honoured. It is
// returned only by New, which is the single validation point: options record
// intent and never silently drop a bad value. Field names the offending
// parameter or option and Err describes what is wrong with it (and may be nil
// when the field name says it all).
type InvalidPolicyError struct {
	Field string
	Err   error
}

func (e *InvalidPolicyError) Error() string {
	msg := "drain: invalid policy for " + strconv.Quote(e.Field)
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns the underlying cause (possibly nil).
func (e *InvalidPolicyError) Unwrap() error { return e.Err }

// GracePeriodExceededError reports that the hard ceiling over the whole sequence
// expired before every hold reached a boundary. It accompanies a valid Outcome:
// the caller still exits cleanly and still gets telemetry. Abandoned is the same
// snapshot as Outcome.Abandoned, not a second read of the counters.
type GracePeriodExceededError struct {
	Elapsed   time.Duration
	Budget    time.Duration
	Abandoned map[Reason]int
}

func (e *GracePeriodExceededError) Error() string {
	total := 0
	for _, n := range e.Abandoned {
		total += n
	}
	return "drain: grace period exceeded after " + e.Elapsed.String() +
		" of " + e.Budget.String() + " budget, " + strconv.Itoa(total) + " hold(s) abandoned"
}

// ErrAlreadyRunning reports a second call to Wait or Run, concurrent or after
// return. A coordinator runs its sequence once and is permanently spent
// afterwards. It carries no fields, so it is a sentinel rather than a struct;
// match it with errors.Is.
var ErrAlreadyRunning = errors.New("drain: already running")
