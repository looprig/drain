package drain_test

import (
	"testing"

	"github.com/looprig/drain"
)

func TestCauseString(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		cause drain.Cause
		want  string
	}{
		{drain.CauseUnknown, "unknown"},
		{drain.CauseSignal, "signal"},
		{drain.CauseIdle, "idle"},
		{drain.CauseExplicit, "explicit"},
		{drain.CauseContext, "context"},
		{drain.Cause(99), "Cause(99)"},
	} {
		if got := tc.cause.String(); got != tc.want {
			t.Errorf("Cause(%d).String() = %q, want %q", tc.cause, got, tc.want)
		}
	}
}

// TestCauseUnknownIsZero pins the fail-secure zero value: a Cause that was never
// set must read as "the sequence has not started", never as a real cause.
func TestCauseUnknownIsZero(t *testing.T) {
	t.Parallel()

	if drain.CauseUnknown != 0 {
		t.Errorf("CauseUnknown = %d, want 0", drain.CauseUnknown)
	}
	var zero drain.Cause
	if zero != drain.CauseUnknown {
		t.Errorf("zero Cause = %v, want CauseUnknown", zero)
	}
}

// TestCauseValuesAreDistinct guards against a duplicated iota constant, which
// would make two causes indistinguishable in telemetry.
func TestCauseValuesAreDistinct(t *testing.T) {
	t.Parallel()

	seen := map[drain.Cause]bool{}
	for _, c := range []drain.Cause{
		drain.CauseUnknown,
		drain.CauseSignal,
		drain.CauseIdle,
		drain.CauseExplicit,
		drain.CauseContext,
	} {
		if seen[c] {
			t.Errorf("duplicate Cause value %d (%s)", c, c)
		}
		seen[c] = true
	}
}

// TestReasonUnspecifiedIsEmpty pins the zero value of the open Reason set.
func TestReasonUnspecifiedIsEmpty(t *testing.T) {
	t.Parallel()

	if drain.ReasonUnspecified != "" {
		t.Errorf("ReasonUnspecified = %q, want the empty string", drain.ReasonUnspecified)
	}
	var zero drain.Reason
	if zero != drain.ReasonUnspecified {
		t.Errorf("zero Reason = %q, want ReasonUnspecified", zero)
	}
}

// TestOutcomeZeroValue documents what a caller sees before a sequence has run:
// no cause, no elapsed time, and no maps to iterate.
func TestOutcomeZeroValue(t *testing.T) {
	t.Parallel()

	var out drain.Outcome
	if out.Cause != drain.CauseUnknown {
		t.Errorf("zero Outcome.Cause = %v, want CauseUnknown", out.Cause)
	}
	if out.Duration != 0 {
		t.Errorf("zero Outcome.Duration = %v, want 0", out.Duration)
	}
	if out.Waited != nil {
		t.Errorf("zero Outcome.Waited = %v, want nil", out.Waited)
	}
	if out.Abandoned != nil {
		t.Errorf("zero Outcome.Abandoned = %v, want nil", out.Abandoned)
	}
}
