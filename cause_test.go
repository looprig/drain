package drain_test

import (
	"testing"

	"github.com/looprig/drain"
)

func TestCauseString(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		cause drain.Cause
		want  string
	}{
		{"CauseUnknown", drain.CauseUnknown, "unknown"},
		{"CauseSignal", drain.CauseSignal, "signal"},
		{"CauseIdle", drain.CauseIdle, "idle"},
		{"CauseExplicit", drain.CauseExplicit, "explicit"},
		{"CauseContext", drain.CauseContext, "context"},
		{"unknown ordinal", drain.Cause(99), "Cause(99)"},
		{"max ordinal", drain.Cause(255), "Cause(255)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.cause.String(); got != tc.want {
				t.Errorf("Cause(%d).String() = %q, want %q", uint8(tc.cause), got, tc.want)
			}
		})
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

// TestCauseOrdinals pins the numeric value of every non-zero cause. String()
// coverage cannot fail on this: reordering the iota block and reordering the
// String switch with it keeps every rendering correct while silently
// renumbering the values a consumer may have persisted or exported as a metric
// label. CauseUnknown's zero is pinned separately by TestCauseUnknownIsZero,
// where it belongs — it is the fail-secure default, not just an ordinal.
func TestCauseOrdinals(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		cause drain.Cause
		want  uint8
	}{
		{"CauseSignal", drain.CauseSignal, 1},
		{"CauseIdle", drain.CauseIdle, 2},
		{"CauseExplicit", drain.CauseExplicit, 3},
		{"CauseContext", drain.CauseContext, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := uint8(tc.cause); got != tc.want {
				t.Errorf("%s = %d, want %d", tc.name, got, tc.want)
			}
		})
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
