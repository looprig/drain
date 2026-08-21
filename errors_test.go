package drain_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/looprig/drain"
)

// Compile-time proof that both typed errors satisfy error as pointers, which is
// the form New and Wait return and the form errors.As callers target.
var (
	_ error = (*drain.InvalidPolicyError)(nil)
	_ error = (*drain.GracePeriodExceededError)(nil)
)

func TestInvalidPolicyErrorUnwrap(t *testing.T) {
	t.Parallel()

	cause := errors.New("must be positive")
	err := &drain.InvalidPolicyError{Field: "gracePeriod", Err: cause}

	if got := errors.Unwrap(err); got != cause {
		t.Errorf("errors.Unwrap() = %v, want %v", got, cause)
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(%v, cause) = false, want true", err)
	}

	var target *drain.InvalidPolicyError
	if !errors.As(fmt.Errorf("wrapped: %w", err), &target) {
		t.Fatal("errors.As did not recover *InvalidPolicyError from a wrapped error")
	}
	if target.Field != "gracePeriod" {
		t.Errorf("recovered Field = %q, want %q", target.Field, "gracePeriod")
	}

	msg := err.Error()
	if !strings.HasPrefix(msg, "drain: ") {
		t.Errorf("Error() = %q, want a %q prefix", msg, "drain: ")
	}
	for _, want := range []string{"gracePeriod", "must be positive"} {
		t.Run("message mentions "+want, func(t *testing.T) {
			t.Parallel()

			if !strings.Contains(msg, want) {
				t.Errorf("Error() = %q, want it to mention %q", msg, want)
			}
		})
	}
}

// TestInvalidPolicyErrorNilCause covers the boundary where the field is invalid
// on its own terms and there is no underlying error to carry.
func TestInvalidPolicyErrorNilCause(t *testing.T) {
	t.Parallel()

	err := &drain.InvalidPolicyError{Field: "preDrainDelay"}
	if got := errors.Unwrap(err); got != nil {
		t.Errorf("errors.Unwrap() = %v, want nil", got)
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "drain: ") || !strings.Contains(msg, "preDrainDelay") {
		t.Errorf("Error() = %q, want a %q prefix naming preDrainDelay", msg, "drain: ")
	}
}

func TestGracePeriodExceededError(t *testing.T) {
	t.Parallel()

	err := &drain.GracePeriodExceededError{
		Elapsed:   6 * time.Second,
		Budget:    5 * time.Second,
		Abandoned: map[drain.Reason]int{"harness/pending_gate": 2, "harness/subprocess": 1},
	}

	var target *drain.GracePeriodExceededError
	if !errors.As(fmt.Errorf("wrapped: %w", err), &target) {
		t.Fatal("errors.As did not recover *GracePeriodExceededError from a wrapped error")
	}
	if target.Elapsed != 6*time.Second || target.Budget != 5*time.Second {
		t.Errorf("recovered Elapsed/Budget = %v/%v, want 6s/5s", target.Elapsed, target.Budget)
	}
	if target.Abandoned["harness/pending_gate"] != 2 {
		t.Errorf("recovered Abandoned = %v, want 2 for harness/pending_gate", target.Abandoned)
	}

	msg := err.Error()
	if !strings.HasPrefix(msg, "drain: ") {
		t.Errorf("Error() = %q, want a %q prefix", msg, "drain: ")
	}
	// "3 hold(s)" is the one value in the message that is computed rather than a
	// field render: Error() sums Abandoned across reasons, so 2 + 1 must not
	// surface as either input on its own.
	for _, want := range []string{"6s", "5s", "3 hold(s) abandoned"} {
		t.Run("message mentions "+want, func(t *testing.T) {
			t.Parallel()

			if !strings.Contains(msg, want) {
				t.Errorf("Error() = %q, want it to mention %q", msg, want)
			}
		})
	}
}

// TestGracePeriodExceededErrorAbandonedTotal pins the derived count on its own:
// the total is a sum over reasons, so a message that echoed a single reason's
// count would still contain a plausible-looking number.
func TestGracePeriodExceededErrorAbandonedTotal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		abandoned map[drain.Reason]int
		want      string
	}{
		{"nil map", nil, "0 hold(s) abandoned"},
		{"empty map", map[drain.Reason]int{}, "0 hold(s) abandoned"},
		{"one reason", map[drain.Reason]int{"a": 4}, "4 hold(s) abandoned"},
		{"summed across reasons", map[drain.Reason]int{"a": 4, "b": 3, "c": 1}, "8 hold(s) abandoned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := &drain.GracePeriodExceededError{
				Elapsed:   2 * time.Second,
				Budget:    time.Second,
				Abandoned: tc.abandoned,
			}
			msg := err.Error()
			if !strings.HasPrefix(msg, "drain: ") {
				t.Errorf("Error() = %q, want a %q prefix", msg, "drain: ")
			}
			if !strings.Contains(msg, tc.want) {
				t.Errorf("Error() = %q, want it to mention %q", msg, tc.want)
			}
		})
	}
}

func TestErrAlreadyRunningIsSentinel(t *testing.T) {
	t.Parallel()

	if drain.ErrAlreadyRunning == nil {
		t.Fatal("ErrAlreadyRunning is nil")
	}
	// The property the sentinel actually depends on is that matching is by
	// identity: an unrelated error carrying the same text must not match, or a
	// caller's errors.Is would classify someone else's failure as this one.
	// (errors.Is(x, x) is a language guarantee and proves nothing.)
	if errors.Is(errors.New("drain: already running"), drain.ErrAlreadyRunning) {
		t.Error("sentinel must match by identity, never by message text")
	}
	if !errors.Is(fmt.Errorf("wait: %w", drain.ErrAlreadyRunning), drain.ErrAlreadyRunning) {
		t.Error("errors.Is did not match ErrAlreadyRunning through a wrap")
	}
	if msg := drain.ErrAlreadyRunning.Error(); !strings.HasPrefix(msg, "drain: ") {
		t.Errorf("ErrAlreadyRunning.Error() = %q, want a %q prefix", msg, "drain: ")
	}
}
