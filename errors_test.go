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
}

// TestInvalidPolicyErrorNilCause covers the boundary where the field is invalid
// on its own terms and there is no underlying error to carry: Unwrap must report
// nil rather than a non-nil error wrapping nothing.
func TestInvalidPolicyErrorNilCause(t *testing.T) {
	t.Parallel()

	err := &drain.InvalidPolicyError{Field: "preDrainDelay"}
	if got := errors.Unwrap(err); got != nil {
		t.Errorf("errors.Unwrap() = %v, want nil", got)
	}
}

// TestInvalidPolicyErrorMessage pins the rendering as whole strings, one
// distinct fixture per case. Substring checks over a single message would pass
// against a message that also said three other things, and the quoting is the
// point: a field name reaches a log line through strconv.Quote so it cannot
// carry a newline into it.
func TestInvalidPolicyErrorMessage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  *drain.InvalidPolicyError
		want string
	}{
		{
			"field alone",
			&drain.InvalidPolicyError{Field: "preDrainDelay"},
			`drain: invalid policy for "preDrainDelay"`,
		},
		{
			"field and cause",
			&drain.InvalidPolicyError{Field: "gracePeriod", Err: errors.New("must be positive")},
			`drain: invalid policy for "gracePeriod": must be positive`,
		},
		{
			"empty field",
			&drain.InvalidPolicyError{},
			`drain: invalid policy for ""`,
		},
		{
			"field needing quoting",
			&drain.InvalidPolicyError{Field: "line\none\nline two"},
			`drain: invalid policy for "line\none\nline two"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGracePeriodExceededErrorAs(t *testing.T) {
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
}

// TestGracePeriodExceededErrorMessage pins the whole message per fixture. The
// hold count is the one value that is computed rather than rendered — Error()
// sums Abandoned across reasons — so the cases vary the map shape as well as the
// durations, and a message that echoed one reason's count would fail rather than
// contain a plausible-looking number.
func TestGracePeriodExceededErrorMessage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  *drain.GracePeriodExceededError
		want string
	}{
		{
			"nil map",
			&drain.GracePeriodExceededError{Elapsed: 2 * time.Second, Budget: time.Second},
			"drain: grace period exceeded after 2s of 1s budget, 0 hold(s) abandoned",
		},
		{
			"empty map",
			&drain.GracePeriodExceededError{
				Elapsed: 2 * time.Second, Budget: time.Second,
				Abandoned: map[drain.Reason]int{},
			},
			"drain: grace period exceeded after 2s of 1s budget, 0 hold(s) abandoned",
		},
		{
			"one reason",
			&drain.GracePeriodExceededError{
				Elapsed: 31 * time.Second, Budget: 30 * time.Second,
				Abandoned: map[drain.Reason]int{"harness/active_turn": 4},
			},
			"drain: grace period exceeded after 31s of 30s budget, 4 hold(s) abandoned",
		},
		{
			"summed across reasons",
			&drain.GracePeriodExceededError{
				Elapsed: 6 * time.Second, Budget: 5 * time.Second,
				Abandoned: map[drain.Reason]int{"a": 4, "b": 3, "c": 1},
			},
			"drain: grace period exceeded after 6s of 5s budget, 8 hold(s) abandoned",
		},
		{
			"sub-second durations",
			&drain.GracePeriodExceededError{
				Elapsed: 1500 * time.Millisecond, Budget: 1200 * time.Millisecond,
				Abandoned: map[drain.Reason]int{"a": 1},
			},
			"drain: grace period exceeded after 1.5s of 1.2s budget, 1 hold(s) abandoned",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
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
