package drain

// This file is an internal test, unlike the rest of the suite, because what
// Task 4 delivers is internal: config, the Option closures that write to it, and
// the single validation point that reads it. The defaults it pins — a 5s
// PreDrainDelay, a zero IdleTimeout, a real system clock — have no exported
// consequence until Wait exists to act on them, and a default that is only
// checked once some later task makes it observable is a default nothing checks.
// New's validation is exercised through the same exported surface a caller uses.

import (
	"context"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// stubClock is a Clock that does nothing. New only stores a clock, so identity
// is all these tests need — and draintest cannot be imported here, since an
// internal test in package drain would make the dependency a cycle.
type stubClock struct{ name string }

func (stubClock) Now() time.Time                        { return time.Time{} }
func (stubClock) AfterFunc(time.Duration, func()) Timer { return stubTimer{} }

type stubTimer struct{}

func (stubTimer) Stop() bool { return false }

// TestNewValidation covers every rejection listed on New's doc comment. The two
// boundaries are asymmetric on purpose and both are here: a deadline equal to
// the grace period is valid, a settle window equal to it is not.
func TestNewValidation(t *testing.T) {
	t.Parallel()

	const grace = 30 * time.Second

	for _, tc := range []struct {
		name   string
		grace  time.Duration
		opts   []Option
		field  string // expected InvalidPolicyError.Field; "" means the policy is valid
		reason Reason // expected InvalidPolicyError.Reason; "" for a failure not scoped to one
	}{
		{name: "zero grace", grace: 0, field: "gracePeriod"},
		{name: "negative grace", grace: -time.Second, field: "gracePeriod"},
		{
			name:  "negative settle",
			grace: grace,
			opts:  []Option{WithPreDrainDelay(-time.Second)},
			field: "preDrainDelay",
		},
		{
			name:  "settle equals grace",
			grace: grace,
			opts:  []Option{WithPreDrainDelay(grace)},
			field: "preDrainDelay",
		},
		{
			name:  "settle exceeds grace",
			grace: grace,
			opts:  []Option{WithPreDrainDelay(grace + time.Second)},
			field: "preDrainDelay",
		},
		{
			name:   "deadline exceeds grace",
			grace:  grace,
			opts:   []Option{WithDeadline("r", grace+time.Second)},
			field:  "deadline",
			reason: "r",
		},
		{
			name:   "negative deadline",
			grace:  grace,
			opts:   []Option{WithDeadline("r", -time.Second)},
			field:  "deadline",
			reason: "r",
		},
		{
			// The whole point of the Reason field: Field alone cannot say which
			// of two WithDeadline options was wrong, and two is the normal case.
			name:  "the offending deadline among several",
			grace: grace,
			opts: []Option{
				WithDeadline("harness/pending_gate", 0),
				WithDeadline("harness/active_turn", grace),
				WithDeadline("harness/subprocess", grace+time.Nanosecond),
			},
			field:  "deadline",
			reason: "harness/subprocess",
		},
		{
			// A reason is the one genuinely caller-supplied string here, so it
			// must reach the message through a quoted path.
			name:   "a deadline for the unspecified reason",
			grace:  grace,
			opts:   []Option{WithDeadline(ReasonUnspecified, -time.Nanosecond)},
			field:  "deadline",
			reason: ReasonUnspecified,
		},
		{
			name:  "negative idle",
			grace: grace,
			opts:  []Option{WithIdleTimeout(-time.Second)},
			field: "idleTimeout",
		},
		{
			name:  "uncatchable signal",
			grace: grace,
			opts:  []Option{WithSignals(syscall.SIGKILL)},
			field: "signals",
		},
		{
			name:  "uncatchable stop",
			grace: grace,
			opts:  []Option{WithSignals(syscall.SIGSTOP)},
			field: "signals",
		},
		{
			// Rejected, not quietly dropped from an otherwise usable set.
			name:  "an uncatchable signal among catchable ones",
			grace: grace,
			opts:  []Option{WithSignals(syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)},
			field: "signals",
		},
		{name: "defaults", grace: grace},
		{
			name:  "deadline equal to grace is valid",
			grace: grace,
			opts:  []Option{WithDeadline("r", grace)},
		},
		{
			name:  "zero deadline is valid",
			grace: grace,
			opts:  []Option{WithDeadline("r", 0)},
		},
		{
			name:  "settle one nanosecond below grace is valid",
			grace: grace,
			opts:  []Option{WithPreDrainDelay(grace - time.Nanosecond)},
		},
		{
			name:  "zero settle is valid",
			grace: grace,
			opts:  []Option{WithPreDrainDelay(0)},
		},
		{
			name:  "zero idle timeout is valid",
			grace: grace,
			opts:  []Option{WithIdleTimeout(0)},
		},
		{
			name:  "an idle timeout longer than grace is valid",
			grace: grace,
			opts:  []Option{WithIdleTimeout(time.Hour)},
		},
		{
			name:  "a one nanosecond grace period is valid",
			grace: time.Nanosecond,
			opts:  []Option{WithPreDrainDelay(0)},
		},
		{
			name:  "no signals is valid",
			grace: grace,
			opts:  []Option{WithSignals()},
		},
		{
			name:  "a nil clock is ignored rather than rejected",
			grace: grace,
			opts:  []Option{WithClock(nil)},
		},
		{
			name:  "every option together",
			grace: grace,
			opts: []Option{
				WithPreDrainDelay(2 * time.Second),
				WithIdleTimeout(10 * time.Minute),
				WithDeadline("harness/active_turn", 20*time.Second),
				WithDeadline("harness/pending_gate", 0),
				WithOnQuiesce(func(Cause) {}),
				WithOnDeadline(func(context.Context, Reason, int) {}),
				WithSignals(syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP),
				WithClock(stubClock{name: "test"}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := New(tc.grace, tc.opts...)

			if tc.field == "" {
				if err != nil {
					t.Fatalf("New() = %v, want a valid coordinator", err)
				}
				if c == nil {
					t.Fatal("New() returned a nil Coordinator with a nil error")
				}
				return
			}

			if c != nil {
				t.Errorf("New() returned a non-nil Coordinator alongside error %v", err)
			}
			var invalid *InvalidPolicyError
			if !errors.As(err, &invalid) {
				t.Fatalf("New() = %v (%T), want an *InvalidPolicyError", err, err)
			}
			if invalid.Field != tc.field {
				t.Errorf("Field = %q, want %q (error: %v)", invalid.Field, tc.field, err)
			}
			if invalid.Reason != tc.reason {
				t.Errorf("Reason = %q, want %q (error: %v)", invalid.Reason, tc.reason, err)
			}
		})
	}
}

// TestNewValidatesGracePeriodFirst pins the ordering that makes the table above
// unambiguous: a non-positive grace period also makes the default 5s settle
// window illegal, and the error must name the parameter the caller passed
// rather than a default they never set.
func TestNewValidatesGracePeriodFirst(t *testing.T) {
	t.Parallel()

	_, err := New(0)
	var invalid *InvalidPolicyError
	if !errors.As(err, &invalid) {
		t.Fatalf("New(0) = %v, want an *InvalidPolicyError", err)
	}
	if invalid.Field != "gracePeriod" {
		t.Errorf("Field = %q, want %q", invalid.Field, "gracePeriod")
	}
}

// TestNewNamesTheUncatchableSignal covers the diagnosis, not just the refusal:
// WithSignals replaces the whole set, so an error that named only the option
// would leave the caller to work out which of the signals they passed is the
// one that cannot be caught. The rendered name is the platform's — "stopped
// (signal)" on Linux, "suspended (signal)" on Darwin — so the expectation is
// built from the signal rather than written out.
func TestNewNamesTheUncatchableSignal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		uncatchable os.Signal
		catchable   os.Signal
	}{
		{"kill", syscall.SIGKILL, syscall.SIGTERM},
		{"stop", syscall.SIGSTOP, syscall.SIGINT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(30*time.Second, WithSignals(tc.catchable, tc.uncatchable))
			if err == nil {
				t.Fatalf("New() with %v = nil, want an error", tc.uncatchable)
			}

			got := err.Error()
			want := "signal " + strconv.Quote(tc.uncatchable.String()) + " cannot be caught"
			if !strings.Contains(got, want) {
				t.Errorf("Error() = %q, want it to mention %q", got, want)
			}
			if strings.Contains(got, tc.catchable.String()) {
				t.Errorf("Error() = %q, want it to name only the uncatchable signal", got)
			}
		})
	}
}

// TestNewDefaults pins the defaults themselves. The 5s settle window is the one
// with a caller-visible edge — see TestNewRejectsTheDefaultSettleWindowAgainstAShortGrace.
func TestNewDefaults(t *testing.T) {
	t.Parallel()

	c, err := New(30 * time.Second)
	if err != nil {
		t.Fatalf("New() = %v, want a valid coordinator", err)
	}

	if got, want := c.gracePeriod, 30*time.Second; got != want {
		t.Errorf("gracePeriod = %v, want %v", got, want)
	}
	if got, want := c.cfg.preDrainDelay, 5*time.Second; got != want {
		t.Errorf("default preDrainDelay = %v, want %v", got, want)
	}
	if got := c.cfg.idleTimeout; got != 0 {
		t.Errorf("default idleTimeout = %v, want 0 — idle self-exit is opt-in", got)
	}
	if got := c.cfg.deadlines; len(got) != 0 {
		t.Errorf("default deadlines = %v, want none — an unconfigured reason waits the full grace period", got)
	}
	if got := c.cfg.onQuiesce; got != nil {
		t.Error("default onQuiesce is non-nil, want nil")
	}
	if got := c.cfg.onDeadline; got != nil {
		t.Error("default onDeadline is non-nil, want nil")
	}
	if want := []os.Signal{syscall.SIGTERM, syscall.SIGINT}; !slices.Equal(c.cfg.signals, want) {
		t.Errorf("default signals = %v, want %v", c.cfg.signals, want)
	}
	if _, ok := c.cfg.clock.(systemClock); !ok {
		t.Errorf("default clock = %T, want systemClock", c.cfg.clock)
	}
}

// TestNewRejectsTheDefaultSettleWindowAgainstAShortGrace is the default's one
// observable edge: PreDrainDelay < GracePeriod is strict and the default is
// exactly 5s, so a 5s budget must lower the settle window rather than inherit
// one that consumes all of it.
func TestNewRejectsTheDefaultSettleWindowAgainstAShortGrace(t *testing.T) {
	t.Parallel()

	if _, err := New(5 * time.Second); err == nil {
		t.Error("New(5s) with the default 5s settle window = nil, want an error")
	}
	if _, err := New(5*time.Second + time.Nanosecond); err != nil {
		t.Errorf("New(5s+1ns) = %v, want a valid coordinator", err)
	}
	if _, err := New(5*time.Second, WithPreDrainDelay(time.Second)); err != nil {
		t.Errorf("New(5s) with a 1s settle window = %v, want a valid coordinator", err)
	}
}

// TestOptionsRecordIntentWithoutValidating pins the division of labour: an
// option writes what the caller asked for, however impossible, and New is the
// only thing that judges it. An option that clamped instead would make a
// misconfiguration invisible.
func TestOptionsRecordIntentWithoutValidating(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		opt  Option
		want func(*testing.T, config)
	}{
		{
			name: "a negative settle window is recorded",
			opt:  WithPreDrainDelay(-time.Hour),
			want: func(t *testing.T, cfg config) {
				if cfg.preDrainDelay != -time.Hour {
					t.Errorf("preDrainDelay = %v, want -1h", cfg.preDrainDelay)
				}
			},
		},
		{
			name: "a negative idle timeout is recorded",
			opt:  WithIdleTimeout(-time.Hour),
			want: func(t *testing.T, cfg config) {
				if cfg.idleTimeout != -time.Hour {
					t.Errorf("idleTimeout = %v, want -1h", cfg.idleTimeout)
				}
			},
		},
		{
			name: "an impossible deadline is recorded",
			opt:  WithDeadline("r", -time.Hour),
			want: func(t *testing.T, cfg config) {
				if cfg.deadlines["r"] != -time.Hour {
					t.Errorf("deadlines[r] = %v, want -1h", cfg.deadlines["r"])
				}
			},
		},
		{
			name: "an uncatchable signal is recorded",
			opt:  WithSignals(syscall.SIGKILL),
			want: func(t *testing.T, cfg config) {
				if !slices.Equal(cfg.signals, []os.Signal{syscall.SIGKILL}) {
					t.Errorf("signals = %v, want [killed]", cfg.signals)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultConfig()
			tc.opt(&cfg)
			tc.want(t, cfg)
		})
	}
}

// TestOptionOverrides covers what happens when the same option is given twice,
// which a composition root does by accident whenever it layers a caller's
// options over its own.
func TestOptionOverrides(t *testing.T) {
	t.Parallel()

	t.Run("the last deadline for a reason wins", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		WithDeadline("r", time.Second)(&cfg)
		WithDeadline("r", 2*time.Second)(&cfg)
		WithDeadline("other", 3*time.Second)(&cfg)

		if got, want := cfg.deadlines["r"], 2*time.Second; got != want {
			t.Errorf("deadlines[r] = %v, want %v", got, want)
		}
		if got, want := cfg.deadlines["other"], 3*time.Second; got != want {
			t.Errorf("deadlines[other] = %v, want %v", got, want)
		}
	})

	t.Run("signals are replaced, not accumulated", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		WithSignals(syscall.SIGHUP)(&cfg)
		WithSignals(syscall.SIGUSR1, syscall.SIGUSR2)(&cfg)

		want := []os.Signal{syscall.SIGUSR1, syscall.SIGUSR2}
		if !slices.Equal(cfg.signals, want) {
			t.Errorf("signals = %v, want %v", cfg.signals, want)
		}
	})

	t.Run("the signal set is copied out of the caller's slice", func(t *testing.T) {
		t.Parallel()

		sigs := []os.Signal{syscall.SIGTERM}
		cfg := defaultConfig()
		WithSignals(sigs...)(&cfg)
		sigs[0] = syscall.SIGKILL

		if !slices.Equal(cfg.signals, []os.Signal{syscall.SIGTERM}) {
			t.Errorf("signals = %v, want [terminated] — the caller's slice must not be aliased", cfg.signals)
		}
	})
}

// TestWithClockNilIsIgnored covers the fail-safe option convention: a nil clock
// leaves the coordinator with a working one rather than failing construction or,
// worse, storing a nil that panics at the first Now.
func TestWithClockNilIsIgnored(t *testing.T) {
	t.Parallel()

	t.Run("with no other clock", func(t *testing.T) {
		t.Parallel()

		c, err := New(30*time.Second, WithClock(nil))
		if err != nil {
			t.Fatalf("New() = %v, want a valid coordinator", err)
		}
		if _, ok := c.cfg.clock.(systemClock); !ok {
			t.Errorf("clock = %T, want systemClock", c.cfg.clock)
		}
	})

	t.Run("after a real clock", func(t *testing.T) {
		t.Parallel()

		clock := stubClock{name: "injected"}
		c, err := New(30*time.Second, WithClock(clock), WithClock(nil))
		if err != nil {
			t.Fatalf("New() = %v, want a valid coordinator", err)
		}
		if got, ok := c.cfg.clock.(stubClock); !ok || got != clock {
			t.Errorf("clock = %#v, want the injected %#v — nil must be ignored, not applied", c.cfg.clock, clock)
		}
	})
}

// TestNewIgnoresNilOption keeps a nil in a caller's option slice from panicking.
// A coordinator whose one job is graceful termination has no business killing
// the process over a nil in a slice it was handed.
func TestNewIgnoresNilOption(t *testing.T) {
	t.Parallel()

	c, err := New(30*time.Second, nil, WithPreDrainDelay(time.Second), nil)
	if err != nil {
		t.Fatalf("New() = %v, want a valid coordinator", err)
	}
	if got, want := c.cfg.preDrainDelay, time.Second; got != want {
		t.Errorf("preDrainDelay = %v, want %v — the surrounding options must still apply", got, want)
	}
}

// TestNewDoesNotShareStateAcrossCoordinators covers the composition root that
// builds a []Option once and constructs from it twice. Each coordinator must own
// its policy: the deadline map is the only reference type in it, and a shared
// one would let a mutation of either coordinator's policy reach the other.
func TestNewDoesNotShareStateAcrossCoordinators(t *testing.T) {
	t.Parallel()

	opts := []Option{WithDeadline("r", time.Second)}
	first, err := New(30*time.Second, opts...)
	if err != nil {
		t.Fatalf("New() = %v, want a valid coordinator", err)
	}
	second, err := New(30*time.Second, opts...)
	if err != nil {
		t.Fatalf("New() = %v, want a valid coordinator", err)
	}

	first.cfg.deadlines["r"] = time.Hour
	if got, want := second.cfg.deadlines["r"], time.Second; got != want {
		t.Errorf("second coordinator's deadline for r = %v, want %v — the map is shared", got, want)
	}
}
