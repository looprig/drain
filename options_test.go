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
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
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

// optionConstructors is every exported function in this package that returns an
// Option, paired with an invocation that carries a value. It is the registry the
// no-defensive-copy reasoning in New rests on: that reasoning is a property of
// the current option set rather than of New, so it has to be re-checked whenever
// the set grows, and a check that names one option checks one option.
//
// TestOptionConstructorsAreAllRegistered fails when a new constructor is not
// added here, and TestNewDoesNotShareStateAcrossCoordinators then builds two
// coordinators from the whole registry and asserts neither can mutate the
// other's policy. The pair is what makes the claim general: the earlier version
// of this test exercised WithDeadline alone, so a hypothetical
// WithDeadlines(map[Reason]time.Duration) that assigned the caller's map
// straight into cfg aliased it into every coordinator built from it and the
// suite still passed.
//
// Every value here must be legal for the grace period the sharing test uses.
var optionConstructors = map[string]Option{
	"WithPreDrainDelay": WithPreDrainDelay(time.Second),
	"WithIdleTimeout":   WithIdleTimeout(time.Second),
	"WithDeadline":      WithDeadline(registeredReason, time.Second),
	"WithOnQuiesce":     WithOnQuiesce(func(Cause) {}),
	"WithOnDeadline":    WithOnDeadline(func(context.Context, Reason, int) {}),
	"WithSignals":       WithSignals(syscall.SIGTERM, syscall.SIGINT),
	"WithClock":         WithClock(stubClock{name: "registered"}),
}

// registeredReason is the reason the registry's WithDeadline entry configures.
const registeredReason Reason = "drain/registered"

// registeredOptions returns every registered option in a deterministic order.
// Order is irrelevant to what the sharing test asserts, but a set of options
// that shuffled per run would make a failure hard to reproduce.
func registeredOptions() []Option {
	opts := make([]Option, 0, len(optionConstructors))
	for _, name := range slices.Sorted(maps.Keys(optionConstructors)) {
		opts = append(opts, optionConstructors[name])
	}
	return opts
}

// TestOptionConstructorsAreAllRegistered is the enforcement half of the pair. It
// parses this package's own source for exported functions returning an Option
// and requires the registry to name every one of them, so a new option cannot
// quietly escape the sharing test the way a hand-maintained list would let it.
//
// go/ast rather than reflection because reflection cannot enumerate a package's
// functions, and the whole point is to find the constructor nobody remembered.
// Both are standard library, which this module's dependency rule requires.
func TestOptionConstructorsAreAllRegistered(t *testing.T) {
	t.Parallel()

	declared := declaredOptionConstructors(t)
	if len(declared) == 0 {
		t.Fatal("found no exported Option constructors in the package source; the parser is not seeing this package")
	}

	for _, name := range declared {
		if _, ok := optionConstructors[name]; !ok {
			t.Errorf("%s returns an Option but is not in optionConstructors; add it, "+
				"so TestNewDoesNotShareStateAcrossCoordinators covers it", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(optionConstructors)) {
		if !slices.Contains(declared, name) {
			t.Errorf("optionConstructors names %s, which is not an exported Option constructor in this package", name)
		}
	}
}

// declaredOptionConstructors reports the exported, receiverless functions in
// this package's non-test files whose single result is an Option.
func declaredOptionConstructors(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}

	var names []string
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		// ParseFile rather than the deprecated ParseDir, and declarations only:
		// nothing here needs a function body.
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && returnsOption(fn) {
				names = append(names, fn.Name.Name)
			}
		}
	}
	slices.Sort(names)
	return names
}

// returnsOption reports whether fn is an exported package-level function whose
// only result is an Option.
func returnsOption(fn *ast.FuncDecl) bool {
	if fn.Recv != nil || !fn.Name.IsExported() {
		return false
	}
	results := fn.Type.Results
	if results == nil || len(results.List) != 1 || len(results.List[0].Names) > 1 {
		return false
	}
	ident, ok := results.List[0].Type.(*ast.Ident)
	return ok && ident.Name == "Option"
}

// TestNewDoesNotShareStateAcrossCoordinators covers the composition root that
// builds a []Option once and constructs from it twice. Each coordinator must own
// its policy: a shared reference would let a mutation of either coordinator's
// policy — or of the caller's own value, still held after New returned — reach
// the other.
//
// It is driven from optionConstructors rather than from one named option,
// because the claim New relies on is about the option set as a whole. Funcs and
// the Clock are deliberately excluded: they are behaviour the caller means to
// share, not policy a coordinator owns.
func TestNewDoesNotShareStateAcrossCoordinators(t *testing.T) {
	t.Parallel()

	// A compile-time pin on config's field list, unkeyed on purpose: a new field
	// breaks this line, which is the prompt to decide whether it is mutable
	// state two coordinators could share and, if it is, to check it below.
	var _ = config{0, 0, nil, nil, nil, nil, nil}

	opts := registeredOptions()
	first, err := New(30*time.Second, opts...)
	if err != nil {
		t.Fatalf("New() = %v, want a valid coordinator", err)
	}
	second, err := New(30*time.Second, opts...)
	if err != nil {
		t.Fatalf("New() = %v, want a valid coordinator", err)
	}

	// The deadline map.
	if got, want := second.cfg.deadlines[registeredReason], time.Second; got != want {
		t.Fatalf("second coordinator's deadline for %q = %v, want %v", registeredReason, got, want)
	}
	first.cfg.deadlines[registeredReason] = time.Hour
	if got, want := second.cfg.deadlines[registeredReason], time.Second; got != want {
		t.Errorf("second coordinator's deadline for %q = %v after mutating the first's, want %v — the map is shared",
			registeredReason, got, want)
	}

	// The signal slice.
	if got, want := second.cfg.signals[0], os.Signal(syscall.SIGTERM); got != want {
		t.Fatalf("second coordinator's first signal = %v, want %v", got, want)
	}
	first.cfg.signals[0] = syscall.SIGHUP
	if got, want := second.cfg.signals[0], os.Signal(syscall.SIGTERM); got != want {
		t.Errorf("second coordinator's first signal = %v after mutating the first's, want %v — the slice is shared", got, want)
	}
}
