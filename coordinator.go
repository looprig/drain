package drain

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"syscall"
	"time"
)

// Validation causes. They are package-level so that every construction site in
// New states the same thing the same way, and so the wording of a policy failure
// is one edit rather than five. Each is carried as InvalidPolicyError.Err, which
// is a detail cause inside a typed error rather than an error minted at a return
// site.
var (
	errNotPositive         = errors.New("must be positive")
	errNegative            = errors.New("must not be negative")
	errNotBelowGracePeriod = errors.New("must be strictly less than the grace period")
	errAboveGracePeriod    = errors.New("must not exceed the grace period")
	errUncatchableSignal   = errors.New("cannot be caught")
)

// Coordinator holds the drain state for one process. The zero value is not
// usable; construct with New. All exported methods are safe from any goroutine.
type Coordinator struct {
	// Policy, fixed at construction and never written again, so it needs no
	// lock: gracePeriod is the hard ceiling over the whole sequence and cfg is
	// everything the options set.
	gracePeriod time.Duration
	cfg         config
}

// New returns a Coordinator whose sequence is bounded by gracePeriod, measured
// from Begin. All validation happens here, in one place: New returns
// *InvalidPolicyError for a non-positive gracePeriod, any negative duration, a
// PreDrainDelay not strictly less than gracePeriod, a per-reason deadline greater
// than gracePeriod, or an uncatchable signal (SIGKILL, SIGSTOP). Options never
// silently drop a bad value.
//
// A nil Option is ignored, as a nil Clock is. Both are fail-safe: a coordinator
// whose one job is graceful termination has no business panicking over a hole in
// a slice it was handed.
func New(gracePeriod time.Duration, opts ...Option) (*Coordinator, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	if err := validate(gracePeriod, cfg); err != nil {
		return nil, err
	}

	// No defensive copy of cfg.deadlines: WithDeadline builds its map inside the
	// closure, against this call's own config, so two coordinators constructed
	// from one []Option already have separate maps and nothing outside New ever
	// held a reference to this one. TestNewDoesNotShareStateAcrossCoordinators
	// pins that, so a future option that took a caller's map fails there.
	return &Coordinator{gracePeriod: gracePeriod, cfg: cfg}, nil
}

// validate is New's single validation pass. The grace period is checked first
// because it is the value every other bound is expressed against: a report that
// the default settle window exceeds a grace period of zero would name a field
// the caller never set.
func validate(gracePeriod time.Duration, cfg config) error {
	if gracePeriod <= 0 {
		return &InvalidPolicyError{Field: "gracePeriod", Err: errNotPositive}
	}
	if cfg.preDrainDelay < 0 {
		return &InvalidPolicyError{Field: "preDrainDelay", Err: errNegative}
	}
	// Strict, unlike the deadline bound below: a settle window equal to the
	// grace period would spend the entire budget before the wait phase began.
	if cfg.preDrainDelay >= gracePeriod {
		return &InvalidPolicyError{Field: "preDrainDelay", Err: errNotBelowGracePeriod}
	}
	if cfg.idleTimeout < 0 {
		return &InvalidPolicyError{Field: "idleTimeout", Err: errNegative}
	}
	if err := validateDeadlines(gracePeriod, cfg.deadlines); err != nil {
		return err
	}
	return validateSignals(cfg.signals)
}

// validateDeadlines reports the first reason, in sorted order, whose deadline
// cannot be honoured. Sorted rather than in map order because a constructor that
// named a different one of two bad deadlines on each run would be untestable and
// unreportable.
//
// A deadline equal to the grace period is legal, where a settle window equal to
// it is not: deadlines are measured from Begin and simply expire with the
// ceiling, so nothing is truncated.
func validateDeadlines(gracePeriod time.Duration, deadlines map[Reason]time.Duration) error {
	for _, r := range slices.Sorted(maps.Keys(deadlines)) {
		switch d := deadlines[r]; {
		case d < 0:
			return &InvalidPolicyError{Field: "deadline", Reason: r, Err: errNegative}
		case d > gracePeriod:
			return &InvalidPolicyError{Field: "deadline", Reason: r, Err: errAboveGracePeriod}
		}
	}
	return nil
}

// validateSignals rejects the two signals no process can catch. Accepting them
// would install a handler that never runs, so a drain configured entirely around
// SIGKILL would report a healthy coordinator and then die undrained.
//
// The cause names the offending signal because the set is replaced wholesale by
// WithSignals: "signals" alone leaves the caller to find which of the three they
// passed is the problem.
func validateSignals(sigs []os.Signal) error {
	for _, sig := range sigs {
		if sig == syscall.SIGKILL || sig == syscall.SIGSTOP {
			return &InvalidPolicyError{
				Field: "signals",
				Err:   fmt.Errorf("signal %q %w", sig.String(), errUncatchableSignal),
			}
		}
	}
	return nil
}
