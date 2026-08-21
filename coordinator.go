package drain

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
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

	// mu guards the four fields below as ONE critical section, and the grouping
	// is load-bearing rather than tidy. A mutex that guarded "only the counters"
	// admits this interleaving: Acquire reads admission as open, the sequence
	// closes admission and snapshots the counters as empty, Acquire then
	// increments and returns ok == true — a hold that is never waited for and
	// never reported. So Acquire's admission check and its increment are one
	// critical section, as closing admission and snapshotting the outstanding
	// reasons will be.
	//
	// No user callback ever runs while this is held. That is what lets an owner
	// call release() from inside OnDeadline, which is the expected shape of
	// abandonment, without deadlocking against the coordinator.
	mu sync.Mutex

	// holds counts outstanding holds per reason. A reason is deleted when its
	// count reaches zero, so the map never accumulates keys for work that
	// finished and len(holds) == 0 means exactly "nothing outstanding".
	holds map[Reason]int

	// total is the sum of holds. It is kept alongside rather than summed on
	// demand because the wait phase's exit condition is "the total reached zero"
	// and the idle timer's arming is a transition of this number, both of which
	// are decisions made under this lock on every release.
	total int

	// admissionOpen reports whether Acquire still grants. It is NOT the draining
	// flag: for CauseSignal the coordinator is draining while admission stays
	// open through the settle window, because the requests already routed here
	// when the signal arrived still have to land. The two separate at the settle
	// boundary.
	admissionOpen bool

	// draining reports whether the sequence has begun. It is written once, by
	// the Begin that transitions it from false to true.
	draining bool
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
	return &Coordinator{
		gracePeriod:   gracePeriod,
		cfg:           cfg,
		holds:         make(map[Reason]int),
		admissionOpen: true,
	}, nil
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

// noopRelease is the release returned by a refused Acquire. It is a package
// value rather than a fresh closure per refusal because every refusal wants the
// same nothing, and because a shared inert function makes it obvious that a
// refused Acquire took no state with it.
//
// Returning this instead of nil is the module's fail-secure default: the shape
// every consumer writes is
//
//	release, ok := c.Acquire(r)
//	defer release()
//
// and a nil release would turn "we are shutting down" into a panic on the way
// out — a shutdown coordinator crashing the process it was asked to retire
// gracefully.
func noopRelease() {}

// Acquire registers a hold for reason r and reports whether it was granted. ok
// is false once admission has closed — at Begin for the idle, explicit, and
// context causes; at the settle boundary for a signal — and forever after the
// sequence completes.
//
// release is never nil. When ok is false it is a no-op, so
//
//	release, ok := c.Acquire(r)
//	defer release()
//
// is always safe. release is idempotent per returned closure, not per reason:
// each Acquire yields its own once-guarded release, and calling one twice
// decrements once. release remains valid after Wait returns, where it still
// decrements so Holds stays truthful for a final metrics scrape, and it is safe
// to call from inside an OnDeadline callback — that is the expected shape of
// abandonment, where the owner interrupts work whose deferred release then runs
// on the callback's stack.
//
// Acquire's ok is the only admission authority. Draining is advisory.
func (c *Coordinator) Acquire(r Reason) (release func(), ok bool) {
	c.mu.Lock()
	if !c.admissionOpen {
		c.mu.Unlock()
		return noopRelease, false
	}
	c.holds[r]++
	c.total++
	c.mu.Unlock()

	// One sync.Once per returned closure, never one per reason: two holds on one
	// reason are two counts and two independent releases, so a per-reason guard
	// would let the first release swallow the second hold's count and strand the
	// coordinator waiting for work that had finished.
	var once sync.Once
	return func() { once.Do(func() { c.releaseHold(r) }) }, true
}

// releaseHold decrements one hold of reason r. It is only ever reached through a
// once-guarded closure, so it cannot be called more times than the reason was
// acquired and the counters cannot go negative.
//
// It takes c.mu and no other lock, which is the whole of the "release never
// takes a lock a callback might already hold" contract: c.mu is the
// coordinator's only lock and no user callback ever runs under it.
func (c *Coordinator) releaseHold(r Reason) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.total--
	if c.holds[r]--; c.holds[r] <= 0 {
		// Deleted rather than left at zero so that the map is the set of
		// reasons actually outstanding. Holds omits zero counts either way, but
		// a map that only ever grew would keep every reason the process has
		// ever used alive for the life of the coordinator.
		delete(c.holds, r)
	}
}

// Draining reports whether the sequence has begun. It is advisory — suitable for
// a readiness probe or a log line. Consulting it before Acquire is a TOCTOU
// race; call Acquire and check ok.
func (c *Coordinator) Draining() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.draining
}

// Holds returns a snapshot copy of the outstanding hold count per reason,
// omitting reasons at zero. The returned map is never the coordinator's own.
// This is a liveness signal ("can I exit"), not an occupancy signal ("how full
// am I") — see the module's non-goals.
//
// Holds returns nil when no holds are outstanding; it never returns an empty
// non-nil map, matching Outcome's map policy.
func (c *Coordinator) Holds() map[Reason]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.holdsLocked()
}

// holdsLocked snapshots the counters. The caller must hold c.mu.
//
// The zero-count filter is redundant against releaseHold's delete and is kept
// anyway: it makes the omit-zero guarantee a property of the snapshot itself
// rather than a consequence of a rule enforced somewhere else, so a later
// change to how counters are retired cannot leak a zero to a caller.
func (c *Coordinator) holdsLocked() map[Reason]int {
	if c.total == 0 {
		// nil, never an empty non-nil map: maps.Clone of an empty map returns
		// one, which would let m == nil and len(m) == 0 disagree for a caller.
		return nil
	}
	snapshot := make(map[Reason]int, len(c.holds))
	for r, count := range c.holds {
		if count > 0 {
			snapshot[r] = count
		}
	}
	if len(snapshot) == 0 {
		return nil
	}
	return snapshot
}
