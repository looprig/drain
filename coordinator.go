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

	// draining reports whether the sequence has begun, cause is the first cause
	// recorded, and beganAt is when. All three are written once, by the Begin
	// that transitions draining from false to true, which is what makes
	// Outcome.Cause deterministic under a race between a signal and the idle
	// timer. beganAt is the epoch every budget in the sequence is measured
	// from: the settle window, the per-reason deadlines, and the grace ceiling.
	draining bool
	cause    Cause
	beganAt  time.Time

	// begun is closed by that same Begin. Wait blocks on it, so the sequence
	// runs on Wait's goroutine no matter which goroutine started it — a signal
	// handler is not a place to run a drain.
	begun chan struct{}

	// waiting records that Wait has been claimed. A coordinator runs its
	// sequence once and is permanently spent afterwards; a second Wait,
	// concurrent or after the first returned, gets ErrAlreadyRunning.
	waiting bool

	// cut records that phase 3 has run, and cutoff is what it saw. The cut is
	// idempotent because two things can reach it — the settle timer's callback
	// and a cancelled context — and the wait phase must be owed exactly one
	// snapshot however it got there.
	cut    bool
	cutoff cutoff

	// drained is closed when the holds the cutoff named have all released. It
	// is the send side of cutoff.drained, kept separately because that one is
	// receive-only, and it is set to nil once closed so the close cannot
	// happen twice. Admission is shut before it exists, so the total only ever
	// falls afterwards and reaches zero exactly once.
	drained chan struct{}
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

	// No defensive copy of cfg. Every option that writes a reference type builds
	// it inside the closure, against this call's own config — WithDeadline its
	// map, WithSignals its slice — so two coordinators constructed from one
	// []Option already own separate copies and nothing outside New ever held a
	// reference to either.
	//
	// That is a property of the current option set, not of New. An option that
	// assigned a caller's map or slice straight into cfg would alias it into
	// every coordinator built from it, and New would not notice. It is checked
	// rather than assumed: optionConstructors in options_test.go registers every
	// exported Option constructor, TestOptionConstructorsAreAllRegistered parses
	// this package and fails when a new one is missing from it, and
	// TestNewDoesNotShareStateAcrossCoordinators constructs two coordinators
	// from the whole registry and asserts neither can mutate the other's
	// policy. Registering a new option is therefore not optional.
	return &Coordinator{
		gracePeriod:   gracePeriod,
		cfg:           cfg,
		holds:         make(map[Reason]int),
		admissionOpen: true,
		begun:         make(chan struct{}),
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
	if !c.admit(r) {
		return noopRelease, false
	}

	// One sync.Once per returned closure, never one per reason: two holds on one
	// reason are two counts and two independent releases, so a per-reason guard
	// would let the first release swallow the second hold's count and strand the
	// coordinator waiting for work that had finished. It is allocated outside
	// the critical section because nothing under the lock depends on it.
	var once sync.Once
	return func() { once.Do(func() { c.releaseHold(r) }) }, true
}

// admit is Acquire's critical section: it reads the admission flag and
// increments the counters under one acquisition of c.mu, and reports whether the
// hold was granted.
//
// It is a function of its own, with a deferred unlock, rather than an inline
// section with two explicit unlocks. Both are correct today, but this is the
// section the sequence races: an implementation that read the flag, dropped the
// lock and then incremented would grant holds the cut has already snapshotted
// past, which TestAdmissionCutoffAndSnapshotAreOneCriticalSection exists to
// catch. Code that will grow — Task 11 adds the idle generation bump on the
// zero→nonzero transition, here, inside this same section — should not grow
// around a hand-balanced unlock, where an inserted early return leaks the mutex.
//
// The name deviates from the -Locked suffix used elsewhere in this file
// (holdsLocked) on purpose: that suffix means the caller must already hold the
// lock, and this takes it.
func (c *Coordinator) admit(r Reason) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.admissionOpen {
		return false
	}
	c.holds[r]++
	c.total++
	return true
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

	// The wait phase's exit condition. drained is nil until the cut installs it
	// and again once it has fired, so a release before the drain, or after the
	// sequence has already ended, simply decrements.
	if c.total == 0 && c.drained != nil {
		close(c.drained)
		c.drained = nil
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

// Begin records the cause and starts the sequence. The first cause wins; later
// calls are no-ops including their cause, so Outcome.Cause is deterministic.
//
// The valid arguments are CauseSignal, CauseIdle, CauseExplicit, and
// CauseContext. Any other value — including CauseUnknown, which names the
// not-yet-started state and is never a valid argument — is recorded as
// CauseExplicit: that is the literal truth (Begin was called directly), and it
// takes the fail-secure admission path, closing admission at Begin rather than
// inheriting CauseSignal's open settle window. CauseUnknown is therefore never
// observable on a started sequence.
//
// Begin does not panic on an out-of-range cause. It is called from signal
// handlers and shutdown paths, and a graceful-termination coordinator that kills
// the process over a taxonomy slip has failed at its one job.
//
// Begin returns promptly and never runs a user callback on its caller's
// goroutine — the sequence executes inside Wait, which proceeds immediately if
// entered after Begin.
func (c *Coordinator) Begin(cause Cause) {
	c.begin(cause)
}

// begin is Begin's critical section, reporting whether this call is the one that
// started the sequence. The sequence itself needs that answer — the idle timer
// and a cancelled context both call it speculatively, and only the caller that
// wins may act as though it started a drain — while Begin's public contract is
// simply that it is a no-op the second time.
//
// The cause, the draining flag, and the admission flag are written in ONE
// critical section. They are three decisions from one fact, and an Acquire that
// observed the admission flag of a drain whose cause had not been recorded yet
// would be reading a state the coordinator was never in.
func (c *Coordinator) begin(cause Cause) bool {
	cause = normalizeCause(cause)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.draining {
		return false
	}
	c.draining = true
	c.cause = cause
	// Read under the lock, alongside the flags it belongs with. Taking c.mu and
	// then the clock's is the module's only lock order and it has no reverse
	// edge: draintest runs timer callbacks with its own lock released.
	c.beganAt = c.cfg.clock.Now()

	// The admission flag is NOT the draining flag. A signal drain is draining
	// with admission still open, because endpoint withdrawal is asynchronous and
	// requests routed here before the signal arrived are still arriving:
	// refusing them immediately turns a graceful shutdown into a burst of 503s.
	// Its cutoff comes later, at the settle boundary. Every other cause is
	// self-inflicted — nothing external is still routing work here — so
	// admission closes now.
	//
	// Written as a close rather than an assignment: admission is a one-way
	// latch, and a later Begin must never be able to reopen it.
	if cause != CauseSignal {
		c.admissionOpen = false
	}

	// Released last, so a Wait woken by it observes a fully recorded sequence.
	close(c.begun)
	return true
}

// normalizeCause maps every value outside the set of valid Begin arguments onto
// CauseExplicit, which is both the literal truth about such a call and the
// fail-secure choice: the alternative — treating an unrecognized cause as a
// signal — would hand an unknown caller the one path that keeps admission open.
//
// CauseUnknown is normalized like any other invalid value rather than special
// cased, because its meaning is "the sequence has not started"; recording it on
// a sequence that has started would make Draining and Outcome.Cause contradict
// each other.
func normalizeCause(cause Cause) Cause {
	switch cause {
	case CauseSignal, CauseIdle, CauseExplicit, CauseContext:
		return cause
	default:
		return CauseExplicit
	}
}
