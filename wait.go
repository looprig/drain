package drain

import (
	"context"
	"maps"
	"slices"
	"time"
)

// cutoff is what phase 3 hands to phase 4: the reasons that were outstanding at
// the instant admission closed, and a channel closed once every one of those
// holds has released.
//
// The two are produced together, under one acquisition of c.mu, because they are
// one fact. A snapshot taken after admission closed — or an admission flag
// closed after the snapshot — leaves a window in which Acquire can grant a hold
// that phase 4 never waits for and no Outcome ever reports.
//
// drained is nil exactly when outstanding is empty: there is nothing to wait for
// and nothing to close.
type cutoff struct {
	outstanding map[Reason]int
	drained     <-chan struct{}
}

// Wait arms the idle timer and blocks until the sequence completes or ctx is
// done. It installs no signal handling; use Run for that, or call Begin from
// your own handler. The idle clock's epoch is Wait entry, not New.
//
// Wait may be called once. A second call, concurrent or after return, yields
// ErrAlreadyRunning. Outcome is always valid, including alongside
// *GracePeriodExceededError or ctx.Err(). After Wait returns the coordinator is
// permanently spent: Acquire refuses forever and Wait errors forever.
func (c *Coordinator) Wait(ctx context.Context) (Outcome, error) {
	if !c.claim() {
		return Outcome{}, ErrAlreadyRunning
	}

	// Phase 1, quiesce. The sequence is started by Begin, from wherever the
	// process decides to drain — a signal handler, the idle timer, a shutdown
	// path — and it runs here, on Wait's goroutine, because none of those is a
	// place to spend a grace period. A cancelled ctx is itself a Begin: the
	// process must never be left running with a coordinator that has silently
	// stopped.
	select {
	case <-c.begun:
	case <-ctx.Done():
		c.begin(CauseContext)
	}
	cause, beganAt := c.sequence()
	c.startQuiesce(cause)

	// Phases 2 and 3, settle and the admission cutoff.
	cut, cancelled := c.settle(ctx, beganAt)

	// Phases 4 and 5, the wait and abandonment.
	if !cancelled {
		cancelled = c.awaitHolds(ctx, cut, beganAt)
	}

	out := Outcome{Cause: cause, Duration: c.cfg.clock.Now().Sub(beganAt)}
	if cancelled {
		return out, ctx.Err()
	}
	return out, nil
}

// claim reports whether this call is the one that gets to run the sequence. It
// is a latch rather than a mutex: the coordinator is spent after Wait returns,
// so the second caller is refused whether the first is still running or long
// finished.
func (c *Coordinator) claim() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.waiting {
		return false
	}
	c.waiting = true
	return true
}

// sequence reports the cause the drain recorded and the instant it started. Both
// are fixed by the time Wait reads them — the Begin that set them closed c.begun
// afterwards — but they are read under the lock because a Begin that lost the
// race is still writing nothing else while another may still be arriving.
func (c *Coordinator) sequence() (Cause, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cause, c.beganAt
}

// startQuiesce runs OnQuiesce on a goroutine the coordinator owns. It is never
// run on Wait's goroutine and never on Begin's: the abandon phase is user code
// and the grace ceiling exists to bound it, so a callback that runs on the
// mechanism's own goroutine is a callback that can hang the mechanism meant to
// bound it. Task 10 tracks this goroutine against the ceiling; until then a
// callback that never returns is simply left to the exiting process.
func (c *Coordinator) startQuiesce(cause Cause) {
	if c.cfg.onQuiesce == nil {
		return
	}
	go c.cfg.onQuiesce(cause)
}

// settle waits out the remainder of the settle window and returns the cutoff
// that closing admission produced, reporting whether ctx cancelled the wait.
//
// The window is measured from Begin, not from Wait entry, so a Wait entered late
// gets what is left of it rather than a fresh one. Cancellation skips whatever
// remains: there is no point protecting in-flight routing for a process that has
// been told to stop.
func (c *Coordinator) settle(ctx context.Context, beganAt time.Time) (cutoff, bool) {
	if ctx.Err() != nil {
		return c.cutAdmission(), true
	}

	// A non-positive remainder is dispatched directly rather than armed. The
	// real clock would fire such a timer almost at once, but a fake cannot run a
	// callback the test never asked for, so arming here would hang every
	// deterministic test of an already-elapsed window instead of failing it.
	remaining := c.cfg.preDrainDelay - c.cfg.clock.Now().Sub(beganAt)
	if remaining <= 0 {
		return c.cutAdmission(), false
	}

	// The cut runs inside the timer's callback, so admission closes when the
	// window expires rather than whenever Wait's goroutine is next scheduled.
	// The buffer keeps that goroutine from blocking if cancellation got here
	// first and nobody is left to receive.
	settled := make(chan cutoff, 1)
	timer := c.cfg.clock.AfterFunc(remaining, func() { settled <- c.cutAdmission() })

	select {
	case cut := <-settled:
		return cut, false
	case <-ctx.Done():
		timer.Stop()
		return c.cutAdmission(), true
	}
}

// cutAdmission is phase 3: it closes admission and snapshots the outstanding
// holds in ONE critical section, and returns what the wait phase is owed.
//
// Both things it does are decisions about the same state, and separating them
// admits the interleaving the concurrency contract calls out: Acquire reads
// admission as open, the cut closes admission and snapshots the counters as
// empty, and Acquire then increments — a hold that is never waited for and never
// reported. TestAdmissionCutoffAndSnapshotAreOneCriticalSection races real
// Acquires against this function and compares the two counts.
//
// It is idempotent, and it has to be: the settle timer and a cancelled context
// can both arrive here, and phase 4 must be owed exactly one snapshot however it
// got there. Admission is a one-way latch, so a repeat call re-closes nothing.
func (c *Coordinator) cutAdmission() cutoff {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cut {
		return c.cutoff
	}
	c.cut = true
	c.admissionOpen = false
	c.cutoff.outstanding = c.holdsLocked()
	if c.total > 0 {
		c.drained = make(chan struct{})
		c.cutoff.drained = c.drained
	}
	return c.cutoff
}

// awaitHolds runs phase 4, the wait. Every reason the cutoff named is given
// until its own deadline, and the reasons run CONCURRENTLY rather than in
// series: the worst case across them is max, not sum, which is what lets New
// validate each deadline against the grace period one at a time.
//
// A deadline does not end the wait. Reaching one is the instant the owner is
// told (phase 5), after which drain keeps waiting for those holds until they
// release or the ceiling expires — that is what makes interrupt-then-drain work,
// and what keeps Abandoned from over-reporting work the owner successfully
// abandoned. So the wait ends when the last hold releases, or when ctx is done;
// the ceiling that bounds it otherwise is Task 10.
//
// It reports whether ctx cancelled the wait.
func (c *Coordinator) awaitHolds(ctx context.Context, cut cutoff, beganAt time.Time) bool {
	if len(cut.outstanding) == 0 {
		return false
	}

	stop := c.armDeadlines(cut, beganAt)
	defer stop()

	select {
	case <-cut.drained:
		return false
	case <-ctx.Done():
		return true
	}
}

// armDeadlines starts phase 4's per-reason deadlines and returns a function that
// stops whichever are still armed when the wait ends. A deadline that never came
// due is stopped rather than left to fire against a spent coordinator.
//
// Elapsed time is read ONCE, for every reason, so that the reason this loop
// reaches last cannot be handed a longer budget than the one it reached first.
// Reasons are visited in sorted order for the same kind of reason validation is:
// the immediate dispatches below are the visible half of this loop, and a map's
// order would make which of two spent deadlines is reported first differ run to
// run.
func (c *Coordinator) armDeadlines(cut cutoff, beganAt time.Time) func() {
	elapsed := c.cfg.clock.Now().Sub(beganAt)

	timers := make([]Timer, 0, len(cut.outstanding))
	for _, r := range slices.Sorted(maps.Keys(cut.outstanding)) {
		// The remainder, not the policy. A deadline is measured from Begin and
		// overlaps the settle window, so a reason admitted three seconds into a
		// five-second settle under a ten-second deadline has five seconds left
		// here, not ten: the budget belongs to the process, not to the request.
		//
		// Which makes a non-positive remainder ordinary rather than exceptional
		// — a zero-deadline pending gate is spent before the sequence starts,
		// and any deadline shorter than the settle window is spent by the cut —
		// and it is dispatched directly. Arming a timer for it would be the
		// literal wrong thing twice over: the spec says such a deadline fires
		// immediately, and a fake clock runs a non-positive timer only on an
		// explicit Advance, so a drain that armed one would hang rather than
		// fail in every deterministic test its consumers write.
		remaining := c.deadlineFor(r) - elapsed
		if remaining <= 0 {
			c.abandon(r)
			continue
		}
		timers = append(timers, c.cfg.clock.AfterFunc(remaining, func() { c.abandon(r) }))
	}

	return func() {
		for _, timer := range timers {
			timer.Stop()
		}
	}
}

// deadlineFor reports the deadline drain applies to reason r: the one the caller
// configured, or the full grace period for a reason nobody configured. The
// cautious default is the one you get by doing nothing, which is what makes a
// misspelled WithDeadline survivable — the real reason keeps waiting instead of
// inheriting the typo's policy — and Outcome.Waited is what makes it visible.
func (c *Coordinator) deadlineFor(r Reason) time.Duration {
	if d, ok := c.cfg.deadlines[r]; ok {
		return d
	}
	return c.gracePeriod
}

// abandon is phase 5 for one reason: the point at which its deadline has passed
// with holds still outstanding. Task 9 fills it in — the re-check under the lock
// that skips a reason whose count reached zero, and OnDeadline on a
// coordinator-owned goroutine.
//
// It is already the single point both ways a deadline can pass converge on: a
// timer that came due, and a deadline that was spent before the wait phase began.
// Keeping them one path is what makes the zero-deadline case ordinary code
// rather than a special case bolted onto phase 5 later.
func (c *Coordinator) abandon(r Reason) {}
