package drain

import (
	"context"
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

	// Phase 4, the wait. Per-reason deadlines are Task 8 and abandonment is
	// Task 9; for now the sequence waits for everything the cutoff named.
	if !cancelled && len(cut.outstanding) > 0 {
		select {
		case <-cut.drained:
		case <-ctx.Done():
			cancelled = true
		}
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
