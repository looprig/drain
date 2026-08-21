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
// settled is nil exactly when outstanding is empty: there is nothing to wait for
// and nothing to close.
type cutoff struct {
	outstanding map[Reason]int
	settled     <-chan struct{}
}

// ended names how a phase of the sequence finished. The three are mutually
// exclusive per phase — one select branch wins — which is what makes the error
// Wait returns a decision made once rather than a re-derivation from state that
// has moved on by the time it is read.
type ended uint8

const (
	// endedComplete: the phase ran to its own end. For the wait phase that
	// means every fired OnDeadline returned and the total reached zero.
	endedComplete ended = iota

	// endedCeiling: the grace period expired first.
	endedCeiling

	// endedCancelled: Wait's ctx was done first.
	endedCancelled
)

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

	// The idle clock's epoch is here, not New, and not Begin: a coordinator
	// that has been constructed but never waited on is a process that has not
	// started serving, and killing it for being idle is the cold-start
	// self-kill loop. See idle.go.
	c.startIdle()

	// Before phase 1: block until something starts the sequence. Begin is
	// called from wherever the process decides to drain — a signal handler, the
	// idle timer, a shutdown path — and the sequence itself runs here, on
	// Wait's goroutine, because none of those is a place to spend a grace
	// period. A cancelled ctx is itself a Begin: the process must never be left
	// running with a coordinator that has silently stopped.
	select {
	case <-c.begun:
	case <-ctx.Done():
		c.begin(CauseContext)
	}
	cause, beganAt := c.sequence()

	// The ceiling is armed BEFORE the first user callback is started, and the
	// order is the whole of "the ceiling covers phases 1-6": OnQuiesce runs
	// inside it, not alongside it. Both it and the callback context are
	// measured from Begin, so a Wait entered late gets what is left of the
	// budget rather than a fresh one — the platform's countdown started at
	// Begin whatever this process was doing at the time.
	callbackCtx, cancelCallbacks := c.callbackContext(ctx, beganAt)
	defer cancelCallbacks(nil)
	ceiling, stopCeiling := c.armCeiling(beganAt, func() { cancelCallbacks(context.DeadlineExceeded) })
	defer stopCeiling()

	// Phase 1, quiesce.
	c.startQuiesce(cause)

	// Phases 2 and 3, settle and the admission cutoff.
	cut, how := c.settle(ctx, beganAt)

	// Phases 4 and 5, the wait and abandonment.
	if how == endedComplete {
		how = c.awaitHolds(ctx, callbackCtx, ceiling, cut, beganAt)
	}

	// Neither of the two ways a drain can end early is a quiet exit. Phase 5
	// owes a report to every reason still outstanding that has not had one, and
	// that set is never reliably empty on either path: at the ceiling because an
	// unconfigured reason waits the full grace period, so its deadline and the
	// ceiling are the same instant and which timer the clock reaches first is
	// not a property worth depending on; and on cancellation because the wait
	// phase was cut short, so most reasons never reached a deadline at all.
	//
	// A cancelled drain abandons work exactly as an overrun does, and the owner
	// is owed the same report — the alternative is a process that walks away
	// from in-flight work silently. abandon is once-per-reason, so a reason
	// already told at its own deadline is not told again here.
	//
	// The callback context is cancelled BEFORE the sweep rather than left to the
	// path that got here, and that ordering is the contract: on both paths the
	// budget is gone by definition, so the invocation is a notification that
	// abandonment happened rather than an invitation to do work inside a ctx
	// that is about to die anyway. Doing it here makes it true at the callback's
	// first statement instead of true shortly afterwards, which is the
	// difference between a documented property and a race.
	// Abandoned is what is still outstanding, snapshotted ONCE and snapshotted
	// HERE, before phase 5's sweep dispatches anything. Every hold that reached
	// a boundary has released by now, so what remains is exactly the work the
	// process is walking away from — and it is exactly what the sweep below is
	// about to be told about, which is the property the ordering buys.
	//
	// Reading the counters after the sweep would make the number depend on a
	// goroutine race. The expected shape of abandonment is an owner whose
	// OnDeadline interrupts work whose deferred release then runs on the
	// callback's own goroutine, so a reason the sweep had just reported would
	// appear or not appear according to what got scheduled first — two answers
	// for one drain, reported to the same operator in the same return.
	//
	// Reasons configured with a zero deadline appear here on every drain by
	// design, which is why an alert on Abandoned > 0 has to exclude the reasons
	// Outcome.Waited reports at zero.
	abandoned := c.Holds()

	if how != endedComplete {
		cancelCallbacks(endingCause(how, ctx))
		c.abandonRemaining(callbackCtx)
	}

	out := Outcome{
		Cause:     cause,
		Duration:  c.cfg.clock.Now().Sub(beganAt),
		Waited:    c.waited(cut),
		Abandoned: abandoned,
	}
	switch how {
	case endedCancelled:
		return out, ctx.Err()
	case endedCeiling:
		// The SAME map, not a second read of the counters: one snapshot under
		// two names can never disagree with itself, and both aliases go to the
		// same caller in the same return, so a defensive copy would protect
		// nobody. The coordinator keeps no reference to it.
		//
		// It is nil when nothing was outstanding, which is a real state rather
		// than a degenerate one: the exit condition is callbacks-returned AND
		// total-zero, so the grace period can expire with every hold released
		// and a wedged OnDeadline still running.
		return out, &GracePeriodExceededError{
			Elapsed:   out.Duration,
			Budget:    c.gracePeriod,
			Abandoned: out.Abandoned,
		}
	}
	return out, nil
}

// callbackContext derives the ctx handed to OnDeadline from Wait's, with its
// deadline at grace expiry. Cancelling it is armCeiling's job.
//
// The expiry is driven by the injected Clock rather than by
// context.WithDeadline, and that is not an implementation detail a consumer can
// ignore. Clock is the coordinator's entire dependency on time; a context
// deadline is measured against the wall clock, so a consumer testing its own
// shutdown wiring against draintest would get a callback ctx that expired at an
// instant unrelated to the drain it was simulating — either seconds of real time
// after the test had finished, or, for a clock started at a fixed date in the
// past, before the callback ever ran.
//
// So the cancellation follows the clock and Deadline reports the instant that
// cancellation is scheduled for, in that clock's own time base. Wait's ctx wins
// when it expires first: a derived context may never outlive its parent's
// deadline, and reporting one that does would make every context.WithTimeout
// built on top of this one wrong.
//
// # Why the cause matters
//
// A clock-driven expiry cannot go through context.WithDeadline, so Err() on
// this context is context.Canceled even when the ceiling is what ended it — and
// a context that reports a deadline invites exactly the check that then reads
// wrong, errors.Is(ctx.Err(), context.DeadlineExceeded). The two situations the
// owner has to tell apart are the ceiling expiring, where the process is out of
// budget and must exit, and Wait's own ctx being cancelled, where the drain was
// called off from outside. So the cancellation carries a CAUSE:
// context.DeadlineExceeded at the ceiling, and Wait's ctx's own cause on the
// cancellation path. context.Cause discriminates them, ctx.Err() alone cannot,
// and WithOnDeadline documents which of the two to read.
func (c *Coordinator) callbackContext(ctx context.Context, beganAt time.Time) (context.Context, context.CancelCauseFunc) {
	base, cancel := context.WithCancelCause(ctx)
	expiry := beganAt.Add(c.gracePeriod)
	if parent, ok := ctx.Deadline(); ok && !parent.After(expiry) {
		return base, cancel
	}
	return ceilingContext{Context: base, deadline: expiry}, cancel
}

// endingCause reports the cause to cancel the callback context with when the
// sequence ended early. Only the two early endings reach it: a completed drain
// tears its context down with a nil cause, which is the ordinary "this is over"
// rather than a claim about why.
//
// The cancellation path forwards Wait's ctx's own cause rather than minting
// one, so an owner that cancelled with context.WithCancelCause sees its own
// reason at the callback instead of a generic one. It is usually redundant —
// the parent's cancellation has normally already propagated by the time this
// runs, and the first cancellation wins — but that propagation is asynchronous
// for a context the caller implemented itself, so whichever of the two lands
// first has to record the same thing.
func endingCause(how ended, ctx context.Context) error {
	if how == endedCeiling {
		return context.DeadlineExceeded
	}
	return context.Cause(ctx)
}

// ceilingContext is a cancellable context that reports the grace ceiling as its
// deadline. Only Deadline is overridden; cancellation, Err, Value and everything
// derived from them are the embedded context's.
type ceilingContext struct {
	context.Context
	deadline time.Time
}

// Deadline reports grace expiry, measured on the coordinator's clock.
func (c ceilingContext) Deadline() (time.Time, bool) { return c.deadline, true }

// armCeiling starts the hard ceiling over the whole sequence and returns a
// channel closed when it expires, together with a stop for the ordinary path
// where the drain finishes first. expire is called at the same instant, and is
// what makes the callback context expire at the ceiling.
//
// A non-positive remainder is closed immediately rather than armed, which is the
// same rule the settle window and the per-reason deadlines follow and it is not
// hypothetical here: a signal that arrives while the process is still starting
// up can spend the entire budget before Wait is ever entered.
func (c *Coordinator) armCeiling(beganAt time.Time, expire func()) (<-chan struct{}, func()) {
	expired := make(chan struct{})
	remaining := c.gracePeriod - c.cfg.clock.Now().Sub(beganAt)
	if remaining <= 0 {
		close(expired)
		expire()
		return expired, func() {}
	}

	timer := c.cfg.clock.AfterFunc(remaining, func() {
		close(expired)
		expire()
	})
	return expired, func() { timer.Stop() }
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
// run on Wait's goroutine and never on Begin's: a callback that runs on the
// mechanism's own goroutine is a callback that can hang the mechanism meant to
// bound it, and Begin is called from signal handlers, which are not a place to
// spend a grace period.
//
// It is started after the ceiling is armed, so a wedged OnQuiesce is covered by
// it — which is why OnQuiesce needs no separate "must not block" contract. It is
// deliberately NOT counted towards the wait phase's exit condition. Covered by
// the ceiling and waited for are different things, and only the first applies:
// admission is closed by the time it could matter, so a process with nothing
// outstanding must not be held for the full grace period waiting on a callback
// whose only effect was to stop traffic that is already being refused.
//
// A wedged OnQuiesce therefore leaks this goroutine. That is deliberate and
// bounded: the ceiling still fires, Wait still returns, and the process is
// exiting. There is no cancellation path because there is nothing useful to
// cancel — OnQuiesce takes no ctx, and a process that has decided to die does
// not need its readiness callback to unwind tidily.
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
// The grace ceiling is deliberately absent from this select, and it is absent
// because it is unreachable rather than because it is unimportant. New rejects a
// settle window that is not strictly less than the grace period, and both are
// measured from the same instant, so whenever the settle remainder is positive
// the ceiling remainder is strictly larger. A settle window that outlives the
// ceiling is a state New will not construct; a branch for it would be code no
// test could reach.
func (c *Coordinator) settle(ctx context.Context, beganAt time.Time) (cutoff, ended) {
	if ctx.Err() != nil {
		return c.cutAdmission(), endedCancelled
	}

	// A non-positive remainder is dispatched directly rather than armed. The
	// real clock would fire such a timer almost at once, but a fake cannot run a
	// callback the test never asked for, so arming here would hang every
	// deterministic test of an already-elapsed window instead of failing it.
	remaining := c.cfg.preDrainDelay - c.cfg.clock.Now().Sub(beganAt)
	if remaining <= 0 {
		return c.cutAdmission(), endedComplete
	}

	// The cut runs inside the timer's callback, so admission closes when the
	// window expires rather than whenever Wait's goroutine is next scheduled.
	// The buffer keeps that goroutine from blocking if cancellation got here
	// first and nobody is left to receive.
	window := make(chan cutoff, 1)
	timer := c.cfg.clock.AfterFunc(remaining, func() { window <- c.cutAdmission() })

	select {
	case cut := <-window:
		return cut, endedComplete
	case <-ctx.Done():
		timer.Stop()
		return c.cutAdmission(), endedCancelled
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
		c.settled = make(chan struct{})
		c.cutoff.settled = c.settled
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
// abandoned.
//
// The wait therefore ends when the last hold has released AND every OnDeadline
// that fired has returned, or at the grace ceiling, or when ctx is done —
// whichever comes first, which is what this function reports.
func (c *Coordinator) awaitHolds(ctx, callbackCtx context.Context, ceiling <-chan struct{}, cut cutoff, beganAt time.Time) ended {
	if len(cut.outstanding) == 0 {
		return endedComplete
	}

	stop := c.armDeadlines(callbackCtx, cut, beganAt)
	defer stop()

	// Completion is preferred over the ceiling when the drain is ALREADY
	// settled on entry, which is the whole of what this non-blocking select
	// buys and the whole of what it claims. A drain whose last hold released
	// before the wait phase was entered would otherwise be reported as a
	// success or an overrun according to how the select below shuffled two
	// cases that were both ready on arrival — two answers for one drain, and a
	// process reports Abandoned to a dashboard. Nothing is outstanding on this
	// branch, so preferring it costs no wait at all.
	//
	// It is not a general tie-break. Two conditions that become ready while the
	// select below is blocked are still chosen between at random, and no
	// arrangement of selects can change that; this makes deterministic the one
	// case that can be made deterministic.
	select {
	case <-cut.settled:
		return endedComplete
	default:
	}

	select {
	case <-cut.settled:
		return endedComplete
	case <-ceiling:
		return endedCeiling
	case <-ctx.Done():
		return endedCancelled
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
func (c *Coordinator) armDeadlines(ctx context.Context, cut cutoff, beganAt time.Time) func() {
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
			c.abandon(ctx, r)
			continue
		}
		timers = append(timers, c.cfg.clock.AfterFunc(remaining, func() { c.abandon(ctx, r) }))
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

// waited reports the deadline drain applied to each reason the cut saw
// outstanding: Outcome.Waited, and the only mitigation an open set of string
// reasons has for its one dangerous failure. Nothing checks that a configured
// reason is a reason anybody holds, so WithDeadline("harness/pending_gaet", 0)
// leaves the real pending_gate at the full grace period — a drain that waits
// hours on a human, which is precisely what the zero deadline was written to
// prevent. Reporting the applied deadline makes that visible on the first
// drain rather than never.
//
// The map is FRESH, and never c.cfg.deadlines. The config map is held by
// reference with no defensive copy — deliberately, since every option builds
// its own — and this map carries reasons nobody configured at the grace period,
// so building it by adding to the config would rewrite the policy of a
// coordinator whose sequence is still reading it.
//
// The scope is the cut's snapshot: the reasons outstanding when admission
// closed. A reason acquired and released entirely inside a signal drain's open
// settle window is therefore absent, which is right rather than a compromise —
// no deadline was ever applied to it, so there is nothing to report and nothing
// a typo could have misconfigured. It also keeps the wait phase's records to
// the one snapshot the cut already produces, instead of tracking every reason
// seen while draining on Acquire's hot path.
func (c *Coordinator) waited(cut cutoff) map[Reason]time.Duration {
	if len(cut.outstanding) == 0 {
		// nil, never an empty non-nil map, matching Holds and Abandoned: a
		// caller's m == nil and len(m) == 0 must never disagree.
		return nil
	}

	waited := make(map[Reason]time.Duration, len(cut.outstanding))
	for r := range cut.outstanding {
		waited[r] = c.deadlineFor(r)
	}
	return waited
}

// abandon is phase 5 for one reason: its deadline has passed, so the owner is
// told. It is the single point both ways a deadline can pass converge on — a
// timer that came due, and a deadline already spent when the wait phase began —
// which is what keeps the zero-deadline pending gate on ordinary code rather
// than a special case bolted onto the side of the phase.
//
// It tells the owner and nothing more. drain does not interrupt, cancel, or
// forget anything: it has no idea what the work is, and the wait continues until
// those holds release or the ceiling expires. That is what makes
// interrupt-then-drain work at all, since interrupted work needs time to unwind,
// and it is what keeps Abandoned from over-reporting work the owner successfully
// abandoned.
//
// The count is re-read under the lock at invocation rather than taken from the
// cut's snapshot, and a reason whose count has since reached zero gets NO
// callback — not a callback with a zero. The owner's abandon path is a real
// action, and telling it to interrupt a turn that already finished is worse than
// saying nothing. The value handed over remains advisory: a release landing
// while the callback is dispatched is a race the contract accepts, because the
// alternative is holding the lock across user code.
//
// It fires at most once per reason. Three paths reach it — a deadline timer that
// came due, a deadline already spent when the wait phase began, and the ceiling
// sweep — and for an unconfigured reason the first and the third are the SAME
// INSTANT, because the deadline nobody configured is the full grace period. Two
// reports for one reason would tell an owner to interrupt work it has already
// been told to interrupt, so claimAbandon settles it under the lock.
//
// ctx is the callback context: Wait's, with its deadline at grace expiry.
func (c *Coordinator) abandon(ctx context.Context, r Reason) {
	if c.cfg.onDeadline == nil {
		return
	}
	outstanding, ok := c.claimAbandon(r)
	if !ok {
		return
	}

	// On a goroutine the coordinator owns, and — the load-bearing half — with
	// c.mu released. The expected shape of abandonment is that the owner
	// interrupts work whose deferred release() then runs on this goroutine
	// before the callback returns, so a callback invoked under the lock would
	// deadlock against releaseHold on the very path it exists to serve.
	//
	// The goroutine is tracked rather than merely started: the wait phase does
	// not end until it has returned, so an owner that is still unwinding
	// interrupted work is not exited out from under. The ceiling is what bounds
	// that, and a callback that never returns leaks this goroutine, which is
	// acceptable for the same reason OnQuiesce's is.
	go func() {
		defer c.callbackReturned()
		c.cfg.onDeadline(ctx, r, outstanding)
	}()
}

// abandonRemaining is phase 5 at the ceiling: every reason still outstanding
// that has not already had its report gets one, and Wait does not wait for them.
//
// Reasons are visited in sorted order for the same reason armDeadlines visits
// them sorted — a map's order would make which of two simultaneous reports is
// dispatched first differ run to run.
func (c *Coordinator) abandonRemaining(ctx context.Context) {
	if c.cfg.onDeadline == nil {
		return
	}
	for _, r := range c.outstandingReasons() {
		c.abandon(ctx, r)
	}
}

// outstandingReasons lists the reasons with holds outstanding right now, sorted.
func (c *Coordinator) outstandingReasons() []Reason {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.holds))
}

// claimAbandon decides, under the lock, whether reason r gets a report now, and
// returns the count to hand the callback if it does.
//
// Three decisions in ONE critical section, and each of them has to be there:
//
//   - The once-per-reason guard, so the deadline timer and the ceiling sweep
//     racing on the same instant produce one report rather than two.
//   - The count re-read, so a reason whose work finished before the deadline
//     expired gets NO callback rather than a callback with a zero. It is read
//     fresh rather than taken from the cut's snapshot, which is the difference
//     between "still in flight" and "was in flight when admission closed".
//   - The in-flight callback count, incremented HERE rather than in the
//     goroutine, so the wait phase can never observe the settled state between
//     a nonzero count being read and the callback that was dispatched on the
//     strength of it starting to run.
//
// The count handed over stays advisory: a release landing while the callback is
// dispatched is a race the contract accepts, because the alternative is holding
// the lock across user code.
func (c *Coordinator) claimAbandon(r Reason) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	outstanding := c.holds[r]
	if outstanding == 0 || c.notified[r] {
		return 0, false
	}
	c.notified[r] = true
	c.callbacks++
	return outstanding, true
}

// callbackReturned records that one OnDeadline invocation has finished, which
// may be the last thing the wait phase was waiting for.
func (c *Coordinator) callbackReturned() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.callbacks--
	c.settleIfQuietLocked()
}
