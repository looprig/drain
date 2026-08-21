package drain

// Idle self-termination: the second trigger, and the same machinery. A process
// with no holds outstanding for IdleTimeout begins draining with CauseIdle,
// exactly as though something external had told it to.
//
// # Why it needs a generation counter
//
// The obvious implementation is a timer stopped on every new Acquire, and it is
// wrong in a way that only shows up under load. Timer.Stop returns false once
// the callback has fired or is already in flight: the runtime has committed to
// running it and cancelling is no longer possible. So a hold granted at
// T + idleTimeout − ε can still be followed by the idle callback running, and a
// callback that trusted its own scheduling would call Begin(CauseIdle) on a
// process that had just accepted work. Under a per-session pod that is a
// session killed at the moment it started.
//
// The fix is that the callback does not trust its own scheduling. A generation
// counter is bumped under the lock on every zero→nonzero transition of the
// total; the timer captures the generation current when it is armed; and the
// callback re-validates, under the lock, that the total is still zero AND the
// generation still matches before it starts anything. Stop remains an
// optimization — it saves a wakeup — and is never the correctness mechanism.
//
// Two checks rather than one, because they catch different things. "Total is
// still zero" catches a hold that is outstanding right now. "Generation still
// matches" catches a hold that was acquired and released again while this timer
// was in flight, which leaves the total at zero and the process anything but
// idle. Neither implies the other.
//
// The visible consequence, and the property to reach for when this is being
// changed: a lost idle race followed by a real SIGTERM yields
// Cause == CauseSignal, never CauseIdle, because the idle callback no-ops and
// Begin records the first cause that actually transitions.
//
// # Why this is opt-in and mostly wrong
//
// See WithIdleTimeout. Under a Deployment, ReplicaSet or StatefulSet the kubelet
// restarts the container on any exit, exit 0 included, so a process that exits
// when idle does not scale to zero — it crashloops. Idle self-exit is correct
// for a Job or KEDA ScaledJob, an operator-owned per-session pod, or a
// systemd/Compose unit that is not restarted, and nowhere else.

// startIdle arms the idle timer at Wait entry, which is the idle clock's epoch.
//
// The epoch is Wait entry rather than New for the cold-start case: a process
// constructed at boot and not serving for another thirty seconds would, with a
// twenty-second idle timeout measured from New, kill itself before it ever
// answered a request — and under a ScaledJob the autoscaler would start another
// one to do the same thing. The documented startup-hold pattern closes the
// remaining window: acquire a hold in main and release it when the process is
// ready to serve, and no idle timer arms until then.
func (c *Coordinator) startIdle() {
	if c.cfg.idleTimeout <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.idleActive = true
	c.armIdleLocked()
}

// armIdleLocked arms the idle timer for the current generation. The caller must
// hold c.mu.
//
// Arming under the lock is safe, and it is worth saying why it is safe rather
// than leaving it to be rediscovered: the module's only lock order is
// c.mu → clock.mu, with no reverse edge, because a Clock must run timer
// callbacks with its own lock released. draintest does, time.AfterFunc does,
// and a Clock that did not would deadlock the coordinator here on the first
// hold transition. The alternative — dropping c.mu to arm — would reopen the
// gap the generation counter exists to close, since the generation captured
// would no longer be the one that was current when the total reached zero.
func (c *Coordinator) armIdleLocked() {
	// Not before Wait, not once the sequence has started, and not while
	// anything is outstanding. Each of the three is a different reason for the
	// timer to be meaningless rather than merely early.
	if !c.idleActive || c.draining || c.total != 0 {
		return
	}

	generation := c.generation
	c.idleTimer = c.cfg.clock.AfterFunc(c.cfg.idleTimeout, func() { c.idleExpired(generation) })
}

// stopIdleLocked cancels the armed idle timer, if there is one. The caller must
// hold c.mu.
//
// Its return value is deliberately ignored. Stop reports whether it cancelled
// the callback in time, and there is nothing useful to do with that answer: the
// case where it says false — the callback is already in flight — is precisely
// the case the generation check handles, and a coordinator that branched on it
// would be trusting the one signal that arrives too late to be trusted.
func (c *Coordinator) stopIdleLocked() {
	if c.idleTimer == nil {
		return
	}
	c.idleTimer.Stop()
	c.idleTimer = nil
}

// idleExpired is the idle timer's callback: it begins an idle drain if the
// process is still idle for the generation this timer was armed in, and no-ops
// otherwise.
//
// The re-validation and the Begin are ONE critical section, through beginLocked
// rather than Begin. A callback that checked the counters, released the lock,
// and then called Begin would have re-created the race it was written to close,
// only in a narrower window: an Acquire landing in that gap is granted against a
// coordinator that is about to record CauseIdle, and the hold it hands out
// belongs to work that will never be waited for as anything but a drain.
func (c *Coordinator) idleExpired(generation uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.total != 0 || c.generation != generation {
		return
	}

	// This timer has fired, so nothing is left to stop; clearing the reference
	// keeps stopIdleLocked from calling Stop on a spent timer.
	c.idleTimer = nil
	c.beginLocked(CauseIdle)
}
