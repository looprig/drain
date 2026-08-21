package drain

import (
	"context"
	"os"
	"os/signal"
)

// The signal package, reachable from this file and nowhere else in the module.
//
// Two things make that a rule rather than an accident. signal.Notify is
// ADDITIVE and process-wide: it does not replace an existing registration, it
// adds one, so a library that quietly registers for SIGINT changes the
// behaviour of every program that links it — and one of this workspace's
// consumers is a TUI that intercepts Ctrl-C for its own UX. Wait therefore
// installs nothing, and an embedder that already owns its signals calls Begin
// from its own handler. Run exists for the process that does not.
//
// They are variables so that the module's own tests can substitute them. There
// is no other way to assert what Run registered for without either sending real
// signals — which for the escalation path means killing the test binary — or
// racing the installation. Nothing outside this file writes them in production;
// TestWaitInstallsNoSignalHandling additionally parses the package source and
// fails if any file other than this one imports os/signal, which is the half
// the seam itself cannot check.
var (
	notifySignals = signal.Notify
	stopSignals   = signal.Stop
)

// Run is Wait plus signal installation: each signal configured by WithSignals
// (default SIGTERM, SIGINT) calls Begin(CauseSignal). After the first such
// signal Run restores the signals' default disposition, so a second Ctrl-C
// kills the process — the operator's last resort against a wedged drain.
//
// Signals delivered before Run is entered get default disposition: instant
// death, no drain. Call Run early in main and keep the window between process
// start and Run entry as short as possible. This is not something New could fix
// by installing handlers at construction: a coordinator that registered for
// SIGINT the moment it was built would take that behaviour away from every
// embedder, including those that call Wait precisely to keep it.
//
// Everything Wait documents holds here unchanged — one call, an Outcome that is
// always valid, a coordinator that is spent afterwards. Run leaves no handler
// behind: the registration is removed by the time it returns, whether it
// returned after a signal, after a clean drain, or after ErrAlreadyRunning.
func (c *Coordinator) Run(ctx context.Context) (Outcome, error) {
	stop := c.installSignals()
	defer stop()
	return c.Wait(ctx)
}

// installSignals registers for the configured signals and returns the teardown.
//
// The watcher goroutine, rather than a handler calling Begin inline, is what
// the signal package's own contract asks for: delivery is a send on a channel,
// so something has to be receiving. It is also what keeps the escalation honest
// — stopSignals runs the moment the first signal is received, before Begin and
// long before the drain ends, and a drain that has wedged is exactly the case
// where the difference matters.
//
// The teardown waits for that goroutine to exit. Run therefore leaves nothing
// running behind it, which matters twice: a process that goes on to do
// something else after a drain is not left with a coordinator's goroutine
// holding a registration, and the module's own tests can restore the seam above
// without racing a watcher that is still reading it.
func (c *Coordinator) installSignals() func() {
	// An empty set is a set, and signal.Notify reads it as "every signal" —
	// which would turn WithSignals() into a drain triggered by SIGWINCH, by
	// SIGCHLD, and by the profiler's SIGPROF. A caller that configured no
	// signals gets no handler, and Run is then exactly Wait.
	if len(c.cfg.signals) == 0 {
		return func() {}
	}

	// Buffered, as signal.Notify requires: the runtime never blocks delivering
	// a signal, so an unbuffered channel would drop the one signal this
	// coordinator exists to hear. One slot is enough because only the first
	// signal does anything — the second is for the kernel, after the default
	// disposition is restored.
	received := make(chan os.Signal, 1)
	notifySignals(received, c.cfg.signals...)

	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)

		// Both exits restore the default disposition, and neither does it with
		// a defer. A deferred restore on the signal path would run after Begin
		// returns, and Begin takes c.mu and reads the clock — so a second
		// signal arriving in that window lands in the now-drained one-slot
		// buffer and is silently swallowed. The operator's escalation against a
		// wedged drain is precisely a second Ctrl-C, and the window a defer
		// opens is precisely the moment a first Ctrl-C makes them press it
		// again. Restoring first makes that window as small as the runtime
		// allows: after this line the kernel, not this process, decides.
		select {
		case <-received:
			stopSignals(received)

			// Begin, and nothing more. The sequence runs inside Wait, on Wait's
			// goroutine, because this one is a signal watcher and a signal
			// watcher is not a place to spend a grace period.
			c.Begin(CauseSignal)
		case <-done:
			// The teardown path: no signal ever arrived, and this is the
			// process-wide registration Run promises not to leave behind.
			stopSignals(received)
		}
	}()

	return func() {
		close(done)
		<-exited
	}
}
