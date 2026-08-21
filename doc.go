// Package drain answers one question for a process: when is it safe to exit,
// and what does it owe the work still in flight?
//
// Graceful termination is application logic that no platform supplies.
// Kubernetes sends SIGTERM and starts a terminationGracePeriodSeconds countdown
// ending in SIGKILL; KEDA sets a replica count and does not choose which pod
// dies; the descheduler evicts by age, not by idleness. Every process hosting
// long-lived work therefore writes the same sequence — let in-flight requests
// land, refuse new work, wait for outstanding work to reach a boundary, give up
// on what will not finish, exit inside the budget — and every process writes it
// slightly wrong. drain writes it once.
//
// Work in flight is tracked as counted holds, one counter per Reason. A hold
// means "this process cannot exit yet"; it does not mean "this process is
// busy". Acquire's ok is the sole admission authority — checking Draining and
// then acquiring is a TOCTOU race.
//
// There are two entry points, and which one is right is decided by who owns the
// process's signals. Run installs handling for SIGTERM and SIGINT; on the first
// one it restores their default disposition and then begins the sequence, in
// that order, so that a second signal kills a wedged drain rather than landing
// in a buffer nobody is reading. It is for the process that has no signal
// handling of its own. Wait installs NOTHING. signal.Notify is additive and
// process-wide, so a library that quietly registers for SIGINT changes the
// behaviour of every program that links it — an embedder that already owns its
// signals, such as a TUI that intercepts Ctrl-C for its own UX, keeps its
// handler and calls Begin from it. Both run the same sequence and return the
// same Outcome; a coordinator is spent after either, and a second call is
// ErrAlreadyRunning.
//
// Idle self-termination is the same machinery with a different cause, and it is
// correct in far fewer places than it looks. Self-exit is not how scale-to-zero
// works: Deployments and StatefulSets require restartPolicy Always, and the
// kubelet restarts the container regardless of exit code — including exit 0 —
// with exponential backoff that surfaces as CrashLoopBackOff. A process under a
// ReplicaSet that exits when idle does not scale to zero, it crashloops. Enable
// WithIdleTimeout only where a clean exit is the intended lifecycle: a Job or
// KEDA ScaledJob, an operator-owned per-session pod, or a systemd/Compose unit
// that is not restarted. Everywhere else leave it at zero and export occupancy
// for the autoscaler to read instead.
//
// drain persists nothing. It has no notion of a session, a journal, a
// workspace, or a checkpoint; it reports that a deadline arrived and lets the
// owner decide. It does not serve HTTP, expose metrics, or read the
// environment, and it does not compute occupancy — a hold answers "can I exit",
// occupancy answers "how full am I", and they are different numbers. The
// composition root wires drain's state to a readiness probe and a gauge.
//
// The error taxonomy is one rule, and it tells a caller which of errors.As and
// errors.Is to reach for: every failure that carries data is a concrete struct
// recovered with errors.As (*InvalidPolicyError, *GracePeriodExceededError), and
// the one failure that carries nothing is a sentinel matched with errors.Is
// (ErrAlreadyRunning). The one error drain does not mint is the one it did not
// cause: Wait returns its own ctx's Err verbatim when the drain was called off
// from outside, so a caller matching with errors.Is should expect
// context.Canceled and context.DeadlineExceeded too. Never classify a drain
// error by its message text.
//
// The module depends on the Go standard library and nothing else.
package drain
