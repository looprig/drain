# drain

Graceful process termination for long-lived Go workloads: when is it safe to exit, and
what does the process owe the work still in flight?

> [!WARNING]
> **Idle self-exit crashloops under a Deployment.** `WithIdleTimeout` is not how
> scale-to-zero works. Deployments, ReplicaSets, and StatefulSets require
> `restartPolicy: Always`, and the kubelet restarts the container on *any* exit —
> including exit 0 — with backoff that surfaces as `CrashLoopBackOff`. Enable it only
> where a clean exit is the intended lifecycle: a Job or KEDA `ScaledJob`, an
> operator-owned per-session pod, or a systemd/Compose unit that is not restarted.
> Everywhere else leave it at zero and export occupancy for the autoscaler to read.

In-flight work is tracked as counted **holds**, one counter per **`Reason`**. A drain runs
one sequence whatever started it — quiesce, settle, cut admission, wait per reason,
abandon what will not finish, exit — bounded by a hard grace period, and returns an
**`Outcome`** telemetry record even when that ceiling is exceeded. `Acquire`'s `ok` is the
sole admission authority; `Draining()` is advisory, and checking it before acquiring is a
TOCTOU race. The clock is injected, so consumers test their own shutdown wiring against
`drain/draintest` without sleeping in real time.

`drain` persists nothing, serves no HTTP, exports no metrics, and reads no environment: it
reports that a deadline arrived and lets the owner decide. Occupancy ("how full am I") is
deliberately not here — a hold answers "can I exit", which is a different number.

This module has **zero third-party dependencies** and will keep it that way.

## Two triggers, one sequence

A drain starts either because something outside told the process to stop, or because the
process noticed it had nothing left to do.

- **A signal.** `Run` installs handling for SIGTERM and SIGINT. On the first one it
  restores their default disposition and *then* calls `Begin(CauseSignal)`, in that
  order, so a second Ctrl-C kills a wedged drain instead of landing in a buffer nobody
  is reading.
- **Idleness.** `WithIdleTimeout` calls `Begin(CauseIdle)` once the process has held
  nothing for that long, timed from `Wait` entry or from the release of the last hold,
  whichever came later. Read the warning above first.

`Begin(CauseExplicit)` covers everything else — a control-plane RPC, a supervisor, a test.
Cancelling `Wait`'s context is a drain too, with `CauseContext`.

## Two entry points, chosen by who owns your signals

**`Wait` installs nothing.** `signal.Notify` is additive and process-wide, so a library
that quietly registers for SIGINT changes the behaviour of every program that links it. A
process that already owns its signals — a TUI intercepting Ctrl-C, a server multiplexing
SIGHUP — keeps its handler and calls `Begin` from it. `Run` is `Wait` plus signal
installation, for the process that has no handler of its own.

`OnQuiesce` is fire-and-forget: it runs once on a coordinator-owned goroutine, and it is
neither waited for nor cancelled, so keep it to flipping a flag. Everything else is
bounded by the ceiling.

```go
coordinator, err := drain.New(30*time.Second,
    drain.WithPreDrainDelay(5*time.Second),
    drain.WithDeadline(reasonActiveTurn, 20*time.Second),
    drain.WithDeadline(reasonPendingGate, 0), // waits on a human: abandon at the cut
    drain.WithOnQuiesce(func(drain.Cause) { ready.Store(false) }),
    drain.WithOnDeadline(func(ctx context.Context, r drain.Reason, outstanding int) {
        interruptWork(r) // its deferred release runs right here
    }),
)
if err != nil {
    return err
}

// Per unit of work, anywhere in the process:
release, ok := coordinator.Acquire(reasonActiveTurn)
defer release() // never nil, so this is safe even when ok is false
if !ok {
    return errShuttingDown
}

// In main:
outcome, err := coordinator.Run(ctx)
log.Info("drained", "cause", outcome.Cause, "took", outcome.Duration,
    "abandoned", outcome.Abandoned)
```

`New` is the single validation point and reports `*InvalidPolicyError`; a ceiling overrun
is `*GracePeriodExceededError` alongside a still-valid `Outcome`; a second `Wait` is
`ErrAlreadyRunning`. Classify with `errors.As`/`errors.Is`, never by message text.

## Runnable examples

Verified on every push by `.github/workflows/docs-examples.yml`.

| Example | Shows |
|---|---|
| [`examples/kubernetes`](examples/kubernetes) | The shared-pod shape: `Run`, `OnQuiesce` failing readiness, a per-turn hold, and a pending gate abandoned at the cut with `WithDeadline(r, 0)` |
| [`examples/startuphold`](examples/startuphold) | The startup hold that keeps `WithIdleTimeout` from killing a slow-booting process |
| [`examples/embedder`](examples/embedder) | A process that owns its own signals: `Begin` from your handler, then `Wait` |

## Two traps worth reading before you configure anything

**An unconfigured reason's `OnDeadline` is not an interrupt hook.** Its deadline *is* the
grace period, so "outstanding at its deadline" and "overran the ceiling" are the same
instant: the callback fires with an already-cancelled context, on a goroutine nobody waits
for, as `Wait` returns. Configure any reason whose abandonment you intend to act on.
`Outcome.Waited` reporting the full grace period is how both this and a misspelled
`WithDeadline` surface — a typo is a valid `Reason` that simply nobody holds.

**The grace period is a ceiling, not a suggestion.** Set it below the platform's own —
Kubernetes' `terminationGracePeriodSeconds`, systemd's `TimeoutStopSec` — because SIGKILL
does not negotiate. `OnDeadline` returns nothing for the same reason: a callback cannot
extend the wait, and a legitimate need for more time is spelled `WithDeadline(r, longer)`.

## Development

```bash
GOWORK=off go test ./... -race
GOWORK=off make check
```

`GOWORK=off` is mandatory: it is how a module in this workspace is verified against its
real pinned dependencies. `drain` has none, which is the point.
