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
