# CLAUDE.md — drain

`drain` is a **stdlib-only leaf module**. It coordinates graceful process termination —
SIGTERM handling and idle self-exit — for long-lived workloads; it depends on nothing
outside the Go standard library, and it must stay that way.

## Dependencies

- **No third-party dependencies, ever.** There is no scenario in this module where stdlib
  is insufficient. Do not `go get` anything. `go.mod` carries the module line and the Go
  version and nothing else — there is no `require` block, now or ever. If a task seems to
  need an external package, the task is wrong for this module.
  (`make check`'s staticcheck/gosec/govulncheck run at pinned versions through `go run`,
  as dev-tool binaries rather than go.mod dependencies — this does not contradict the
  rule above.)

## Scope

- `drain` answers one question for a process: when is it safe to exit, and what does it
  owe the work still in flight?
- **It persists nothing.** No sessions, journals, workspaces, or checkpoints. It reports
  that a deadline arrived and lets the owner decide what to do about it.
- **Occupancy is not in this module.** A hold means "cannot exit"; occupancy means "how
  full am I". They are different numbers and conflating them breaks both.
- **No HTTP, no metrics, no environment reads.** `drain` reports state; the composition
  root wires that state to a probe and a gauge.
- The only package beyond the root is `drain/draintest`, which ships the deterministic
  clock and is also stdlib-only.

## Code rules (same discipline as the consuming repos)

- **Strict typing.** No `any`/`interface{}` except at explicit serialization boundaries,
  narrowed immediately. Named types (`type Reason string`) over bare primitives when the
  value carries domain meaning. No untyped magic numbers/strings.
- **All errors are typed.** Every distinct failure mode that carries data is a concrete
  struct with an `Error()` method (and `Unwrap()` when it carries a cause). **Never return
  `errors.New(...)`/`fmt.Errorf(...)` from a package-level API** — the single exception is
  a fieldless failure, which is a package-level sentinel `var` (`ErrAlreadyRunning`)
  matched by identity, not an error value minted at the return site. Callers classify with
  `errors.As`/`errors.Is` — never by string.
  - Settled, so it is not relitigated per task: a non-nil `InvalidPolicyError.Err`
    constructed with `errors.New`/`fmt.Errorf` is a **wrapped detail cause carried inside
    a typed struct**, reached through `Unwrap`. It is not "returning `errors.New` from a
    package-level API" — the value the API returns is the typed struct — and the rule
    above permits it.
- **Contracts first.** Write the interface, then the implementation. Keep interfaces small
  and segregated; a caller never depends on methods it does not use.
- Return errors explicitly; never swallow with `_`.
- Functions over ~30 lines invite an SRP check before growing further.

## Concurrency

- Every exported method is safe from any goroutine.
- One mutex guards the per-reason counters, the total, and the admission flag **together**;
  the coordinator's decisions depend on the state it guards, so they are one critical
  section.
- **No user callback ever runs under the lock**, and none ever runs on a caller's
  goroutine: callbacks run on coordinator-owned goroutines so an owner's `release()` can
  run from inside `OnDeadline` without deadlocking.

## Security

- Validate all policy at the constructor boundary. `New` is the single validation point;
  options record intent and never silently drop a bad value.
- Fail secure: an unconfigured reason gets the full grace period, and the zero `Cause` is
  `CauseUnknown`. On error or ambiguity, wait rather than abandon.
- Every blocking API takes a `context.Context`; the grace period is a hard ceiling. No
  unbounded blocking.

## Testing

- **Table-driven tests, mandatory**, each with `t.Parallel()` where the case allows.
  Cover happy path, boundary values (zero/empty/max), error cases, and domain edge cases
  (zero deadlines, unconfigured reasons, releases racing the deadline).
- **Always `-race`:** `GOWORK=off go test -race ./...`. A test that needs `-race` to fail
  is not passing.
- Time-dependent behavior is tested through the injected `Clock`, not `time.Sleep`.
- `gofmt`-clean and `go vet`-clean at all times.

## Build & workspace

- **Every Go command runs with `GOWORK=off`** — there is a `go.work` at the workspace root
  that must not capture this module.
- `make check` (fmt-check + vet + staticcheck + gosec + vuln + `-race` test + build)
  before every commit.
