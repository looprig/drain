# Contributing to looprig/drain

Thanks for considering a contribution. `drain` is the leaf module that
coordinates graceful process termination — SIGTERM handling and idle
self-exit — for long-lived workloads; sibling repos in the looprig
ecosystem depend on it. This file is the short guide for working in *this*
repository.

## Before you write code

1. Read [`CLAUDE.md`](CLAUDE.md) (a.k.a. `AGENTS.md`) first. It is the
   authoritative source for the design, security, dependency, build, and
   code rules this module follows. PRs that contradict it will be asked to
   change.
2. **No third-party dependencies, ever.** This is the central, deliberate
   constraint on this module: there is no scenario here where stdlib is
   insufficient. Do not `go get` anything. `go.mod` has no `require`
   block and never will. If a task seems to need an external package,
   the task is wrong for this module — raise that in the issue/PR
   discussion before writing code, don't route around it.
3. Open an issue for anything non-trivial so direction can be agreed before
   you spend the time, especially for anything that touches the public
   contracts (`Coordinator`, `Reason`, `Cause`, `Outcome`, the `Option`
   set, or the exported error types).

## Design and security rules (the short version)

- **Strict typing.** No `any`/`interface{}` except at explicit
  serialization boundaries, narrowed immediately. Named types
  (`type Reason string`) over bare primitives when the value carries
  domain meaning. No untyped magic numbers/strings.
- **All errors are typed.** Every distinct failure mode that carries data
  is a concrete struct with an `Error()` method (and `Unwrap()` when it
  carries a cause); the one error with no fields is a sentinel. Callers
  classify with `errors.As`/`errors.Is` — never by string.
- **Contracts first.** Write the interface, then the implementation. Keep
  interfaces small and segregated; a caller never depends on methods it
  does not use.
- Return errors explicitly; never swallow with `_`.
- Functions over ~30 lines invite an SRP check before growing further.
- Validate all policy at the constructor boundary: `New` is the single
  validation point, and options never silently drop a bad value. Every
  blocking method takes a `context.Context` and the grace period is a hard
  ceiling — no unbounded blocking. Fail secure: an unconfigured reason
  gets the full grace period, and on ambiguity `drain` waits rather than
  abandons.
- No user callback runs under the lock or on a caller's goroutine.

## Build, test, and secure

Run these before pushing. CI runs the same.

```sh
make fmt          # gofmt the whole module in place
make fmt-check    # verify gofmt cleanliness without writing
make vet          # go vet
make test         # go test -race ./...           (always -race)
make staticcheck  # staticcheck ./...
make gosec        # gosec security scan
make vuln         # govulncheck ./...
make secure       # fmt-check + vet + staticcheck + gosec + vuln
make check        # the full pre-commit gate (all of the above + build)
```

`staticcheck`, `gosec`, and `govulncheck` are **not** module dependencies —
they are resolved from `PATH` (falling back to `$(go env GOPATH)/bin`) at
`make` time, exactly because of the no-third-party-dependencies rule above.
Adding them via `go get -tool` or a `require`/`tool` directive would put an
external package in `go.mod`, which this module does not permit under any
circumstance. If a target's binary isn't installed, the target prints an
install hint and skips gracefully rather than failing the build — install
what you need locally to get full coverage before sending a PR.

Every Go command in the Makefile runs with `GOWORK=off` so the parent
`go.work` at the workspace root never captures this module.

## Tests

- **Table-driven tests, mandatory**, each subtest calling `t.Parallel()`.
  Cover the happy path, boundary values (zero/empty/max), error cases, and
  domain edge cases (zero deadlines, unconfigured reasons, releases racing
  the deadline). Time-dependent behavior is tested through the injected
  `Clock`, not `time.Sleep`.
- A test that passes without `-race` but fails with it is **not passing**.
- Fuzz any parser of external input:
  `go test -fuzz=FuzzXxx -fuzztime=30s ./...`.
- Never assume a test framework or script beyond what's in the `Makefile`;
  if you change how tests run, update it.

## Pull requests

- Branch from `main`, name the branch something descriptive.
- One logical change per PR. Write a clear description: what, why, the
  design alternative you rejected, and how you verified. `make check` (and
  ideally `make secure`) output is welcome in the PR body.
- **This module is depended on by sibling repos.** Treat any change to a
  public contract — `Coordinator`, `Reason`, `Cause`, `Outcome`, the
  `Option` set, or the exported error types — as a breaking-change
  candidate: call it out explicitly in the PR description and think
  through who downstream needs to update.
- Don't force-push after review; add commits and let the reviewer squash.
- Don't commit secrets, tokens, or credentials. Don't add a new external
  dependency — see the dependency rule above; this is not negotiable via
  PR.
- Don't update `CLAUDE.md`, `Makefile`, or `go.mod` unless the change is
  the point of the PR.

## Code of conduct

Be excellent to each other. Discussions stay technical and respectful;
personal attacks, harassment, and discrimination are not welcome.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, as described in [`LICENSE`](LICENSE).
