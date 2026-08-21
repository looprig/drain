# -timeout is explicit and short. The default is ten minutes, and this suite's
# most valuable regression — a user callback invoked under c.mu — is caught ONLY
# as a hang: a goroutine parked on sync.Mutex.Lock is not durably blocked, so
# testing/synctest cannot see it and no assertion in the suite converts it to a
# failure. Ten minutes of silence for a deadlock the test knows about is a bad
# trade; two minutes is far more than the whole suite needs, since nothing in it
# sleeps in real time.
test:
	GOWORK=off go test -race -timeout 120s ./...

fmt:
	gofmt -w .

fmt-check:
	@files="$(CHECK_GO_FILES)"; \
	if [ -n "$$files" ] && [ -n "$$(gofmt -l $$files)" ]; then gofmt -l $$files; exit 1; fi

vet:
	GOWORK=off go vet ./...

# `secure` is an alias for the security-relevant half of `check`, not a second
# implementation of it. It used to be one: PATH-resolved staticcheck/gosec/
# govulncheck targets that printed "not installed; skipping" and exited 0, so on
# a machine with none of the three installed `make secure` was green while
# having checked nothing at all. A security target that passes by finding no
# tools is worse than no target. Both surfaces now run the same pinned versions
# through `go run`, which adds nothing to go.mod.
secure: fmt-check vet check-staticcheck check-gosec check-vuln

# --- standardized check surface -------------------------------------------
# One target, the same set of checks, in every module. CI calls exactly this,
# so a check can no longer pass locally and be silently absent in CI (or the
# reverse). The lint/security tools are run at a pinned version with `go run`,
# which adds nothing to go.mod: this module's CLAUDE.md forbids any go.mod
# dependency at all — there is no require block — and sanctions dev-tool
# BINARIES instead.
#
# CHECK_GO_DIRS scopes gosec: gosec is NOT module-aware, so a bare ./... is a
# filesystem walk that descends into nested .worktrees/ checkouts, which are
# separate modules. go vet and staticcheck are module-aware and need no scope.
CHECK_GO_DIRS = $(shell GOWORK=off go list -f '{{.Dir}}' ./...)
# CHECK_GO_FILES is what gofmt gets. Never hand it CHECK_GO_DIRS: gofmt RECURSES
# into directory operands, so for a module with a root package it would walk the
# whole tree, nested .worktrees/ checkouts included.
CHECK_GO_FILES = $(foreach dir,$(CHECK_GO_DIRS),$(wildcard $(dir)/*.go))

check-staticcheck:
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

check-gosec:
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 -quiet $(CHECK_GO_DIRS)

check-vuln:
	GOWORK=off go mod verify
	GOWORK=off go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

build:
	GOWORK=off go build ./...

check: fmt-check vet check-staticcheck check-gosec check-vuln test build

.PHONY: build check check-gosec check-staticcheck check-vuln fmt fmt-check secure test vet
