package drain

// Internal, like options_test.go, though nearly everything here is asserted
// through the exported surface a caller uses. The one internal reach left is
// outstandingTotal: the running total the mutex guards alongside the per-reason
// counters has no exported reader by design — Holds is per reason — and the
// invariant that it agrees with the sum of the counters is worth asserting
// directly, because the wait phase is driven by the total and not by the map.
//
// This file used to close admission by writing the flag itself, which modelled
// the cutover differently from the way production performs it: production closes
// admission and snapshots the outstanding holds in one critical section, and a
// helper that only wrote the flag could not have exercised that. Admission is
// now closed the way a process closes it — by beginning a drain.

import (
	"sync"
	"testing"
	"time"
)

// testGrace is the grace period every coordinator in this file is built with.
// Task 5 has no sequence, so no test here can observe it; it exists only to
// satisfy New.
const testGrace = 30 * time.Second

// newCoordinator builds a default-policy coordinator or fails the test.
func newCoordinator(t *testing.T, opts ...Option) *Coordinator {
	t.Helper()
	c, err := New(testGrace, opts...)
	if err != nil {
		t.Fatalf("New(%v) = _, %v, want no error", testGrace, err)
	}
	return c
}

// outstandingTotal reads the running total the mutex guards alongside the
// per-reason counters. It has no exported reader — Holds is per reason — and the
// invariant that it agrees with the sum of the counters is worth asserting
// directly, because the wait phase will be driven by the total and not by the
// map.
func outstandingTotal(c *Coordinator) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// TestAcquireGrantsWhenAdmissionOpen covers the open-admission path for the
// kinds of reason a consumer can pass, the unspecified zero value included: it
// is an ordinary key, not a rejection.
func TestAcquireGrantsWhenAdmissionOpen(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		reason Reason
	}{
		{name: "package-prefixed reason", reason: "harness/active_turn"},
		{name: "unspecified reason", reason: ReasonUnspecified},
		{name: "reason with no prefix", reason: "subprocess"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newCoordinator(t)
			release, ok := c.Acquire(tc.reason)
			if !ok {
				t.Fatalf("Acquire(%q) = _, false, want true with admission open", tc.reason)
			}
			if release == nil {
				t.Fatal("Acquire returned a nil release")
			}
			if got := c.Holds()[tc.reason]; got != 1 {
				t.Errorf("Holds()[%q] = %d, want 1", tc.reason, got)
			}
			if got := outstandingTotal(c); got != 1 {
				t.Errorf("total = %d, want 1", got)
			}

			release()
			if got := c.Holds(); got != nil {
				t.Errorf("Holds() = %v after release, want nil", got)
			}
			if got := outstandingTotal(c); got != 0 {
				t.Errorf("total = %d after release, want 0", got)
			}
		})
	}
}

// TestReleaseIsIdempotentPerClosure is the assertion this task exists to make.
// Idempotency is per returned closure, never per reason: two Acquires on one
// reason are two counts and two independent releases, so calling one of them
// twice must decrement exactly once and must not swallow the other's count. A
// coordinator that guarded idempotency per reason — one sync.Once keyed by
// Reason — passes a casual single-hold test and loses a count here.
func TestReleaseIsIdempotentPerClosure(t *testing.T) {
	t.Parallel()

	const reason Reason = "harness/active_turn"
	c := newCoordinator(t)

	releaseFirst, ok := c.Acquire(reason)
	if !ok {
		t.Fatal("first Acquire refused with admission open")
	}
	releaseSecond, ok := c.Acquire(reason)
	if !ok {
		t.Fatal("second Acquire refused with admission open")
	}
	if got := c.Holds()[reason]; got != 2 {
		t.Fatalf("Holds()[%q] = %d after two Acquires, want 2", reason, got)
	}

	releaseFirst()
	releaseFirst()
	releaseFirst()
	if got := c.Holds()[reason]; got != 1 {
		t.Errorf("Holds()[%q] = %d after calling one release three times, want 1", reason, got)
	}
	if got := outstandingTotal(c); got != 1 {
		t.Errorf("total = %d after calling one release three times, want 1", got)
	}

	releaseSecond()
	if got := c.Holds(); got != nil {
		t.Errorf("Holds() = %v after both releases, want nil", got)
	}
	if got := outstandingTotal(c); got != 0 {
		t.Errorf("total = %d after both releases, want 0", got)
	}
}

// TestReleaseNeverNil pins the fail-secure half of the comma-ok shape: release
// is non-nil on both sides of ok, so `release, _ := c.Acquire(r); defer
// release()` is safe even in the caller that never checks. On the refused side
// it must also be inert — a no-op that decremented would drive somebody else's
// counter negative.
func TestReleaseNeverNil(t *testing.T) {
	t.Parallel()

	const (
		granted Reason = "harness/active_turn"
		refused Reason = "harness/pending_gate"
	)

	for _, tc := range []struct {
		name            string
		admissionClosed bool
		wantOK          bool
		wantTotal       int // outstanding immediately after the second Acquire
	}{
		{name: "admission open", wantOK: true, wantTotal: 2},
		{name: "admission closed", admissionClosed: true, wantTotal: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newCoordinator(t)
			// A hold taken before admission closes, so a no-op release that
			// wrongly decremented would be visible as a lost count rather than
			// silently making an already-empty coordinator emptier.
			keep, ok := c.Acquire(granted)
			if !ok {
				t.Fatal("Acquire refused with admission open")
			}
			defer keep()

			if tc.admissionClosed {
				// Begin(CauseIdle), because an idle drain closes admission at
				// Begin: that is the cutover a consumer actually meets.
				c.Begin(CauseIdle)
			}

			release, ok := c.Acquire(refused)
			if ok != tc.wantOK {
				t.Fatalf("Acquire(%q) = _, %t, want %t", refused, ok, tc.wantOK)
			}
			if release == nil {
				t.Fatalf("Acquire(%q) returned a nil release with ok == %t", refused, ok)
			}
			if got := outstandingTotal(c); got != tc.wantTotal {
				t.Fatalf("total = %d after Acquire(%q), want %d", got, refused, tc.wantTotal)
			}

			// Twice, because a refused release must be as inert on the second
			// call as on the first, and a granted one must decrement once.
			release()
			release()

			if got := c.Holds()[granted]; got != 1 {
				t.Errorf("Holds()[%q] = %d, want 1: the release touched an unrelated reason", granted, got)
			}
			if got := outstandingTotal(c); got != 1 {
				t.Errorf("total = %d after releasing, want 1: only the unrelated hold should remain", got)
			}
		})
	}
}

// TestHoldsReturnsSnapshotCopy proves the returned map is never the
// coordinator's own. A caller handed the live map could corrupt the counters the
// wait phase is about to read.
func TestHoldsReturnsSnapshotCopy(t *testing.T) {
	t.Parallel()

	const reason Reason = "harness/subprocess"
	c := newCoordinator(t)
	release, ok := c.Acquire(reason)
	if !ok {
		t.Fatal("Acquire refused with admission open")
	}
	defer release()

	snapshot := c.Holds()
	snapshot[reason] = 99
	snapshot["injected"] = 1
	delete(snapshot, reason)

	fresh := c.Holds()
	if got := fresh[reason]; got != 1 {
		t.Errorf("Holds()[%q] = %d after mutating an earlier snapshot, want 1", reason, got)
	}
	if _, injected := fresh["injected"]; injected {
		t.Error(`Holds() contains "injected": the snapshot aliases the coordinator's map`)
	}
}

// TestHoldsOmitsZeroCounts asserts a reason that went back to zero is absent,
// not present with a zero value. A caller iterating the snapshot must not see a
// reason drain is not waiting for.
func TestHoldsOmitsZeroCounts(t *testing.T) {
	t.Parallel()

	const (
		held     Reason = "harness/active_turn"
		released Reason = "harness/pending_gate"
	)
	c := newCoordinator(t)

	keep, ok := c.Acquire(held)
	if !ok {
		t.Fatal("Acquire refused with admission open")
	}
	defer keep()
	drop, ok := c.Acquire(released)
	if !ok {
		t.Fatal("Acquire refused with admission open")
	}
	drop()

	holds := c.Holds()
	if got, present := holds[released]; present {
		t.Errorf("Holds()[%q] = %d, present: want the reason omitted at zero", released, got)
	}
	if got := len(holds); got != 1 {
		t.Errorf("len(Holds()) = %d, want 1: %v", got, holds)
	}
}

// TestHoldsIsNilWhenEmpty pins the module's map policy — nil, never an empty
// non-nil map — so that `m == nil` and `len(m) == 0` can never disagree for a
// caller. It must hold both before any hold exists and after every hold has
// gone, which is the case an implementation that clones a live empty map gets
// wrong.
func TestHoldsIsNilWhenEmpty(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		prepare func(c *Coordinator)
	}{
		{name: "no hold ever acquired", prepare: func(*Coordinator) {}},
		{
			name: "every hold released",
			prepare: func(c *Coordinator) {
				release, _ := c.Acquire("harness/active_turn")
				release()
			},
		},
		{
			name: "several reasons all released",
			prepare: func(c *Coordinator) {
				first, _ := c.Acquire("harness/active_turn")
				second, _ := c.Acquire("harness/pending_gate")
				first()
				second()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newCoordinator(t)
			tc.prepare(c)
			if got := c.Holds(); got != nil {
				t.Errorf("Holds() = %#v (len %d), want nil", got, len(got))
			}
		})
	}
}

// TestDrainingFalseBeforeBegin covers the advisory flag before any sequence
// exists. Acquiring and releasing holds is not a drain, so nothing here may set
// it.
func TestDrainingFalseBeforeBegin(t *testing.T) {
	t.Parallel()

	c := newCoordinator(t)
	if c.Draining() {
		t.Error("Draining() = true on a fresh coordinator, want false")
	}

	release, ok := c.Acquire("harness/active_turn")
	if !ok {
		t.Fatal("Acquire refused with admission open")
	}
	if c.Draining() {
		t.Error("Draining() = true while a hold is outstanding, want false")
	}
	release()
	if c.Draining() {
		t.Error("Draining() = true after the hold released, want false")
	}
}

// TestConcurrentAcquireRelease is the -race test for the critical section. It
// asserts the invariant that outlives this task: a snapshot never shows a
// negative or absent-but-counted reason, and the books balance once every
// goroutine has finished.
func TestConcurrentAcquireRelease(t *testing.T) {
	t.Parallel()

	reasons := []Reason{"harness/active_turn", "harness/pending_gate", ReasonUnspecified}
	const (
		goroutines = 16
		iterations = 200
	)

	c := newCoordinator(t)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reason := reasons[g%len(reasons)]
			for range iterations {
				release, ok := c.Acquire(reason)
				if !ok {
					// Unreachable in this task: nothing closes admission.
					// Asserted rather than assumed, so a future change that
					// closes admission early fails here instead of quietly
					// turning this into a no-op loop.
					t.Errorf("Acquire(%q) refused with admission open", reason)
					return
				}
				release()
				release()
			}
		}()
	}

	// A concurrent reader, so the snapshot path is exercised under -race
	// against the writers rather than only after them.
	done := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			for reason, count := range c.Holds() {
				if count <= 0 {
					t.Errorf("Holds()[%q] = %d, want a positive count", reason, count)
					return
				}
			}
		}
	}()

	wg.Wait()
	close(done)
	readerWG.Wait()

	if got := c.Holds(); got != nil {
		t.Errorf("Holds() = %v after every hold released, want nil", got)
	}
	if got := outstandingTotal(c); got != 0 {
		t.Errorf("total = %d after every hold released, want 0", got)
	}
}
