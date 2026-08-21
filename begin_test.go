package drain

// Internal, like holds_test.go: the cause Begin records has no exported reader
// until Wait returns an Outcome in Task 7, and "the first cause wins" is the one
// thing this task is for. Everything observable through the exported surface —
// Draining, and which side of the admission cutoff Acquire lands on — is
// asserted through it.

import (
	"sync"
	"testing"
)

// recordedCause reads the cause under the lock that guards it. Reading the field
// bare would be a data race against a concurrent Begin, which is exactly what
// TestBeginFirstCauseWins runs.
func recordedCause(c *Coordinator) Cause {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cause
}

// admits reports whether Acquire still grants, without leaving a hold behind.
func admits(c *Coordinator) bool {
	release, ok := c.Acquire("probe")
	release()
	return ok
}

// TestBeginRecordsCause covers each of the four causes that are valid arguments.
// CauseUnknown is not among them: it names the not-yet-started state and is
// normalized, which TestBeginNormalizesInvalidCause covers.
func TestBeginRecordsCause(t *testing.T) {
	t.Parallel()

	for _, cause := range []Cause{CauseSignal, CauseIdle, CauseExplicit, CauseContext} {
		t.Run(cause.String(), func(t *testing.T) {
			t.Parallel()

			c := newCoordinator(t)
			c.Begin(cause)
			if got := recordedCause(c); got != cause {
				t.Errorf("recorded cause = %v, want %v", got, cause)
			}
		})
	}
}

// TestBeginFirstCauseWins pins the determinism Outcome.Cause depends on. The
// concurrent case matters as much as the sequential one: whichever cause wins a
// race, the admission flag must agree with it, because the two are set in the
// same critical section or not at all.
func TestBeginFirstCauseWins(t *testing.T) {
	t.Parallel()

	t.Run("sequential", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name  string
			calls []Cause
			want  Cause
		}{
			{name: "idle then signal", calls: []Cause{CauseIdle, CauseSignal}, want: CauseIdle},
			{name: "signal then idle", calls: []Cause{CauseSignal, CauseIdle}, want: CauseSignal},
			{
				name:  "context then explicit then signal",
				calls: []Cause{CauseContext, CauseExplicit, CauseSignal},
				want:  CauseContext,
			},
			{
				// The normalized cause wins too: a later valid cause cannot
				// overwrite it.
				name:  "invalid then signal",
				calls: []Cause{Cause(99), CauseSignal},
				want:  CauseExplicit,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				c := newCoordinator(t)
				for _, cause := range tc.calls {
					c.Begin(cause)
				}
				if got := recordedCause(c); got != tc.want {
					t.Errorf("recorded cause = %v after Begin%v, want %v", got, tc.calls, tc.want)
				}
			})
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		t.Parallel()

		causes := []Cause{CauseSignal, CauseIdle, CauseExplicit, CauseContext}
		c := newCoordinator(t)

		var wg sync.WaitGroup
		for i := range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.Begin(causes[i%len(causes)])
			}()
		}
		wg.Wait()

		got := recordedCause(c)
		if got == CauseUnknown {
			t.Fatal("recorded cause = unknown after 32 concurrent Begins, want one of the four")
		}
		// The invariant that would break if the cause and the admission flag
		// were written under separate locks: a signal drain that closed
		// admission at Begin, or an idle drain that left it open.
		if wantAdmits := got == CauseSignal; admits(c) != wantAdmits {
			t.Errorf("admission open = %t with recorded cause %v, want %t", !wantAdmits, got, wantAdmits)
		}
	})
}

// TestBeginIsIdempotent asserts a later Begin is a no-op in every respect, not
// merely in the cause it records. A second Begin that re-ran the admission
// decision would let Begin(CauseIdle) slam the settle window shut on a signal
// drain — the exact burst of 503s the window exists to prevent.
func TestBeginIsIdempotent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		first      Cause
		second     Cause
		wantAdmits bool
	}{
		{name: "signal then idle keeps admission open", first: CauseSignal, second: CauseIdle, wantAdmits: true},
		{name: "signal then signal keeps admission open", first: CauseSignal, second: CauseSignal, wantAdmits: true},
		{name: "idle then signal keeps admission closed", first: CauseIdle, second: CauseSignal},
		{name: "explicit then explicit keeps admission closed", first: CauseExplicit, second: CauseExplicit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newCoordinator(t)
			c.Begin(tc.first)
			beforeCause, beforeAdmits := recordedCause(c), admits(c)

			c.Begin(tc.second)

			if got := recordedCause(c); got != beforeCause {
				t.Errorf("recorded cause = %v after the second Begin, want %v", got, beforeCause)
			}
			if got := admits(c); got != beforeAdmits || got != tc.wantAdmits {
				t.Errorf("admission open = %t after the second Begin, want %t", got, tc.wantAdmits)
			}
			if !c.Draining() {
				t.Error("Draining() = false after two Begins, want true")
			}
		})
	}
}

// TestBeginSetsDrainingImmediately asserts the flag is set synchronously, on
// Begin's return, and not on some goroutine the caller would have to wait for. A
// readiness probe that reads it one instruction after Begin must already see the
// drain.
func TestBeginSetsDrainingImmediately(t *testing.T) {
	t.Parallel()

	c := newCoordinator(t)
	if c.Draining() {
		t.Fatal("Draining() = true before Begin, want false")
	}
	c.Begin(CauseSignal)
	if !c.Draining() {
		t.Error("Draining() = false immediately after Begin returned, want true")
	}
}

// TestBeginDoesNotRunCallbacksOnCallerGoroutine pins the rule that keeps a hung
// callback inside the grace ceiling: user code runs on coordinator-owned
// goroutines, inside Wait, never on the caller's stack. Begin is called from
// signal handlers, where blocking is not an option.
//
// The callback here blocks forever if it is ever invoked, so a synchronous
// implementation fails this as a hang rather than an assertion — see the plan's
// note that this is the expensive failure mode. The select is for the milder
// defect of a callback that runs synchronously and returns.
func TestBeginDoesNotRunCallbacksOnCallerGoroutine(t *testing.T) {
	t.Parallel()

	quiesced := make(chan Cause, 1)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	c := newCoordinator(t, WithOnQuiesce(func(cause Cause) {
		quiesced <- cause
		<-block
	}))

	c.Begin(CauseSignal)

	select {
	case cause := <-quiesced:
		t.Errorf("OnQuiesce ran during Begin with cause %v; it must run on a coordinator-owned goroutine", cause)
	default:
	}
	if !c.Draining() {
		t.Error("Draining() = false after Begin, want true")
	}
}

// TestSignalCauseKeepsAdmissionOpen is one half of the point of this task: for a
// signal the coordinator is draining while admission is still open. The
// load balancer has not finished withdrawing this endpoint, so the requests
// already in flight toward it still have to land; they stop being admitted at
// the settle boundary in Task 7, not here.
func TestSignalCauseKeepsAdmissionOpen(t *testing.T) {
	t.Parallel()

	const reason Reason = "harness/active_turn"
	c := newCoordinator(t)
	c.Begin(CauseSignal)

	if !c.Draining() {
		t.Fatal("Draining() = false after Begin(CauseSignal), want true")
	}
	release, ok := c.Acquire(reason)
	if !ok {
		t.Fatal("Acquire refused during a signal drain's settle window, want granted")
	}
	if got := c.Holds()[reason]; got != 1 {
		t.Errorf("Holds()[%q] = %d, want 1", reason, got)
	}
	release()
}

// TestIdleCauseClosesAdmissionAtBegin is the other half. Nothing external
// triggered these drains, so there is no in-flight routing to protect and
// admitting work would land it on a process that has already decided to die.
// Holds taken before Begin are untouched: closing admission refuses new work, it
// does not cancel work already granted.
func TestIdleCauseClosesAdmissionAtBegin(t *testing.T) {
	t.Parallel()

	const existing Reason = "harness/active_turn"

	for _, cause := range []Cause{CauseIdle, CauseExplicit, CauseContext} {
		t.Run(cause.String(), func(t *testing.T) {
			t.Parallel()

			c := newCoordinator(t)
			keep, ok := c.Acquire(existing)
			if !ok {
				t.Fatal("Acquire refused before Begin, want granted")
			}

			c.Begin(cause)

			if !c.Draining() {
				t.Errorf("Draining() = false after Begin(%v), want true", cause)
			}
			release, ok := c.Acquire("harness/pending_gate")
			if ok {
				t.Errorf("Acquire granted after Begin(%v), want refused: admission closes at Begin", cause)
			}
			if release == nil {
				t.Fatal("Acquire returned a nil release on the refused path")
			}
			release()

			if got := c.Holds()[existing]; got != 1 {
				t.Errorf("Holds()[%q] = %d, want 1: an existing hold must survive Begin", existing, got)
			}
			keep()
			if got := c.Holds(); got != nil {
				t.Errorf("Holds() = %v after releasing, want nil: release works after Begin", got)
			}
		})
	}
}

// TestBeginNormalizesInvalidCause covers the amended rule. Begin is called from
// signal handlers and shutdown paths, so an out-of-range cause must not panic;
// it is recorded as CauseExplicit, which is the literal truth — Begin was called
// directly — and takes the fail-secure admission path rather than inheriting
// CauseSignal's open settle window. CauseUnknown is included because it names
// the not-yet-started state and must never be observable on a started sequence.
func TestBeginNormalizesInvalidCause(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		cause Cause
	}{
		{name: "unknown", cause: CauseUnknown},
		{name: "just past the set", cause: Cause(5)},
		{name: "far out of range", cause: Cause(99)},
		{name: "max", cause: Cause(255)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newCoordinator(t)
			c.Begin(tc.cause) // must not panic

			if got := recordedCause(c); got != CauseExplicit {
				t.Errorf("recorded cause = %v after Begin(%v), want %v", got, tc.cause, CauseExplicit)
			}
			if !c.Draining() {
				t.Errorf("Draining() = false after Begin(%v), want true", tc.cause)
			}
			if admits(c) {
				t.Errorf("Acquire granted after Begin(%v), want refused: the fail-secure path closes admission at Begin", tc.cause)
			}
		})
	}
}
