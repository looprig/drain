// Package embedderexample shows the path for a process that already owns its
// signals: Begin from your own handler, and Wait for the sequence.
//
// This is the reason Wait installs nothing. signal.Notify is additive and
// process-wide — it does not replace an existing registration, it adds one — so
// a library that quietly registers for SIGINT changes the behaviour of every
// program that links it. A TUI that intercepts Ctrl-C for its own UX, a server
// that already multiplexes SIGHUP and SIGTERM through one handler, or a test
// binary that must not be killed by its own dependency all keep their handler
// and call Begin from it. Run exists for the process that has none.
//
// The example drives Begin directly rather than installing a handler, which is
// exactly what such a handler would do: Begin returns promptly, records the
// cause, and never runs a user callback on its caller's goroutine — a signal
// handler is not a place to spend a grace period.
package embedderexample_test

import (
	"context"
	"fmt"
	"time"

	"github.com/looprig/drain"
	"github.com/looprig/drain/draintest"
)

// Deliberately left unconfigured, to show what that costs. A reason with no
// WithDeadline waits the FULL grace period, which is the cautious default —
// it never abandons work early — but it also means its OnDeadline report and
// the ceiling are the same instant, so it is not a working interrupt hook.
// Outcome.Waited reporting the whole grace period is how that shows up, and it
// is also how a misspelled WithDeadline shows up.
const reasonRequest drain.Reason = "tui/request"

const (
	gracePeriod = 30 * time.Second
	settle      = 5 * time.Second
)

func Example_embedderOwnsItsSignals() {
	clock := draintest.NewClock(time.Unix(0, 0))

	coordinator, err := drain.New(gracePeriod,
		drain.WithClock(clock),
		drain.WithPreDrainDelay(settle),
	)
	if err != nil {
		panic(err)
	}

	// A request already in flight when the operator hits Ctrl-C.
	release, ok := coordinator.Acquire(reasonRequest)
	defer release()
	if !ok {
		panic("expected the request to be admitted before the drain")
	}
	fmt.Println("draining before the signal:", coordinator.Draining())

	go func() {
		// The embedder's own handler, having decided that this particular
		// Ctrl-C means shut down rather than cancel the focused view.
		coordinator.Begin(drain.CauseSignal)

		// Admission stays open across the settle window for a signal drain, so
		// requests already routed here still land.
		clock.BlockUntil(2) // the settle timer and the grace ceiling
		clock.Advance(settle)

		// The in-flight request reaches a boundary and the process can exit.
		release()
	}()

	out, err := coordinator.Wait(context.Background())
	fmt.Println("cause:", out.Cause)
	fmt.Println("error:", err)
	fmt.Println("outstanding at exit:", len(out.Abandoned))
	fmt.Println("deadline applied to the request:", out.Waited[reasonRequest])

	// The coordinator is spent: it runs its sequence once.
	_, err = coordinator.Wait(context.Background())
	fmt.Println("second Wait:", err)
	_, ok = coordinator.Acquire(reasonRequest)
	fmt.Println("admitted after the drain:", ok)

	// Output:
	// draining before the signal: false
	// cause: signal
	// error: <nil>
	// outstanding at exit: 0
	// deadline applied to the request: 30s
	// second Wait: drain: already running
	// admitted after the drain: false
}
