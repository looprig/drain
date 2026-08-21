// Package kubernetesexample shows the shape drain was built for: a pod shared
// by several sessions, drained by the SIGTERM the kubelet sends before it
// starts counting terminationGracePeriodSeconds.
//
// Two kinds of work are in flight and they want opposite policies. A turn that
// is mid-inference will finish if it is given a little time, so it gets a
// deadline inside the grace period. A gate that is waiting for a human to
// approve a tool call will not finish at all, so it gets WithDeadline(r, 0) and
// is abandoned the instant admission closes — there is nothing to wait for, and
// waiting costs the turn its budget.
//
// The clock is injected so the example runs in microseconds. A production main
// passes no WithClock and the same code drains over five and thirty real
// seconds.
package kubernetesexample_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/looprig/drain"
	"github.com/looprig/drain/draintest"
)

// The consumer's vocabulary. drain defines no domain reasons; a consumer
// declares its own as typed constants, and a misspelling here is a valid reason
// nobody holds rather than a compile error — which is what Outcome.Waited is
// for.
const (
	reasonActiveTurn  drain.Reason = "harness/active_turn"
	reasonPendingGate drain.Reason = "harness/pending_gate"
)

const (
	gracePeriod = 30 * time.Second // must be under terminationGracePeriodSeconds
	settle      = 5 * time.Second  // long enough for kube-proxy to stop routing here
	turnBudget  = 20 * time.Second // leaves 10s of the ceiling for everything else
)

func Example_kubernetesSharedPod() {
	// ready is what the /readyz handler reads. Failing the probe is the whole
	// job of OnQuiesce: it is what makes the endpoint controller pull this pod
	// out of the Service, and the settle window below is the time that removal
	// needs to propagate.
	var ready atomic.Bool
	ready.Store(true)

	// Closed by OnQuiesce. A production readiness handler just reads the flag;
	// the channel is here so the example can let the settle window elapse only
	// once the probe has actually been failed, which is the ordering the settle
	// window exists to protect.
	readinessFailed := make(chan struct{})

	// Closed by OnDeadline once the pending gate has been abandoned.
	gateAbandoned := make(chan struct{})

	clock := draintest.NewClock(time.Unix(0, 0))

	var releaseGate func()
	coordinator, err := drain.New(gracePeriod,
		drain.WithClock(clock),
		drain.WithPreDrainDelay(settle),

		// A turn gets most of the budget. A gate waiting on a human gets none:
		// zero means abandon at the cut.
		drain.WithDeadline(reasonActiveTurn, turnBudget),
		drain.WithDeadline(reasonPendingGate, 0),

		drain.WithOnQuiesce(func(cause drain.Cause) {
			ready.Store(false)
			close(readinessFailed)
		}),

		// Invoked once per reason whose deadline expired with work still
		// outstanding. The expected shape is exactly this: interrupt the work,
		// and let its deferred release run right here on the callback's own
		// goroutine.
		drain.WithOnDeadline(func(ctx context.Context, r drain.Reason, outstanding int) {
			fmt.Printf("abandoning %s with %d outstanding\n", r, outstanding)
			if r == reasonPendingGate {
				releaseGate()
				close(gateAbandoned)
			}
		}),

		// No WithIdleTimeout. Under a Deployment the kubelet restarts the
		// container on any exit, so a process that exits when idle crashloops.
	)
	if err != nil {
		panic(err)
	}

	// Work already in flight when the signal arrives. Acquire's ok is the sole
	// admission authority — a Draining() check before this would be a TOCTOU
	// race — and release is never nil, so the defer idiom is always safe.
	releaseTurn, ok := coordinator.Acquire(reasonActiveTurn)
	defer releaseTurn()
	if !ok {
		panic("expected the turn to be admitted before the drain")
	}
	gate, ok := coordinator.Acquire(reasonPendingGate)
	defer gate()
	if !ok {
		panic("expected the gate to be admitted before the drain")
	}
	releaseGate = gate

	// Run is Wait plus signal installation. On the first SIGTERM or SIGINT it
	// restores the default disposition and then calls Begin(CauseSignal), in
	// that order, so a second Ctrl-C still kills a wedged drain.
	type result struct {
		out drain.Outcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := coordinator.Run(context.Background())
		done <- result{out, err}
	}()

	// Stands in for the kubelet's SIGTERM, so the example is deterministic.
	// In production nothing calls this — the handler Run installed does.
	coordinator.Begin(drain.CauseSignal)

	<-readinessFailed
	fmt.Println("readyz:", ready.Load())

	// The settle window. Admission stays OPEN through it for a signal drain,
	// because the requests already routed here have to be allowed to land;
	// refusing them turns a graceful shutdown into a burst of 503s.
	clock.BlockUntil(2) // the settle timer and the grace ceiling
	clock.Advance(settle)

	// Admission has closed. The gate's zero deadline is already spent, so it is
	// abandoned here; the turn has its remaining budget to finish in.
	<-gateAbandoned
	releaseTurn()

	got := <-done
	fmt.Println("cause:", got.out.Cause)
	fmt.Println("error:", got.err)
	fmt.Println("still outstanding at exit:", len(got.out.Abandoned))
	fmt.Println("waited on the turn for:", got.out.Waited[reasonActiveTurn])
	fmt.Println("waited on the gate for:", got.out.Waited[reasonPendingGate])

	// Output:
	// readyz: false
	// abandoning harness/pending_gate with 1 outstanding
	// cause: signal
	// error: <nil>
	// still outstanding at exit: 0
	// waited on the turn for: 20s
	// waited on the gate for: 0s
}
