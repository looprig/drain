// Package startupholdexample shows the startup-hold pattern, which is what
// makes WithIdleTimeout safe to enable on a process that is slow to boot.
//
// The idle clock's epoch is Wait entry, not New — a coordinator constructed at
// boot and not serving for another thirty seconds would otherwise, with a
// twenty-second idle timeout, kill itself before it ever answered a request.
// Wait entry is not far enough on its own, though: a process that enters Wait
// and then spends ten minutes loading a model is idle by every measure drain
// can see. The fix is a hold taken in main and released when the process is
// actually ready to serve. No idle timer arms until then, because a timer arms
// only when the total reaches zero.
//
// Enable WithIdleTimeout only where a clean exit is the intended lifecycle: a
// Job or KEDA ScaledJob, an operator-owned per-session pod, or a
// systemd/Compose unit that is not restarted. Under a Deployment, ReplicaSet or
// StatefulSet the kubelet restarts the container on any exit, exit 0 included,
// and this option produces a CrashLoopBackOff.
package startupholdexample_test

import (
	"context"
	"fmt"
	"time"

	"github.com/looprig/drain"
	"github.com/looprig/drain/draintest"
)

const reasonStartup drain.Reason = "server/startup"

const (
	idleTimeout = 10 * time.Minute
	gracePeriod = 30 * time.Second
	settle      = 5 * time.Second
	bootTime    = time.Hour // a deliberately absurd boot, to make the point
)

func Example_startupHold() {
	clock := draintest.NewClock(time.Unix(0, 0))

	coordinator, err := drain.New(gracePeriod,
		drain.WithClock(clock),
		drain.WithPreDrainDelay(settle),

		// Correct here: this is an operator-owned per-session pod, and a clean
		// exit is what the operator is waiting for.
		drain.WithIdleTimeout(idleTimeout),
	)
	if err != nil {
		panic(err)
	}

	// Taken in main, before Wait, and released when the process is ready to
	// serve. Until then the total is nonzero and no idle timer exists.
	startup, ok := coordinator.Acquire(reasonStartup)
	if !ok {
		panic("expected the startup hold to be admitted")
	}

	go func() {
		// Boot. An hour of it, and not one idle timer.
		clock.Advance(bootTime)
		fmt.Println("idle timers armed while booting:", clock.Armed())

		// Ready to serve. This is the transition that arms the idle timer, so
		// the ten minutes start here rather than an hour ago.
		startup()
		clock.BlockUntil(1)
		fmt.Println("idle timers armed once ready:", clock.Armed())

		// Nothing arrives. The idle timeout expires and the coordinator begins
		// draining itself with CauseIdle, which runs the same sequence a signal
		// would: quiesce, settle, cut admission, wait, exit.
		clock.Advance(idleTimeout)
		clock.BlockUntil(2) // the settle timer and the grace ceiling
		clock.Advance(settle)
	}()

	// Wait installs no signal handling. Nothing here needs it: the trigger is
	// the idle timeout, not an operator.
	out, err := coordinator.Wait(context.Background())
	fmt.Println("cause:", out.Cause)
	fmt.Println("error:", err)
	fmt.Println("outstanding at exit:", len(out.Abandoned))

	// Output:
	// idle timers armed while booting: 0
	// idle timers armed once ready: 1
	// cause: idle
	// error: <nil>
	// outstanding at exit: 0
}
