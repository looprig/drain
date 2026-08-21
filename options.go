package drain

import (
	"context"
	"os"
	"slices"
	"syscall"
	"time"
)

// Default policy. The settle window is the only one that is not simply "off":
// 5s suits a signal-caused drain behind kube-proxy, and it is deliberately not
// zero, because refusing the requests already routed here when the signal
// arrived turns a graceful shutdown into a burst of 503s.
const (
	defaultPreDrainDelay = 5 * time.Second
	defaultIdleTimeout   = 0 // idle self-exit is opt-in; it crashloops under a ReplicaSet
)

// config is the policy a Coordinator is built from. Options write to it and New
// reads it; nothing else ever sees it, and it is copied into the coordinator so
// that no caller retains a handle on a constructed coordinator's policy.
//
// Options record intent here and never validate or clamp. That division is what
// makes New the single validation point: a value that an option quietly
// corrected would be a misconfiguration nothing ever reports.
type config struct {
	preDrainDelay time.Duration
	idleTimeout   time.Duration
	deadlines     map[Reason]time.Duration
	onQuiesce     func(cause Cause)
	onDeadline    func(ctx context.Context, r Reason, outstanding int)
	signals       []os.Signal
	clock         Clock
}

// defaultConfig is the policy of a coordinator constructed with no options.
func defaultConfig() config {
	return config{
		preDrainDelay: defaultPreDrainDelay,
		idleTimeout:   defaultIdleTimeout,
		signals:       []os.Signal{syscall.SIGTERM, syscall.SIGINT},
		clock:         systemClock{},
	}
}

// Option configures a Coordinator. Options record intent; New validates.
//
// An option that accepts a reference type from the caller — a map, a slice —
// must copy it into the config rather than assign it, or the caller keeps a
// handle on the policy of every coordinator built from that option. New makes
// no defensive copy and relies on this. Every new constructor must be added to
// optionConstructors in options_test.go, which is where that reliance is
// checked; a completeness test fails if it is not.
type Option func(*config)

// WithPreDrainDelay sets the settle window. Default 5s, which suits a
// signal-caused drain behind kube-proxy. An idle- or explicit-caused drain
// behind a load balancer needs this raised to cover readiness-probe latency
// (periodSeconds × failureThreshold) as well as dataplane propagation.
func WithPreDrainDelay(d time.Duration) Option {
	return func(cfg *config) { cfg.preDrainDelay = d }
}

// WithIdleTimeout makes the coordinator Begin(CauseIdle) after d elapses with
// zero holds outstanding, timed from Wait entry. Default 0, disabled.
//
// Only enable this where a clean exit is the intended lifecycle: a Job or KEDA
// ScaledJob, an operator-owned per-session pod, or a systemd/Compose unit that
// is not restarted. Under a Deployment, ReplicaSet, or StatefulSet the kubelet
// restarts the container on any exit, including exit 0, and this option produces
// a crashloop. There, export occupancy for the autoscaler instead.
func WithIdleTimeout(d time.Duration) Option {
	return func(cfg *config) { cfg.idleTimeout = d }
}

// WithDeadline sets how long drain waits for holds of reason r, measured from
// Begin. Unconfigured reasons get the full grace period. A deadline of 0 means
// abandon immediately — correct for a hold that waits on a human, and a trap for
// an unset config field.
//
// An unconfigured reason's report is not actionable. Its deadline IS the grace
// period, so "still outstanding at its deadline" and "overran the ceiling" are
// the same instant: OnDeadline fires with a callback context already cancelled,
// on a goroutine nobody waits for, in the same instant Wait returns. Configure
// any reason whose abandonment you intend to act on. The full-grace default is
// the cautious one — it never abandons work early — but it buys caution, not a
// working interrupt hook.
//
// The last WithDeadline for a reason wins, so a composition root can layer a
// caller's options over its own defaults.
func WithDeadline(r Reason, d time.Duration) Option {
	return func(cfg *config) {
		if cfg.deadlines == nil {
			cfg.deadlines = make(map[Reason]time.Duration)
		}
		cfg.deadlines[r] = d
	}
}

// WithOnQuiesce registers a callback that observes the start of the sequence;
// use it to fail a readiness probe. It runs once, on a coordinator-owned
// goroutine, inside the grace ceiling.
func WithOnQuiesce(fn func(cause Cause)) Option {
	return func(cfg *config) { cfg.onQuiesce = fn }
}

// WithOnDeadline registers a callback invoked once per reason whose deadline
// expired with holds still outstanding, on coordinator-owned goroutines,
// possibly concurrently for different reasons. ctx expires at the grace ceiling.
//
// Read context.Cause(ctx), not ctx.Err(), to tell the two endings apart. The
// ceiling is driven by the injected Clock rather than by context.WithDeadline,
// so Err() is context.Canceled whichever way the sequence ended; the cause is
// context.DeadlineExceeded when the grace ceiling expired, and Wait's own ctx's
// cause when the drain was called off from outside. On that second path the ctx
// is already done at the invocation — the call is a notification that
// abandonment happened, not a budget to work within.
//
// It returns nothing: the callback cannot extend the wait, because the ceiling
// exists precisely because SIGKILL does not negotiate. A legitimate need for
// more time is already spelled WithDeadline(r, longer). outstanding is a
// snapshot taken under the lock at invocation and is advisory — releases race
// with it by design.
//
// Bounding the whole shutdown with context.WithTimeout collapses the ceiling
// and parent discrimination above, and it collapses it for the callback only:
// endingCause forwards context.DeadlineExceeded either way, so a callback can no
// longer tell "the grace ceiling expired" from "the caller's own timeout fired".
// Wait's return still distinguishes them — a ceiling overrun is
// *GracePeriodExceededError and a cancelled parent is ctx.Err() — so put the
// logic that needs to know which happened after Wait, not inside OnDeadline.
func WithOnDeadline(fn func(ctx context.Context, r Reason, outstanding int)) Option {
	return func(cfg *config) { cfg.onDeadline = fn }
}

// WithSignals replaces the signals Run installs. Default SIGTERM, SIGINT.
// SIGKILL and SIGSTOP cannot be caught; passing either is an InvalidPolicyError
// rather than a silent no-op.
//
// The set is replaced, not extended, and it is copied, so a caller that reuses
// its slice cannot alter a constructed coordinator's policy.
//
// WithSignals() with no arguments installs no handler at all, and Run is then
// exactly Wait. That is not the degenerate reading: an empty set is a set, and
// signal.Notify documents it as EVERY signal — so the literal reading would
// turn an empty WithSignals into a drain begun by SIGWINCH, by SIGCHLD, and by
// the profiler's SIGPROF. A caller who configured no signals wanted no signals.
func WithSignals(sigs ...os.Signal) Option {
	return func(cfg *config) { cfg.signals = slices.Clone(sigs) }
}

// WithClock injects a clock for deterministic tests. A nil clock is ignored,
// following the fail-safe option convention: the coordinator keeps the clock it
// already has rather than failing construction or storing a nil that would panic
// at the first reading of the time.
func WithClock(c Clock) Option {
	return func(cfg *config) {
		if c != nil {
			cfg.clock = c
		}
	}
}
