package drain

// Internal, like options_test.go and for the same kind of reason: what Run does
// is install process-wide signal handling, and the only way to assert that
// without either killing the test binary or racing it is to substitute the two
// signal-package functions Run calls. That seam is unexported, so these tests
// live inside the package.
//
// # Two rules for this file
//
// No test here runs in parallel. The seam is package state, so two tests
// swapping it at once would race; Go runs the package's sequential tests one at
// a time and releases the parallel ones afterwards, so declining t.Parallel is
// what keeps this file's tests from overlapping each other and from overlapping
// anything else while a real SIGTERM handler is installed.
//
// A second real signal is never sent. Run restores the default disposition
// after the first one — that is the escalation the operator relies on when a
// drain is wedged — and the default disposition for SIGTERM is death, so a test
// that verified the restore by sending another one would verify it by killing
// the test binary. It is verified through the seam instead.
//
// Nothing here uses real time. The coordinators are built on stubClock with a
// zero settle window, so a drain with no holds completes the instant it is
// entered and a drain with one hold outstanding stays open until the test
// releases it. The only real durations in the file are watchdogs, which elapse
// only when a test is already failing.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// runGrace is the ceiling these tests give the sequence. It is never reached:
// the stub clock's timers do not fire.
const runGrace = 30 * time.Second

// heldWork is the reason the tests that need a drain to stay open acquire.
const heldWork Reason = "harness/active_turn"

// watchdog bounds every receive in this file. It is not a timing dependency:
// nothing here waits for it to elapse, and it elapses only when the thing being
// waited for never happened, which is a failure. It exists because the
// alternative failure — a blocked receive — is the test binary's ten-minute
// timeout and a goroutine dump that has to be read to learn what was missing.
const watchdog = 10 * time.Second

// signalSeam substitutes the signal-package functions Run uses and records what
// Run asked for. Every channel is buffered past any test's expectations, so an
// unexpected extra call is a failed assertion rather than a wedged Run.
//
// passthrough decides whether the real signal package is still called
// underneath. Exactly one test wants that — the one that sends a real SIGTERM,
// which needs the handler genuinely installed and, just as much, needs to know
// WHEN it was, since a signal sent before signal.Notify returns is delivered to
// the default disposition and kills the test binary. Every other test wants no
// process-wide effect at all.
type signalSeam struct {
	sets      chan []os.Signal
	installed chan chan<- os.Signal
	stopped   chan chan<- os.Signal
}

// passthrough and isolated name signalSeam's two modes at the call site, so a
// bare true or false cannot quietly decide whether a test installs a real
// handler.
const (
	passthrough = true
	isolated    = false
)

func newSignalSeam(t *testing.T, real bool) *signalSeam {
	t.Helper()

	seam := &signalSeam{
		sets:      make(chan []os.Signal, 4),
		installed: make(chan chan<- os.Signal, 4),
		stopped:   make(chan chan<- os.Signal, 4),
	}

	notify, stop := notifySignals, stopSignals
	notifySignals = func(ch chan<- os.Signal, sigs ...os.Signal) {
		if real {
			notify(ch, sigs...)
		}
		seam.sets <- sigs
		seam.installed <- ch
	}
	stopSignals = func(ch chan<- os.Signal) {
		if real {
			stop(ch)
		}
		seam.stopped <- ch
	}
	t.Cleanup(func() { notifySignals, stopSignals = notify, stop })

	return seam
}

// installed reports the channel Run registered, and the set it registered it
// for. Both are read together because they are one call.
func (s *signalSeam) install(t *testing.T) ([]os.Signal, chan<- os.Signal) {
	t.Helper()
	return receive(t, s.sets, "signal.Notify's signal set"), receive(t, s.installed, "signal.Notify's channel")
}

// stop reports the channel Run unregistered.
func (s *signalSeam) stop(t *testing.T) chan<- os.Signal {
	t.Helper()
	return receive(t, s.stopped, "signal.Stop's channel")
}

// quiet asserts that Run — or Wait — installed nothing at all. It is sound only
// once the call under test has returned, which every caller has already waited
// for.
func (s *signalSeam) quiet(t *testing.T, why string) {
	t.Helper()

	select {
	case ch := <-s.installed:
		t.Errorf("signal.Notify was called with %v, want no signal handling at all: %s", ch, why)
	default:
	}
	select {
	case ch := <-s.stopped:
		t.Errorf("signal.Stop was called with %v, want no signal handling at all: %s", ch, why)
	default:
	}
}

// runResult is one return from Run or Wait, carried off the goroutine that made
// it.
type runResult struct {
	out Outcome
	err error
}

// runAsync starts Run on its own goroutine. It is only used where the drain has
// to still be running while the test asserts something about it.
func runAsync(c *Coordinator, ctx context.Context) <-chan runResult {
	done := make(chan runResult, 1)
	go func() {
		out, err := c.Run(ctx)
		done <- runResult{out: out, err: err}
	}()
	return done
}

// receive blocks for one value, failing the test rather than the binary if it
// never arrives. It must be called from the test's own goroutine.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(watchdog):
		t.Fatalf("timed out after %v waiting for %s", watchdog, what)
		var zero T
		return zero
	}
}

// newSignalCoord builds a coordinator on the stub clock with a zero settle
// window, so its sequence turns entirely on what the test does and not at all
// on time. The fixed options are appended last, where last-option-wins keeps a
// case from accidentally reintroducing a five-second window.
func newSignalCoord(t *testing.T, opts ...Option) *Coordinator {
	t.Helper()

	all := append(slices.Clone(opts), WithPreDrainDelay(0), WithClock(stubClock{name: "run"}))
	c, err := New(runGrace, all...)
	if err != nil {
		t.Fatalf("New(%v) = _, %v, want a valid coordinator", runGrace, err)
	}
	return c
}

// TestRunBeginsOnSignal is the one test in the file that sends a real signal.
// Everything else about Run can be asserted through the seam, but "a SIGTERM
// delivered to this process starts the drain" cannot: it is the whole point of
// the method, and a fake delivery would pass against a Run that registered for
// the wrong signal, or for none.
//
// The seam is in passthrough mode, so the handler really is installed, and the
// signal is sent only after signal.Notify has returned. That ordering is not a
// nicety: a SIGTERM that arrives a moment early gets the default disposition
// and kills the test binary.
func TestRunBeginsOnSignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), watchdog)
	defer cancel()

	seam := newSignalSeam(t, passthrough)
	c := newSignalCoord(t)

	done := runAsync(c, ctx)
	if _, ch := seam.install(t); ch == nil {
		t.Fatal("signal.Notify was called with a nil channel")
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("sending SIGTERM to this process: %v", err)
	}

	res := receive(t, done, "Run to return")
	if res.err != nil {
		t.Errorf("Run() = _, %v, want no error", res.err)
	}
	// CauseContext here would mean the ctx watchdog ended the drain rather than
	// the signal, which is the failure this assertion is really about.
	if res.out.Cause != CauseSignal {
		t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, CauseSignal)
	}
}

// TestRunRestoresDefaultDispositionAfterFirstSignal covers the operator's last
// resort. A drain can wedge — a hold nobody releases, a callback that never
// returns — and the answer to a wedged drain is a second Ctrl-C that actually
// kills the process, which only works if Run has put SIGTERM and SIGINT back on
// their default disposition by then.
//
// "By then" is the assertion, and it is what the outstanding hold is for. Wait
// cannot return while the hold is outstanding, and the test releases it only
// after observing the restore, so the restore provably happened DURING the
// drain. A Run that merely unregistered on its way out — which is where a
// deferred signal.Stop alone would put it — leaves that receive waiting and
// fails here.
func TestRunRestoresDefaultDispositionAfterFirstSignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), watchdog)
	defer cancel()

	seam := newSignalSeam(t, isolated)
	c := newSignalCoord(t)

	release, ok := c.Acquire(heldWork)
	if !ok {
		t.Fatal("Acquire refused before the drain began, want granted")
	}

	done := runAsync(c, ctx)
	_, ch := seam.install(t)

	// Delivery, exactly as the signal package would do it: the channel is
	// buffered, so this cannot block.
	ch <- syscall.SIGTERM

	if got := seam.stop(t); got != ch {
		t.Errorf("signal.Stop(%v), want the channel signal.Notify was given, %v", got, ch)
	}

	release()
	res := receive(t, done, "Run to return")
	if res.err != nil {
		t.Errorf("Run() = _, %v, want no error", res.err)
	}
	if res.out.Cause != CauseSignal {
		t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, CauseSignal)
	}
	seam.quiet(t, "the disposition was already restored at the first signal; Run does not stop twice")
}

// TestRunUsesConfiguredSignals pins the set Run registers for, including the
// empty case, which is the dangerous one: signal.Notify with no signals relays
// EVERY signal to the channel, so a Run that passed an empty set straight
// through would turn WithSignals() into "drain on SIGWINCH", and on SIGCHLD,
// and on the profiler's SIGPROF.
func TestRunUsesConfiguredSignals(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want []os.Signal
	}{
		{
			name: "the default set",
			want: []os.Signal{syscall.SIGTERM, syscall.SIGINT},
		},
		{
			name: "WithSignals replaces the set",
			opts: []Option{WithSignals(syscall.SIGUSR1, syscall.SIGHUP)},
			want: []os.Signal{syscall.SIGUSR1, syscall.SIGHUP},
		},
		{
			name: "WithSignals() installs nothing",
			opts: []Option{WithSignals()},
		},
	}

	for _, tc := range tests {
		// No t.Parallel: the seam these subtests swap is package state.
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), watchdog)
			defer cancel()

			seam := newSignalSeam(t, isolated)
			c := newSignalCoord(t, tc.opts...)

			if len(tc.want) == 0 {
				c.Begin(CauseExplicit)
				if _, err := c.Run(ctx); err != nil {
					t.Fatalf("Run() = _, %v, want no error", err)
				}
				seam.quiet(t, "an empty signal set means no handler, never signal.Notify's every-signal default")
				return
			}

			done := runAsync(c, ctx)
			got, ch := seam.install(t)
			if !slices.Equal(got, tc.want) {
				t.Errorf("signal.Notify registered %v, want %v", got, tc.want)
			}

			ch <- tc.want[0]
			res := receive(t, done, "Run to return")
			if res.out.Cause != CauseSignal {
				t.Errorf("Outcome.Cause = %v, want %v", res.out.Cause, CauseSignal)
			}
		})
	}
}

// TestRunIsWaitPlusSignals states the relationship the two methods have, by
// driving two identically configured coordinators through the same sequence —
// one with Wait, one with Run and no signal — and requiring the same Outcome
// and the same error. Run adds signal handling and subtracts nothing.
//
// It also pins the bookkeeping around that: the handler Run installs is removed
// again when Run returns, so a process that goes on to do something else after
// a drain is not left with a coordinator's handler attached to it.
func TestRunIsWaitPlusSignals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), watchdog)
	defer cancel()

	seam := newSignalSeam(t, isolated)

	waited := newSignalCoord(t)
	waited.Begin(CauseExplicit)
	waitOut, waitErr := waited.Wait(ctx)

	ran := newSignalCoord(t)
	ran.Begin(CauseExplicit)
	runOut, runErr := ran.Run(ctx)

	if runErr != waitErr {
		t.Errorf("Run() = _, %v but Wait() = _, %v; Run is Wait plus signals", runErr, waitErr)
	}
	if !reflect.DeepEqual(runOut, waitOut) {
		t.Errorf("Run() = %+v, Wait() = %+v; the two must produce the same Outcome", runOut, waitOut)
	}

	_, ch := seam.install(t)
	if got := seam.stop(t); got != ch {
		t.Errorf("signal.Stop(%v), want the channel signal.Notify was given, %v: "+
			"Run leaves no handler behind", got, ch)
	}
}

// TestWaitInstallsNoSignalHandling is the regression test for the finding this
// module's API review put first. signal.Notify is additive and process-wide: a
// library that quietly registers for SIGINT changes the behaviour of every
// program that links it, and one of this workspace's consumers is a TUI that
// intercepts Ctrl-C for its own UX. Wait is the method such an embedder calls,
// precisely so that it can keep its own handler and call Begin itself.
//
// Two assertions, because they fail to different mistakes. The first catches a
// Wait that installs handling through the same seam Run uses; the second
// catches one that reaches for os/signal directly, which the seam cannot see.
func TestWaitInstallsNoSignalHandling(t *testing.T) {
	t.Run("Wait calls neither signal.Notify nor signal.Stop", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), watchdog)
		defer cancel()

		seam := newSignalSeam(t, isolated)
		c := newSignalCoord(t)

		c.Begin(CauseExplicit)
		if _, err := c.Wait(ctx); err != nil {
			t.Fatalf("Wait() = _, %v, want no error", err)
		}
		seam.quiet(t, "Wait installs no signal handling; an embedder keeps its own")
	})

	// The structural half. It is a whitelist rather than a scan for a
	// particular call, so a future Wait that used signal.Ignore, or
	// signal.NotifyContext, or anything else the package grows, fails here too.
	t.Run("only run.go imports os/signal", func(t *testing.T) {
		want := []string{"run.go"}
		if got := filesImporting(t, "os/signal"); !slices.Equal(got, want) {
			t.Errorf("os/signal is imported by %v, want only %v: "+
				"signal handling is Run's, and installing it anywhere else changes an embedder's process", got, want)
		}
	})
}

// filesImporting reports this package's non-test files that import path,
// sorted. It parses the source rather than inspecting the binary because the
// question is where a package may be reached from, not whether a particular
// call happened to run.
func filesImporting(t *testing.T, path string) []string {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}

	var names []string
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, spec := range file.Imports {
			if imported(spec) == path {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	return names
}

// imported reports the import path an ImportSpec names, unquoted.
func imported(spec *ast.ImportSpec) string {
	unquoted, err := strconv.Unquote(spec.Path.Value)
	if err != nil {
		return ""
	}
	return unquoted
}
