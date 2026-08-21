// Package draintest provides deterministic test doubles for
// github.com/looprig/drain.
//
// Clock is a drain.Clock whose time moves only when Advance is called, so a
// test drives a five-second settle window or a ten-minute idle timeout in
// microseconds and with no scheduling flake. It is here rather than unexported
// inside drain because drain's consumers have the same problem: a composition
// root wiring a coordinator to its readiness probe and its work needs to test
// that wiring, and it cannot sleep for the grace period to do it.
//
// Clock.BlockUntil is how a test orders the code under test arming its timer
// against the test advancing the clock; skipping it is the standard way to write
// a shutdown test that hangs. For a test inside drain, testing/synctest is a
// good alternative — synctest.Wait blocks until every goroutine in the bubble is
// durably blocked, which orders the arm just as well and needs no cooperation
// from the clock. It is not what this package is built around because draintest
// exists for drain's consumers, and running a consumer's own shutdown wiring
// inside synctest.Test is a constraint on their code that this module has no
// standing to impose.
//
// Like drain itself, this package depends on the Go standard library and
// nothing else.
package draintest
