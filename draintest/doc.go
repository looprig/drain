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
// Like drain itself, this package depends on the Go standard library and
// nothing else.
package draintest
