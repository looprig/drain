package drain_test

import "github.com/looprig/drain"

// Compile-time proof that the default identifier for the root module is drain.
// An explicit import alias would hide a stale package declaration, so this import
// is intentionally unaliased.
var _ drain.Reason
