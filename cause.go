package drain

import "strconv"

// Cause records why the sequence started. Unlike Reason it is a closed set:
// drain itself is the only thing that can start a drain, so it knows every way
// that can happen.
type Cause uint8

const (
	CauseUnknown  Cause = iota // 0 — the DEFAULT: the sequence has not started
	CauseSignal                // a signal installed by Run was received
	CauseIdle                  // no holds outstanding for IdleTimeout
	CauseExplicit              // Begin was called directly
	CauseContext               // Wait's ctx was cancelled before any other cause
)

// String renders the cause for logs. Unknown values render as Cause(n).
func (c Cause) String() string {
	switch c {
	case CauseUnknown:
		return "unknown"
	case CauseSignal:
		return "signal"
	case CauseIdle:
		return "idle"
	case CauseExplicit:
		return "explicit"
	case CauseContext:
		return "context"
	default:
		return "Cause(" + strconv.Itoa(int(c)) + ")"
	}
}
