package drain

// Reason names a class of in-flight work. It is an open set: drain defines no
// domain reasons, because a generic module has no business naming one
// consumer's vocabulary. Consumers declare their own as typed constants.
//
// The policy for a reason nobody configured is the full grace period, so the
// cautious default is the one you get by doing nothing. Three consequences of
// the set being open are worth stating outright, because none of them is
// checked for you:
//
//   - There is no exhaustiveness check. Nothing tells you a reason went
//     unconfigured.
//   - Typos fail in the dangerous direction. WithDeadline("pending_gaet", 0)
//     silently leaves the real pending_gate at the full grace period — a drain
//     that waits hours on a human, which is the exact failure a zero deadline
//     exists to prevent. Outcome.Waited is the mitigation, within its limits:
//     it reports the deadline actually applied to each reason outstanding when
//     admission closed, so the real pending_gate shows up there at the full
//     grace period instead of the zero that was intended. The typo itself is
//     invisible — a reason nobody holds has no entry — so what telemetry
//     surfaces is the reason that WASN'T configured, not the string that was
//     misspelled.
//   - Two subsystems sharing one coordinator can silently share a counter.
//     Prefix reason strings with the owning package or module by convention
//     ("harness/pending_gate").
//
// Reasons are a low-cardinality static vocabulary. Never derive one from a
// per-session identifier ("session-<uuid>"). The counter map deletes a reason
// when its count reaches zero, so the cost is not an unbounded map; it is that
// every per-reason mechanism becomes per-session. WithDeadline can no longer
// name a reason, so every derived reason silently takes the full grace period;
// Outcome.Waited and Outcome.Abandoned become unbounded cardinality in whatever
// consumes them; and the deadline sweep arms one timer per live session.
type Reason string

// ReasonUnspecified is the zero Reason — a hold whose class the caller never
// named. It is an ordinary key like any other: unconfigured, it waits the full
// grace period.
const ReasonUnspecified Reason = ""
