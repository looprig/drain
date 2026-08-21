package drain

// Internal, like cutoff_test.go and for the same shape of reason: the property
// asserted here has no exported consequence at all, so no external test can
// distinguish the correct implementation from the broken one.
//
// Outcome.Waited carries an entry for every reason the cut saw outstanding,
// including the ones nobody configured, at the full grace period. The obvious
// way to build that is to start from the coordinator's own deadline map and
// fill in the gaps — and the coordinator's deadline map is held by reference,
// with no defensive copy, because every Option builds its own. So the obvious
// implementation rewrites the policy of a live coordinator, adding a grace
// period for a reason the caller deliberately left unconfigured.
//
// From outside, that is invisible. The Outcome looks right, the drain has
// already finished, and nothing reads the config afterwards. From in here it is
// two assertions: the returned map is not the config, and the config still says
// what the caller configured.

import (
	"maps"
	"testing"
	"time"
)

// TestWaitedDoesNotAliasConfiguredDeadlines is a unit test of the one function
// that builds Outcome.Waited, rather than a drain driven end to end. That is
// deliberate: outcome_test.go already pins what a real sequence reports, and
// what is under test here is an aliasing bug whose whole signature is the
// absence of an allocation.
func TestWaitedDoesNotAliasConfiguredDeadlines(t *testing.T) {
	t.Parallel()

	const (
		grace               = 30 * time.Second
		deadline            = 10 * time.Second
		configured   Reason = "harness/active_turn"
		unconfigured Reason = "harness/pending_gate"
	)

	c, err := New(grace, WithDeadline(configured, deadline))
	if err != nil {
		t.Fatalf("New(%v) = _, %v, want a valid coordinator", grace, err)
	}
	policy := maps.Clone(c.cfg.deadlines)

	waited := c.waited(cutoff{outstanding: map[Reason]int{configured: 1, unconfigured: 2}})

	want := map[Reason]time.Duration{configured: deadline, unconfigured: grace}
	if !maps.Equal(waited, want) {
		t.Errorf("waited() = %v, want %v", waited, want)
	}

	// The entry for the unconfigured reason must not have been written into the
	// coordinator's policy on the way out.
	if !maps.Equal(c.cfg.deadlines, policy) {
		t.Errorf("the configured deadlines became %v, want %v: Waited is built into a fresh map, "+
			"never into the coordinator's own configuration", c.cfg.deadlines, policy)
	}

	// And the map belongs to the caller: the Outcome is documented as theirs to
	// keep, hand on, or edit.
	waited[unconfigured] = time.Hour
	delete(waited, configured)
	if !maps.Equal(c.cfg.deadlines, policy) {
		t.Errorf("the configured deadlines became %v after the caller edited its Outcome, want %v",
			c.cfg.deadlines, policy)
	}
}

// TestWaitedIsNilForAnEmptyCut pins the single-representation rule at the
// boundary the external suite reaches only through a whole clean drain: no
// reason outstanding at the cut is nil, not an allocated empty map.
func TestWaitedIsNilForAnEmptyCut(t *testing.T) {
	t.Parallel()

	const grace = 30 * time.Second

	c, err := New(grace)
	if err != nil {
		t.Fatalf("New(%v) = _, %v, want a valid coordinator", grace, err)
	}

	for _, empty := range []map[Reason]int{nil, {}} {
		if got := c.waited(cutoff{outstanding: empty}); got != nil {
			t.Errorf("waited(%v) = %v (len %d), want nil", empty, got, len(got))
		}
	}
}
