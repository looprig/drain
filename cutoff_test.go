package drain

// Internal, and for a reason that no exported surface can supply: this file
// asserts that closing admission and snapshotting the outstanding holds happen
// in ONE critical section, and the only way to say that is to run the real cut
// against real concurrent Acquires and compare what it saw with what was
// granted. It needs no clock, so unlike sequence_test.go it can be internal
// without importing draintest into a cycle.

import (
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAdmissionCutoffAndSnapshotAreOneCriticalSection is the test that was
// previously believed impossible to write before the later tasks landed. It is
// not: the observable is a grant that escapes the cutover, and it becomes
// observable the moment the cutover snapshots — which is exactly what the spec
// requires it to do.
//
// The interleaving it hunts is the one the "mutex guards only the counters"
// reading admits. Split Acquire into two critical sections —
//
//	c.mu.Lock(); open := c.admissionOpen; c.mu.Unlock()
//	if !open { return noopRelease, false }
//	c.mu.Lock(); c.holds[r]++; c.total++; c.mu.Unlock()
//
// — and a hold can be granted against an admission flag that the cut closes
// before the increment lands. The snapshot then misses it, and the result is a
// hold that is never waited for and never reported in Abandoned: work dropped on
// the floor by a module whose entire job is not to drop it.
//
// Nothing here releases: a granted hold is meant to be outstanding at the end,
// so the grant count and the snapshot are two readings of the same number and
// any difference is the defect.
func TestAdmissionCutoffAndSnapshotAreOneCriticalSection(t *testing.T) {
	t.Parallel()

	const (
		trials    = 2000
		acquirers = 8
	)

	// More than one reason, so a snapshot that lost a whole counter fails as
	// loudly as one that lost a count.
	reasons := []Reason{"drain/one", "drain/two", "drain/three"}

	for trial := range trials {
		c, err := New(30 * time.Second)
		if err != nil {
			t.Fatalf("New() = %v, want a valid coordinator", err)
		}

		var (
			wg       sync.WaitGroup
			granted  [3]atomic.Int64
			snapshot map[Reason]int
		)

		// One start signal for every goroutine, so the cut lands in the middle
		// of the acquisitions rather than reliably before or after them.
		start := make(chan struct{})
		for i := range acquirers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, ok := c.Acquire(reasons[i%len(reasons)]); ok {
					granted[i%len(reasons)].Add(1)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			snapshot = c.cutAdmission().outstanding
		}()
		close(start)
		wg.Wait()

		// snapshot is written by one goroutine and read after it has joined.
		want := make(map[Reason]int, len(reasons))
		for i, reason := range reasons {
			if n := int(granted[i].Load()); n > 0 {
				want[reason] = n
			}
		}
		if !maps.Equal(snapshot, want) {
			t.Fatalf("trial %d: the cut snapshotted %v but Acquire granted %v: a granted hold escaped the cutover",
				trial, snapshot, want)
		}
	}
}
