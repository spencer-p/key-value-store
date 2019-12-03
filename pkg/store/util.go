package store

import (
	"github.com/spencer-p/cse138/pkg/clock"
)

// waitUntilCurrent returns a function that stalls until the waiting vector
// clock is not causally from the future.  the write mutex must be held on the
// store.
func (s *Store) waitUntilCurrent(waiting clock.VectorClock) bool {
	for {
		// As long as this clock is not from the future, we can apply it.
		if cmp := waiting.Compare(s.vc); cmp != clock.Greater {
			return true
		}
		s.vcCond.Wait()
	}
}
