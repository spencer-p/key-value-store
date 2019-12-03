package store

import (
	"sync"
	"testing"

	"github.com/spencer-p/cse138/pkg/clock"
)

type WriteBatch []struct {
	id         string
	key, value string
	vc         clock.VectorClock
	wait       bool
}

func dowrites(s *Store, batch WriteBatch) map[string]bool {
	results := make(map[string]bool)
	var wg sync.WaitGroup
	for _, b := range batch {
		b := b
		if b.wait {
			wg.Add(1)
		}
		go func() {
			err, _, _ := s.Write(b.vc, true, b.key, b.value)
			results[b.id] = err != nil
			if b.wait {
				wg.Done()
			}
		}()
	}
	wg.Wait()
	return results
}

func shouldRead(t *testing.T, s *Store, tc clock.VectorClock, key, value string) (passed bool) {
	t.Helper()
	err, e, ok, _ := s.Read(tc, key)
	if err != nil {
		t.Errorf("Unexpected error reading %q: %v", key, err)
	} else if !ok {
		t.Errorf("Key %q not present, wanted %q", key, value)
	} else if e.Value != value {
		t.Errorf("Read %q=%q, wanted %q=%q", key, e.Value, key, value)
	} else {
		passed = true
	}
	return
}

func TestCausality(t *testing.T) {

	t.Run("writes apply causally", func(t *testing.T) {
		s := New("a")

		dowrites(s, WriteBatch{{
			id:    "a",
			key:   "a",
			value: "100",
			vc:    clock.VectorClock{"a": 100},
			wait:  false, // this write should not commit
		}, {
			id:    "y",
			key:   "y",
			value: "2",
			vc:    clock.VectorClock{"a": 1},
			wait:  true,
		}, {
			id:    "x",
			key:   "x",
			value: "1",
			vc:    clock.VectorClock{},
			wait:  true,
		}})

		err, e, ok, _ := s.Read(clock.VectorClock{}, "x")
		if err != nil || !ok {
			t.Errorf("unexpected read fail on x")
		}
		if e.Value != "1" {
			t.Errorf("got value x=%q but wanted %q", e.Value, "1")
		}

		err, e, ok, _ = s.Read(clock.VectorClock{}, "y")
		if err != nil || !ok {
			t.Errorf("unexpected read fail on y")
		}
		if e.Value != "2" {
			t.Errorf("got value y=%q but wanted %q", e.Value, "2")
		}

		err, e, ok, _ = s.Read(clock.VectorClock{}, "a")
		if ok {
			t.Errorf("read of a returned a=%q, should not have committed", e.Value)
		}
	})

	t.Run("writes can interleave", func(t *testing.T) {
		// have two "clients" write separate histories

		s := New("a")
		var wg sync.WaitGroup
		wg.Add(2)

		c1 := clock.VectorClock{}
		c2 := clock.VectorClock{}

		go func() {
			var err error
			if err, _, c1 = s.Write(c1, true, "x", "1"); err != nil {
				t.Errorf("Failed to write x: %v", err)
			}
			if err, _, c1 = s.Write(c1, true, "y", "2"); err != nil {
				t.Errorf("Failed to write y: %v", err)
			}
			wg.Done()
		}()

		go func() {
			var err error
			if err, _, c2 = s.Write(c2, true, "a", "1"); err != nil {
				t.Errorf("Failed to write a: %v", err)
			}
			if err, _, c2 = s.Write(c2, true, "b", "2"); err != nil {
				t.Errorf("Failed to write b: %v", err)
			}
			if err, _, c2 = s.Write(c2, true, "c", "3"); err != nil {
				t.Errorf("Failed to write c: %v", err)
			}
			wg.Done()
		}()

		wg.Wait()

		// if we read c, we must read b, and we must read a.
		shouldRead(t, s, c1, "c", "3")
		shouldRead(t, s, c1, "b", "2")
		shouldRead(t, s, c1, "a", "1")
		// if we read y, we must read x.
		shouldRead(t, s, c1, "y", "2")
		shouldRead(t, s, c1, "x", "1")
	})

	t.Run("reads block until applicable", func(t *testing.T) {
		done := make(chan struct{})
		s := New("a")
		var e Entry
		var ok bool
		go func() {
			_, e, ok, _ = s.Read(clock.VectorClock{"a": 3}, "x")
			done <- struct{}{}
		}()

		dowrites(s, WriteBatch{{
			id:    "x1",
			key:   "x",
			value: "stale",
			vc:    clock.VectorClock{},
		}, {
			id:    "y",
			key:   "y",
			value: "2",
			vc:    clock.VectorClock{"a": 1},
		}, {
			id:    "x2",
			key:   "x",
			value: "fresh",
			vc:    clock.VectorClock{"a": 2},
		}})

		<-done

		if !ok {
			t.Errorf("got no value for x")
		} else if e.Value != "fresh" {
			t.Errorf("got value %q, wanted \"fresh\"", e.Value)
		}

		// rogue thought. we can apply a write with a past vector clock
		// UNLESS the write is to a key with a > vector clock
	})

	t.Run("multireplicant write is handled", func(t *testing.T) {
		// client writes x to r1, then writes y to r2. Gossip is required before y is written.
		s := New("a")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			err, _, _ := s.Write(clock.VectorClock{"b": 1}, true, "y", "2")
			if err != nil {
				t.Errorf("Failed to write y=2: %v", err)
			}
			wg.Done()
		}()
		go func() {
			err, _, _ := s.Write(clock.VectorClock{"b": 1}, false, "x", "1")
			if err != nil {
				t.Errorf("Failed to gossip x=1 from b: %v", err)
			}
			wg.Done()
		}()
		wg.Wait()

		shouldRead(t, s, clock.VectorClock{}, "x", "1")
		shouldRead(t, s, clock.VectorClock{}, "y", "2")
	})

	t.Run("gossip can merge in", func(t *testing.T) {
	})

	t.Run("divergent gossip is ok", func(t *testing.T) {
	})

	t.Run("divergent gossip with shared history is ok", func(t *testing.T) {
	})

	t.Run("conflicting writes are resolved", func(t *testing.T) {
	})
}
