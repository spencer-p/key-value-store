package store

import (
	"context"
	"sync"
	"testing"

	"github.com/spencer-p/cse138/pkg/clock"
)

func dowait(wg *sync.WaitGroup, f func()) {
	go func() {
		f()
		wg.Done()
	}()
}

func TestCausality(t *testing.T) {
	t.Run("writes apply causally", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx := context.Background()
		s := New("a")

		// Do not wait for this; should never apply
		go s.Write(ctx, clock.VectorClock{"a": 100}, "z", "3")

		// These two writes should commit, but not in the order issued
		wg.Add(3)
		dowait(&wg, func() { s.Write(ctx, clock.VectorClock{"b": 1}, "w", "4") })
		dowait(&wg, func() { s.Write(ctx, clock.VectorClock{"a": 1}, "y", "2") })
		dowait(&wg, func() { s.Write(ctx, clock.VectorClock{}, "x", "1") })
		wg.Wait()

		val, ok := s.Read("x")
		if !ok {
			t.Errorf("unexpected read fail on x")
		}
		if val != "1" {
			t.Errorf("got value x=%q but wanted %q", val, "1")
		}

		val, ok = s.Read("y")
		if !ok {
			t.Errorf("unexpected read fail on y")
		}
		if val != "2" {
			t.Errorf("got value y=%q but wanted %q", val, "2")
		}
	})
}
