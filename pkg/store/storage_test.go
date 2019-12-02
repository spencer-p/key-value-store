package store

import (
	"context"
	"sync"
	"testing"

	"github.com/spencer-p/cse138/pkg/clock"
)

type WriteBatch []struct {
	id         string
	key, value string
	ctx        context.Context
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
			committed, _, _ := s.Write(b.ctx, b.vc, b.key, b.value)
			if b.wait {
				wg.Done()
			}
			results[b.id] = committed
		}()
	}
	wg.Wait()
	return results
}

func TestCausality(t *testing.T) {
	t.Run("writes apply causally", func(t *testing.T) {
		ctx := context.Background()
		s := New("a")

		dowrites(s, WriteBatch{{
			id:    "a",
			key:   "a",
			value: "100",
			ctx:   ctx,
			vc:    clock.VectorClock{"a": 100},
			wait:  false, // this write should not commit
		}, {
			id:    "w",
			key:   "w",
			value: "4",
			ctx:   ctx,
			vc:    clock.VectorClock{"b": 1},
			wait:  true, // this write should pass
		}, {
			id:    "y",
			key:   "y",
			value: "2",
			ctx:   ctx,
			vc:    clock.VectorClock{"a": 1},
			wait:  true,
		}, {
			id:    "x",
			key:   "x",
			value: "1",
			ctx:   ctx,
			vc:    clock.VectorClock{},
			wait:  true,
		}})

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

		val, ok = s.Read("w")
		if !ok {
			t.Errorf("unexpected read fail on w")
		}
		if val != "4" {
			t.Errorf("got value y=%q but wanted %q", val, "4")
		}

		val, ok = s.Read("a")
		if ok {
			t.Errorf("read of a returned a=%q, should not have committed", val)
		}
	})
}
