package handlers

import (
	"testing"

	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
)

func TestDeleteBatches(t *testing.T) {
	s := &State{
		store: store.New(),
	}

	s.store.Set("x", "1")
	s.store.Set("y", "2")

	s.deleteEntries([]types.Entry{{
		Key: "x",
	}})

	v, ok := s.store.Read("x")
	if ok {
		t.Errorf("Got x=%q, wanted it to be missing", v)
	}

	v, ok = s.store.Read("y")
	if v != "2" || !ok {
		t.Errorf("Expected read y to return \"2\", ok; got %q and %t", v, ok)
	}
}

func TestViewEqual(t *testing.T) {
	tests := []struct {
		v1, v2 []string
		want   bool
	}{{
		v1:   []string{"a", "b"},
		v2:   []string{"a", "c"},
		want: false,
	}, {
		v1:   []string{"a", "b"},
		v2:   []string{"a", "b"},
		want: true,
	}, {
		v1:   []string{},
		v2:   []string{"a", "b"},
		want: false,
	}, {
		v1:   []string{"a", "b", "c"},
		v2:   []string{"a", "b"},
		want: false,
	}}

	for _, tc := range tests {
		got := viewEqual(tc.v1, tc.v2)
		if got != tc.want {
			t.Errorf("For %v == %v, got %t, wanted %t", tc.v1, tc.v2, got, tc.want)
		}
	}
}
