package hash

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestEltsEqual(t *testing.T) {
	tests := []struct {
		e1, e2 []string
		want   bool
	}{{
		e1:   []string{"a", "b"},
		e2:   []string{"a", "c"},
		want: false,
	}, {
		e1:   []string{"a", "b"},
		e2:   []string{"a", "b"},
		want: true,
	}, {
		e1:   []string{"a", "b"},
		e2:   []string{"b", "a"},
		want: true,
	}, {
		e1:   []string{},
		e2:   []string{"a", "b"},
		want: false,
	}, {
		e1:   []string{"a", "b", "c"},
		e2:   []string{"a", "b"},
		want: false,
	}}

	for _, tc := range tests {
		got := eltsEqual(tc.e1, tc.e2)
		if got != tc.want {
			t.Errorf("For %v == %v, got %t, wanted %t", tc.e1, tc.e2, got, tc.want)
		}
	}
}

func TestShardMembers(t *testing.T) {
	m := NewModulo(3)
	m.Set([]string{"a", "b", "c", "d", "e", "f"})

	if diff := cmp.Diff(m.ShardMembers(0), []string{"a", "b", "c"}); diff != "" {
		t.Errorf("did not get correct shard (-got,+want): %s", diff)
	}
}
