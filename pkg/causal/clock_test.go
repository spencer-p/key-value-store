package causal

import (
	"testing"
)

func TestVectorClock(t *testing.T) {
	tests := []struct {
		v1, v2 VectorClock
		want   bool
	}{{
		v1:   []int64{1, 2, 3},
		v2:   []int64{0, 1, 2},
		want: false,
	}, {
		v1:   []int64{0, 1, 2},
		v2:   []int64{1, 2, 3},
		want: true,
	}, {
		v1:   []int64{0, 1, 2},
		v2:   []int64{2, 3},
		want: false,
	}, {
		v1:   []int64{0, 5, 2},
		v2:   []int64{1, 2, 3},
		want: false,
	}}

	for _, tc := range tests {
		if got := tc.v1.Less(tc.v2); got != tc.want {
			t.Errorf("%v < %v returned %t, wanted %t", tc.v1, tc.v2, got, tc.want)
		}
	}
}
