package clock

import (
	"testing"
)

func TestVectorClock(t *testing.T) {
	tests := []struct {
		v1, v2 VectorClock
		want   CompareResult
	}{{
		v1: map[string]uint64{
			"a": 1,
			"b": 2,
		},
		v2: map[string]uint64{
			"a": 2,
			"b": 3,
		},
		want: Less,
	}, {
		v1: map[string]uint64{
			"a": 5,
			"b": 2,
		},
		v2: map[string]uint64{
			"a": 2,
			"b": 3,
			"C": 4,
		},
		want: NoRelation,
	}, {
		v1: map[string]uint64{
			"a": 1,
		},
		v2: map[string]uint64{
			"a": 2,
			"b": 2,
		},
		want: Less,
	}, {
		v1: map[string]uint64{
			"a": 2,
			"b": 2,
		},
		v2: map[string]uint64{
			"a": 1,
		},
		want: Greater,
	}, {
		v1: map[string]uint64{
			"a": 2,
			"b": 2,
		},
		v2: map[string]uint64{
			"a": 2,
			"b": 2,
		},
		want: Equal,
	}, {
		v1: map[string]uint64{
			"a": 1,
			"b": 0,
		},
		v2: map[string]uint64{
			"a": 0,
			"b": 1,
		},
		want: NoRelation,
	}, {
		v1: map[string]uint64{
			"a": 1,
			"b": 0,
		},
		v2:   map[string]uint64{},
		want: Greater,
	}, {
		v1: map[string]uint64{},
		v2: map[string]uint64{
			"a": 1,
		},
		want: Less,
	}, {
		v1:   map[string]uint64{},
		v2:   map[string]uint64{},
		want: Equal,
	}}

	for _, tc := range tests {
		if got := tc.v1.Compare(tc.v2); got != tc.want {
			t.Errorf("%v compare %v returned %d, wanted %d", tc.v1, tc.v2, got, tc.want)
		}
	}
}
