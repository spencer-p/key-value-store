package causal

type Clock interface {
	Less(Clock) bool
}

type VectorClock []int64

func (v1 VectorClock) Less(c Clock) bool {
	v2, ok := c.(VectorClock)
	if !ok {
		return false
	}

	if len(v1) != len(v2) {
		return false
	}

	eq := true
	for i := range v1 {
		if v1[i] != v2[i] {
			eq = false
		}
		if v1[i] > v2[i] {
			// An item in v1 dominating v2 means the relation does not hold
			return false
		}
	}

	return !eq
}
