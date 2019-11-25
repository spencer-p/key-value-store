package causal

type VectorClock map[string]uint64

func (a VectorClock) Compare(c Clock) CompareResult {
	b, ok := c.(VectorClock)
	if !ok {
		// Wrong types means no relation
		return NoRelation
	}

	// Determine if the scalars are all pairwise equal, less than or equal, OR
	// greater than or equal in one pass.
	equal := true
	lessEqual := true
	greaterEqual := true
	for k := range allKeys(a, b) {
		// Set missing keys to zero to compare properly
		if _, ok := a[k]; !ok {
			a[k] = 0
		}
		if _, ok := b[k]; !ok {
			b[k] = 0
		}

		if a[k] != b[k] {
			equal = false
		}
		if !(a[k] <= b[k]) {
			lessEqual = false
		}
		if !(a[k] >= b[k]) {
			greaterEqual = false
		}
	}

	if lessEqual && !equal {
		// The clock a < b if they are pairwise <= and the entire clock is not
		// equal.
		return Less
	} else if greaterEqual && !equal {
		// Opposite of above
		return Greater
	} else if equal {
		// Simple case!
		return Equal
	}
	return NoRelation
}

func (a VectorClock) Increment(k string) {
	a[k] += 1
}

func (a VectorClock) Copy() Clock {
	b := make(VectorClock)
	for k := range a {
		b[k] = a[k]
	}
	return b
}

func allKeys(a, b VectorClock) map[string]struct{} {
	result := make(map[string]struct{})
	for k := range a {
		result[k] = struct{}{}
	}
	for k := range b {
		result[k] = struct{}{}
	}
	return result
}
