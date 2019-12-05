package clock

type VectorClock map[string]uint64

// Compare determines the relationship between two vector clocks.
// a may be less than b, equal to b, greater than b, or there may be no comparison.
func (a VectorClock) Compare(b VectorClock) CompareResult {
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

// OneUp returns true if a is b plus one for only one key, which is returned as the second argument.
// The values for the key in self are completely ignored.
func (a VectorClock) OneUpExcept(self string, b VectorClock) (bool, string) {
	oneupkey := ""
	for k := range allKeys(a, b) {
		// Skip self
		if k == self {
			continue
		}

		// Add defaults for missing values
		if _, ok := a[k]; !ok {
			a[k] = 0
		}
		if _, ok := b[k]; !ok {
			b[k] = 0
		}

		// Test oneup property
		if a[k] == b[k]+1 {
			if oneupkey != "" {
				return false, ""
			}
			oneupkey = k
		}
	}

	return oneupkey != "", oneupkey
}

// Increment increases the value for a given key by one.
func (a VectorClock) Increment(k string) {
	cur, ok := a[k]
	if !ok {
		cur = 0
	}
	a[k] = cur + 1
}

// Max modifies a to be the pairwise max of a and b.
func (a VectorClock) Max(b VectorClock) {
	for k := range allKeys(a, b) {
		if a[k] < b[k] {
			a[k] = b[k]
		}
	}
}

// Copy returns a new identical vector clock.
func (a VectorClock) Copy() VectorClock {
	b := make(VectorClock)
	for k := range a {
		b[k] = a[k]
	}
	return b
}

// Subset returns a vector clock consisting of the subset of keys given.
func (a VectorClock) Subset(keys []string) VectorClock {
	s := VectorClock{}
	for _, key := range keys {
		v, ok := a[key]
		if !ok {
			s[key] = 0
		} else {
			s[key] = v
		}
	}
	return s
}

// allKeys zips together all the keys for two clocks.
// If any key is missing from one but not the other, it is
// defaulted to zero in the clock missing the key.
func allKeys(a, b VectorClock) map[string]struct{} {
	result := make(map[string]struct{})
	for k := range a {
		result[k] = struct{}{}
		if _, ok := b[k]; !ok {
			b[k] = 0
		}
	}
	for k := range b {
		result[k] = struct{}{}
		if _, ok := a[k]; !ok {
			a[k] = 0
		}
	}
	return result
}
