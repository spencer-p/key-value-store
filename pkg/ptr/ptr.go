// Package ptr boxes literals.
package ptr

func Bool(b bool) *bool {
	return &b
}
