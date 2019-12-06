package store

// IterAction describes an action that should be taken for a given iteration.
// Iteraction can continue or stop the loop.
type IterAction int

const (
	CONTINUE IterAction = 1 << iota
	STOP
)

// IterBody is a function that describes the body of iteration on a store.
type IterBody func(key string, e Entry) IterAction

// For runs an IterBody on each key/entry pair of the store. It acquires an
// exclusive write lock on the store. The body SHOULD NOT modify the store.
func (s *Store) For(body IterBody) {
	s.m.Lock()
	defer s.m.Unlock()

	for key, entry := range s.store {
		ret := body(key, entry)
		if ret&STOP != 0 {
			break
		}
	}
}
