package store

// IterAction describes an action that should be taken for a given iteration.
// Iteraction can continue or stop, and the key/value can optionally be deleted.
type IterAction int

const (
	CONTINUE IterAction = 1 << iota
	DELETE
	STOP
)

// IterBody is a function that describes the body of iteration on a store.
type IterBody func(key, value string) IterAction

// For runs an IterBody on each key/value pair of the store. It acquires an
// exclusive write lock on the store. The body should not perform writes or
// deletes to the store explicitly. Deletes may be requested by return value
// from the body.
func (s *Store) For(body IterBody) {
	s.m.Lock()
	defer s.m.Unlock()

	for key, value := range s.Store {
		ret := body(key, value.Value)
		if ret&DELETE != 0 {
			delete(s.Store, key)
		}
		if ret&STOP != 0 {
			break
		}
	}
}
