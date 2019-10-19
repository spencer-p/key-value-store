package leader

import (
	"log"
	"sync"
)

// storage abstracts the volatile kv store for this instance
type storage struct {
	store map[string]string
	m     sync.RWMutex
}

func newStorage() *storage {
	return &storage{
		store: make(map[string]string),
		// Note that the zero value for a mutex is unlocked.
	}
}

// Set sets key=value and returns true iff the value replaced an old value.
func (s *storage) Set(key, value string) bool {
	s.m.Lock()
	defer s.m.Unlock()

	old, updating := s.store[key]
	s.store[key] = value

	log.Printf("Set %q=%q", key, value)
	if updating {
		log.Printf("Old value was %q", old)
	}

	return updating
}

// Delete removes a key.
func (s *storage) Delete(key string) {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.store, key)

	log.Printf("Deleted %q\n", key)
}

// Read returns the value for a key in the storage.
func (s *storage) Read(key string) string {
	s.m.RLock()
	defer s.m.RUnlock()

	value := s.store[key]

	log.Printf("Reading %q=%q\n", key, value)

	return value
}
