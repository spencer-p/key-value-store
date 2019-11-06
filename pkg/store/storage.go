package store

import (
	"fmt"
	"log"
	"sync"
)

// Store represents a volatile key value store.
type Store struct {
	store map[string]string
	m     sync.RWMutex
}

// New constructs an empty store.
func New() *Store {
	return &Store{
		store: make(map[string]string),
		// Note that the zero value for a mutex is unlocked.
	}
}

// Set sets key=value and returns true iff the value replaced an old value.
func (s *Store) Set(key, value string) bool {
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
func (s *Store) Delete(key string) {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.store, key)

	log.Printf("Deleted %q\n", key)
}

// Read returns the value for a key in the Store.
func (s *Store) Read(key string) (string, bool) {
	s.m.RLock()
	defer s.m.RUnlock()

	value, ok := s.store[key]

	log.Printf("Reading %q=%q\n", key, value)

	return value, ok
}

// NumKeys returns the number of keys in the store.
func (s *Store) NumKeys() int {
	s.m.RLock()
	defer s.m.RUnlock()

	return len(s.store)
}

func (s *Store) String() string {
	s.m.RLock()
	defer s.m.RUnlock()
	return fmt.Sprintf("%+v", s.store)
}
