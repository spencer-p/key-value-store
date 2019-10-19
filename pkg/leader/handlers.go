// Package leader implements all behavior specific to a leader instance.
package leader

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

const (
	HELLO = "Hello, world!"
)

// storage abstracts the volatile kv store for this instance
type storage struct {
	store map[string]string
	m     sync.Mutex
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

func (s *storage) indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(HELLO))
	s.Set("TODO", HELLO)
}

func Route(r *mux.Router) {
	s := newStorage()

	r.HandleFunc("/", s.indexHandler).Methods(http.MethodGet)
}