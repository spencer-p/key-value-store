// Package leader implements all behavior specific to a leader instance.
package leader

import (
    "fmt"
	"log"
	"net/http"
	"sync"
    "io/ioutil"
    "encoding/json"

	"github.com/gorilla/mux"
)

const (
	HELLO = "Hello, world!"
)

type response struct {
    Message     string      `json:"message,omitEmpty"`
}

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

func (s *storage) putHandler(w http.ResponseWriter, r *http.Request) {
    params := mux.Vars(r)
    body, _ := ioutil.ReadAll(r.Body)
    var value string
    json.Unmarshal(body, &value)
    s.Set(params["key"], value)
    for key, val := range s.store {
        fmt.Println("Key: ", key, "Value:", val)
    }
}

func Route(r *mux.Router) {
	s := newStorage()

	r.HandleFunc("/", s.indexHandler).Methods(http.MethodGet)
	r.HandleFunc("/kv-store/{key}", s.putHandler).Methods(http.MethodPut)
}
