// Package leader implements all behavior specific to a leader instance.
package leader

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

const (
	HELLO = "Hello, world!"

	FailedToParse = "Failed to parse request body"
	PutSuccess    = "Added successfully"
)

type Response struct {
	// The status code is not marshalled to JSON. The wrapper function uses this
	// to write the HTTP response body. Defaults to 200.
	status int

	Message  string `json:"message,omitempty"`
	Exists   *bool  `json:"doexExist,omitempty"`
	Replaced *bool  `json:"replaced,omitempty"`
}

// Input stores arguments to each api request
type Input struct {
	Key   string
	Value string `json:"value"`
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

func (s *storage) putHandler(in Input, res *Response) {
	replaced := s.Set(in.Key, in.Value)

	res.Replaced = &replaced
	res.Message = PutSuccess
}

func withJSON(next func(Input, *Response)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in Input
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&in); err != nil {
			log.Println("Could not decode incoming request: ", err)
			http.Error(w, FailedToParse, http.StatusBadRequest)
			return
		}

		params := mux.Vars(r)
		in.Key = params["key"]

		result := &Response{
			// Default to OK status
			status: http.StatusOK,
		}
		next(in, result)

		w.WriteHeader(result.status)
		enc := json.NewEncoder(w)
		if err := enc.Encode(result); err != nil {
			log.Println("Failed to marshal JSON response: ", err)
			// response writer is likely fubar at this point.
			return
		}
	}
}

func Route(r *mux.Router) {
	s := newStorage()

	r.HandleFunc("/", s.indexHandler).Methods(http.MethodGet)
	r.HandleFunc("/kv-store/{key}", withJSON(s.putHandler)).Methods(http.MethodPut)
}
