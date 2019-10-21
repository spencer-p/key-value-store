// Package leader implements handlers for follower instances.
package follower

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

const (
	SystemOfTheDown = "Main instance is down"
)

// TODO adam, vineet ?
// follower holds all state that a follower needs to operate.

type Response struct {
	status int `json:"-"`

	Message string `json:"message,omitempty"`
}
type follower struct {
	addr string
}

func (f *follower) indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "My forwarding address is %s", f.addr)
}

func Route(r *mux.Router, fwd string) {
	f := follower{fwd}

	r.HandleFunc("/", f.indexHandler).Methods(http.MethodGet)
}
