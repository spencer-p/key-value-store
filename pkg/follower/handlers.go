// Package leader implements handlers for follower instances.
package follower

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

// TODO adam, vineet ?

// follower holds all state that a follower needs to operate.
type follower struct {
	client http.Client
	addr   *url.URL
}

func (f *follower) indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "My forwarding address is %s", f.addr)
	// TODO i think use http.NewRequest and f.client.Do
}

func Route(r *mux.Router, fwd string) {
	addr, err := url.Parse(fwd)
	if err != nil {
		// TODO return an error instead of fataling
		log.Fatalf("Bad forwarding address %q: %v\n", fwd, addr)
	}

	f := follower{
		client: http.Client{
			Timeout: TIMEOUT,
		},
		addr: addr,
	}

	r.PathPrefix("/").Handler(http.HandlerFunc(f.indexHandler))
}
