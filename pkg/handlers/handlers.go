// Package leader implements all behavior specific to a leader instance.
package handlers

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/gorilla/mux"
	"stathat.com/c/consistent"
)

type State struct {
	store   *store.Store
	hash    *consistent.Consistent
	address string
}

func (s *State) deleteHandler(in types.Input, res *types.Response) {
	if in.Key == "" {
		res.Error = msg.KeyMissing
		res.Status = http.StatusBadRequest
		return
	}

	_, ok := s.store.Read(in.Key)
	res.Exists = &ok

	s.store.Delete(in.Key)

	if !ok {
		res.Status = http.StatusNotFound
		res.Error = msg.KeyDNE
		return
	}
	res.Message = msg.DeleteSuccess
}

func (s *State) getHandler(in types.Input, res *types.Response) {
	value, exists := s.store.Read(in.Key)

	res.Exists = &exists
	if exists {
		res.Message = msg.GetSuccess
		res.Value = value
	} else {
		res.Error = msg.KeyDNE
		res.Status = http.StatusNotFound
	}
}

func (s *State) putHandler(in types.Input, res *types.Response) {
	if in.Value == "" {
		res.Error = msg.ValueMissing
		res.Status = http.StatusBadRequest
		return
	}

	replaced := s.store.Set(in.Key, in.Value)

	res.Replaced = &replaced
	res.Message = msg.PutSuccess
	if replaced {
		res.Message = msg.UpdateSuccess
	} else {
		res.Status = http.StatusCreated
	}
}

func (s *State) shouldForward(r *http.Request, rm *mux.RouteMatch) bool {
	// TODO: get the key in this function
	key := mux.Vars(r)["Key"]
	log.Println("key " + key)

	hashedAddress, err := s.hash.Get(key)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("hashed address: " + hashedAddress)
	if hashedAddress != s.address {
		return true
	}
	return false
}

func Route(r *mux.Router, address string) error {
	s := State{
		store:   store.New(),
		hash:    consistent.New(),
		address: address,
	}

	// TODO Route needs to be passed the address and initial view
	// The view should be set in the consistent hash here.

	if !strings.HasPrefix(address, "http://") {
		address = "http://" + address
	}

	addr, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("Bad forwarding address %q: %v\n", address, addr)
	}

	// Only necessary if we need to forward
	f := forwarder{
		client: http.Client{
			Timeout: CLIENT_TIMEOUT,
		},
		addr: addr,
	}

	r.HandleFunc("/kv-store/{key:.*}", f.forwardMessage).MatcherFunc(s.shouldForward)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(s.putHandler)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(s.deleteHandler)).Methods(http.MethodDelete)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(s.getHandler)).Methods(http.MethodGet)

	return nil
}
