// Package leader implements all behavior specific to a leader instance.
package handlers

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/spencer-p/cse138/pkg/hash"
	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/gorilla/mux"
)

type State struct {
	store   *store.Store
	hash    hash.Interface
	address string
}

func NewState(addr string, view []string) *State {
	s := State{
		store:   store.New(),
		hash:    hash.NewModulo(),
		address: addr,
	}

	log.Println("My address is: " + s.address)

	allViews := strings.Join(view, ",")

	log.Println("Adding these node address to members of hash " + allViews)

	s.hash.Set(view)

	return &s
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
	// TODO: try out other ways to parse URL?
	// parses the key from /kv-store/keys/{key}

	key := path.Base(r.URL.Path)
	log.Println("Key: " + key)

	nodeAddr, err := s.hash.Get(key)
	if err != nil {
		log.Fatalln(err)
	}

	if nodeAddr == s.address {
		return false
	}
	return true
}

func (s *State) Route(r *mux.Router) error {

	// TODO: figure out a way to get the forwarding address
	var address string
	if !strings.HasPrefix(address, "http://") {
		address = "http://" + "localhost:" + "8081"
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

	r.HandleFunc("/kv-store/keys/{key:.*}", f.forwardMessage).MatcherFunc(s.shouldForward)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(s.putHandler)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(s.deleteHandler)).Methods(http.MethodDelete)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(s.getHandler)).Methods(http.MethodGet)

	return nil
}

func InitNode(r *mux.Router, addr string, view []string) {
	s := NewState(addr, view)
	s.Route(r)
}
