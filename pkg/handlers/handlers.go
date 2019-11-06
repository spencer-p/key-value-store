// Package leader implements all behavior specific to a leader instance.
package handlers

import (
	"net/http"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/gorilla/mux"
	"stathat.com/c/consistent"
)

type State struct {
	store   *store.Store
	c       *consistent.Consistent
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

func New(addr string, view []string) *State {
	s := &State{
		store:   store.New(),
		c:       consistent.New(),
		address: addr,
	}

	s.c.Set(view)

	return s
}

func (s *State) Route(r *mux.Router) {
	r.HandleFunc("/kv-store/view-change", types.WrapHTTP(s.viewChange)).Methods(http.MethodPost)

	r.HandleFunc("/kv-store/{key:.*}", types.WrapHTTP(s.putHandler)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/{key:.*}", types.WrapHTTP(s.deleteHandler)).Methods(http.MethodDelete)
	r.HandleFunc("/kv-store/{key:.*}", types.WrapHTTP(s.getHandler)).Methods(http.MethodGet)
}
