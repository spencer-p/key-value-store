// Package leader implements all behavior specific to a leader instance.
package handlers

import (
	"log"
	"net/http"

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
	cli     *http.Client
}

func (s *State) deleteHandler(in types.Input, res *types.Response) {
	if in.Key == "" {
		res.Error = msg.KeyMissing
		res.Status = http.StatusBadRequest
		return
	}

	err, ok, vc := s.store.Delete(in.CausalCtx, in.Key)
	if err != nil {
		res.Status = http.StatusServiceUnavailable
		res.Error = msg.Unavailable
		return
	}

	res.Exists = &ok
	res.CausalCtx = vc

	if !ok {
		res.Status = http.StatusNotFound
		res.Error = msg.KeyDNE
		return
	}
	res.Message = msg.DeleteSuccess
}

func (s *State) getHandler(in types.Input, res *types.Response) {
	err, e, ok, vc := s.store.Read(in.CausalCtx, in.Key)
	if err != nil {
		res.Status = http.StatusServiceUnavailable
		res.Error = msg.Unavailable
		return
	}
	res.CausalCtx = vc
	res.Exists = &ok
	if ok {
		res.Message = msg.GetSuccess
		res.Value = e.Value
	} else {
		res.Error = msg.KeyDNE
		res.Status = http.StatusNotFound
	}
}

func (s *State) countHandler(in types.Input, res *types.Response) {
	err, count, vc := s.store.NumKeys(in.CausalCtx)
	if err != nil {
		res.Status = http.StatusServiceUnavailable
		res.Error = msg.Unavailable
		return
	}

	res.Message = msg.NumKeySuccess
	res.KeyCount = &count
	res.CausalCtx = vc
}

func (s *State) putHandler(in types.Input, res *types.Response) {
	if in.Value == "" {
		res.Error = msg.ValueMissing
		res.Status = http.StatusBadRequest
		return
	}

	err, replaced, vc := s.store.Write(in.CausalCtx, in.Key, in.Value)
	if err != nil {
		res.Status = http.StatusServiceUnavailable
		res.Error = msg.Unavailable
		return
	}

	res.CausalCtx = vc
	res.Replaced = &replaced
	res.Message = msg.PutSuccess
	if replaced {
		res.Message = msg.UpdateSuccess
	} else {
		res.Status = http.StatusCreated
	}
}

func InitNode(r *mux.Router, addr string, view []string, journal chan<- store.Entry) {
	s := NewState(addr, view, journal)
	s.Route(r)
}

func NewState(addr string, view []string, journal chan<- store.Entry) *State {
	s := &State{
		store:   store.New(addr, []string{addr}, journal),
		hash:    hash.NewModulo(1 /*TODO replicacation factor*/),
		address: addr,
		cli: &http.Client{
			Timeout: CLIENT_TIMEOUT,
		},
	}

	log.Println("Adding these node address to members of hash", view)
	s.hash.Set(view)
	s.store.SetShard(s.hash.ShardMembers(s.hash.ShardOf(addr)))

	return s
}

func (s *State) Route(r *mux.Router) {
	r.HandleFunc("/kv-store/view-change", types.WrapHTTP(s.viewChange)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/key-count", types.WrapHTTP(s.countHandler)).Methods(http.MethodGet)

	r.HandleFunc("/kv-store/keys/{key:.*}", s.forwardMessage).MatcherFunc(s.shouldForward)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.putHandler))).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.deleteHandler))).Methods(http.MethodDelete)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.getHandler))).Methods(http.MethodGet)
}
