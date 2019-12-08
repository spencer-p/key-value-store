// Package leader implements all behavior specific to a leader instance.
package handlers

import (
	"context"
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
	hash    *hash.Hash
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

func (s *State) idHandler(in types.Input, res *types.Response) {
	err, KeyCount, CausalCtx := s.store.NumKeys(in.CausalCtx)
	if err != nil {
		log.Println("Error on id handler", err)
	}
	id := s.hash.GetShardId(s.address)
	res.ShardId = &id
	res.Message = msg.ShardInfoSuccess
	res.KeyCount = &KeyCount
	res.CausalCtx = CausalCtx
	res.Replicas = s.hash.GetReplicas(id)
}

func (s *State) shardsHandler(in types.Input, res *types.Response) {
	view := s.hash.GetView()
	maxShardId := len(view.Members) / view.ReplFactor
	shards := make([]int, maxShardId)
	for i := 1; i <= maxShardId; i++ {
		shards[i-1] = i
	}
	res.Shards = shards
	res.Message = msg.ShardMembSuccess
	res.CausalCtx = s.store.Clock()
}

func NewState(ctx context.Context, addr string, view types.View) *State {
	journal := make(chan store.Entry, 10)
	hash := hash.New(view)
	s := &State{
		store:   store.New(addr, hash.GetReplicas(hash.GetShardId(addr)), journal),
		hash:    hash,
		address: addr,
		cli: &http.Client{
			Timeout: CLIENT_TIMEOUT,
		},
	}

	log.Println("Starting gossip dispatcher")
	go s.dispatchGossip(ctx, journal)

	return s
}

func (s *State) Route(r *mux.Router) {
	r.HandleFunc("/kv-store/gossip", s.receiveGossip).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/gossip-increment", s.receiveIncrement)
	r.HandleFunc("/kv-store/key-count", types.WrapHTTP(s.countHandler)).Methods(http.MethodGet)

	r.HandleFunc("/kv-store/shards", types.WrapHTTP(s.shardsHandler)).Methods(http.MethodGet)
	r.HandleFunc("/kv-store/shards/{key:[0-9]+}", s.forwardMessage).MatcherFunc(s.shouldForwardId).Methods(http.MethodGet)
	r.HandleFunc("/kv-store/shards/{key:[0-9]+}", types.WrapHTTP(s.idHandler)).Methods(http.MethodGet)

	r.HandleFunc("/kv-store/keys/{key:.*}", s.forwardMessage).MatcherFunc(s.shouldForwardKey).Methods(http.MethodPut, http.MethodDelete)
	r.HandleFunc("/kv-store/keys/{key:.*}", s.forwardMessage).MatcherFunc(s.shouldForwardRead).Methods(http.MethodGet)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.putHandler))).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.deleteHandler))).Methods(http.MethodDelete)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.getHandler))).Methods(http.MethodGet)

	r.HandleFunc("/kv-store/view-change", types.WrapHTTP(s.viewChange)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/view-change/primary-collect", types.WrapHTTP(s.primaryCollect))
	r.HandleFunc("/kv-store/view-change/primary-replace", types.WrapHTTP(s.primaryReplace))
	r.HandleFunc("/kv-store/view-change/secondary-collect", types.WrapHTTP(s.secondaryCollect))
	r.HandleFunc("/kv-store/view-change/secondary-replace", types.WrapHTTP(s.secondaryReplace))
}
