// Package leader implements all behavior specific to a leader instance.
package handlers

import (
	"log"
	"net/http"

	//"github.com/spencer-p/cse138/pkg/gossip"
	"github.com/spencer-p/cse138/pkg/hash"
	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/gorilla/mux"
)

type State struct {
	Store   *store.Store
	repFact int
	id      string
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

	_, ok := s.Store.Read(in.Key)
	res.Exists = &ok

	s.Store.Delete(in.Key)

	if !ok {
		res.Status = http.StatusNotFound
		res.Error = msg.KeyDNE
		return
	}
	res.Message = msg.DeleteSuccess
}

func (s *State) getHandler(in types.Input, res *types.Response) {
	value, exists := s.Store.Read(in.Key)

	res.Exists = &exists
	if exists {
		res.Message = msg.GetSuccess
		res.Value = value
	} else {
		log.Println("does not exist")
		res.Error = msg.KeyDNE
		res.Status = http.StatusNotFound
	}
}

func (s *State) countHandler(in types.Input, res *types.Response) {
	KeyCount := s.Store.NumKeys()

	res.Message = msg.NumKeySuccess
	res.KeyCount = &KeyCount
}

func (s *State) putHandler(in types.Input, res *types.Response) {
	if in.Value == "" {
		res.Error = msg.ValueMissing
		res.Status = http.StatusBadRequest
		return
	}

	replaced := s.Store.Set(in.Key, in.Value, s.address)

	res.Replaced = &replaced
	res.Message = msg.PutSuccess
	if replaced {
		res.Message = msg.UpdateSuccess
	} else {
		res.Status = http.StatusCreated
	}
}

func (s *State) idHandler(in types.Input, res *types.Response) {
	KeyCount := s.Store.NumKeys()
	if s.id != in.Key {
		log.Println("Wrong shard!")
	}
	res.ShardId = s.id
	res.KeyCount = &KeyCount
	res.Replicas = s.Store.Replicas
}

func (s *State) shardsHandler(in types.Input, res *types.Response) {
	res.Shards = s.getShardInfo(s.hash.Members())
	res.Message = msg.ShardMembSuccess
}

func InitNode(r *mux.Router, addr string, repFact int, replicas []string, view []string, shardId string) *State {
	s := NewState(addr, replicas, view, repFact, shardId)
	s.Route(r, repFact)

	return s
}

func NewState(addr string, replicas []string, view []string, repFact int, id string) *State {
	s := &State{
		Store:   store.New(replicas),
		repFact: repFact,
		id:      id,
		hash:    hash.NewModulo(),
		address: addr,
		cli: &http.Client{
			Timeout: CLIENT_TIMEOUT,
		},
	}

	log.Println("Adding these node address to members of hash", view)
	s.hash.Set(view, repFact)

	return s
}

func (s *State) Route(r *mux.Router, repFact int) {

	r.HandleFunc("/kv-store/view-change", types.WrapHTTP(s.viewChange)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/key-count", types.WrapHTTP(s.countHandler)).Methods(http.MethodGet)

	r.HandleFunc("/kv-store/shards", types.WrapHTTP(s.shardsHandler)).Methods(http.MethodGet)
	r.HandleFunc("/kv-store/shards/{key:[0-9]+}", s.forwardMessage).MatcherFunc(s.shouldForwardId)
	r.HandleFunc("/kv-store/shards/{key:[0-9]+}", types.WrapHTTP(s.idHandler)).Methods(http.MethodGet)

	r.HandleFunc("/kv-store/keys/{key:.*}", s.forwardMessage).MatcherFunc(s.shouldForwardKey)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.putHandler))).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.deleteHandler))).Methods(http.MethodDelete)
	r.HandleFunc("/kv-store/keys/{key:.*}", types.WrapHTTP(types.ValidateKey(s.getHandler))).Methods(http.MethodGet)

}
