// Package leader implements all behavior specific to a leader instance.
package handlers

import (
	"net/http"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/gorilla/mux"
)

func (s *storage) deleteHandler(in types.Input, res *types.Response) {
	if in.Key == "" {
		res.Error = msg.KeyMissing
		res.Status = http.StatusBadRequest
		return
	}

	_, ok := s.Read(in.Key)
	res.Exists = &ok

	s.Delete(in.Key)

	if !ok {
		res.Status = http.StatusNotFound
		res.Error = msg.KeyDNE
		return
	}
	res.Message = msg.DeleteSuccess
}

func (s *storage) getHandler(in types.Input, res *types.Response) {
	value, exists := s.Read(in.Key)

	res.Exists = &exists
	if exists {
		res.Message = msg.GetSuccess
		res.Value = value
	} else {
		res.Error = msg.KeyDNE
		res.Status = http.StatusNotFound
	}
}

func (s *storage) putHandler(in types.Input, res *types.Response) {
	if in.Value == "" {
		res.Error = msg.ValueMissing
		res.Status = http.StatusBadRequest
		return
	}

	replaced := s.Set(in.Key, in.Value)

	res.Replaced = &replaced
	res.Message = msg.PutSuccess
	if replaced {
		res.Message = msg.UpdateSuccess
	} else {
		res.Status = http.StatusCreated
	}
}

func Route(r *mux.Router) {
	s := newStorage()

	r.HandleFunc("/kv-store/{key:.*}", types.WrapHTTP(s.putHandler)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/{key:.*}", types.WrapHTTP(s.deleteHandler)).Methods(http.MethodDelete)
	r.HandleFunc("/kv-store/{key:.*}", types.WrapHTTP(s.getHandler)).Methods(http.MethodGet)
}
