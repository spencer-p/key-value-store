// Package leader implements all behavior specific to a leader instance.
package leader

import (
	"net/http"

	"github.com/spencer-p/cse138/pkg/types"

	"github.com/gorilla/mux"
)

const (
	PutSuccess    = "Added successfully"
	UpdateSuccess = "Updated successfully"
	GetSuccess    = "Retrieved successfully"
	DeleteSuccess = "Deleted successfully"

	KeyDNE       = "Key does not exist"
	ValueMissing = "Value is missing"
	KeyMissing   = "Key is missing"
)

func (s *storage) deleteHandler(in types.Input, res *types.Response) {
	if in.Key == "" {
		res.Error = KeyMissing
		res.Status = http.StatusBadRequest
		return
	}

	_, ok := s.Read(in.Key)
	res.Exists = &ok

	s.Delete(in.Key)

	if !ok {
		res.Status = http.StatusNotFound
		res.Error = KeyDNE
		return
	}
	res.Message = DeleteSuccess
}

func (s *storage) getHandler(in types.Input, res *types.Response) {
	value, exists := s.Read(in.Key)

	res.Exists = &exists
	if exists {
		res.Message = GetSuccess
		res.Value = value
	} else {
		res.Error = KeyDNE
		res.Status = http.StatusNotFound
	}
}

func (s *storage) putHandler(in types.Input, res *types.Response) {
	if in.Value == "" {
		res.Error = ValueMissing
		res.Status = http.StatusBadRequest
		return
	}

	replaced := s.Set(in.Key, in.Value)

	res.Replaced = &replaced
	res.Message = PutSuccess
	if replaced {
		res.Message = UpdateSuccess
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
