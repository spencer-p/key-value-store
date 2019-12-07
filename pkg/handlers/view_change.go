package handlers

import (
	"log"
	"net/http"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"
)

const (
	VIEWCHANGE_ENDPOINT = "/kv-store/view-change"
	KEYCOUNT_ENDPOINT   = "/kv-store/key-count"
)

func (s *State) viewChange(in types.Input, res *types.Response) {
	if len(in.View.Members) == 0 || in.View.ReplFactor == 0 {
		res.Status = http.StatusBadRequest
		res.Error = msg.FailedToParse
		return
	}

	log.Printf("Received view change %#v\n", in.View)

	// TODO coordinator view change
}

func (s *State) primaryCollect(in types.Input, res *types.Response) {
	// TODO collect from secondaries
	// return our state
}

func (s *State) primaryReplace(in types.Input, res *types.Response) {
	s.store.ReplaceEntries(in.StorageState)
	// TODO dispatch the same input to all of the secondaries
}

func (s *State) secondaryCollect(in types.Input, res *types.Response) {
	s.hash.TestAndSet(in.View)
	res.CausalCtx = s.store.Clock()
}

func (s *State) secondaryReplace(in types.Input, res *types.Response) {
	s.store.ReplaceEntries(in.StorageState)
}

// sendHttp builds a request and issues it with a JSON body matching input.
// The response is unmarshalled into response and the http response is returned (or an error).
func (s *State) sendHttp(method, address string, body, response interface{}) (*http.Response, error) {
	// TODO write this method
	return nil, nil
}
