package handlers

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"path"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"
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
func (s *State) sendHttp(method, address, endpoint string, input, response interface{}) (*http.Response, error) {
	// Encode the input body
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(input); err != nil {
		return nil, err
	}

	// Build the target URL
	target, err := url.Parse(util.CorrectURL(address))
	if err != nil {
		log.Printf("Bad forwarding address %q: %v\n", address, err)
		return nil, err
	}
	target.Path = path.Join(target.Path, endpoint)

	// Build request
	request, err := http.NewRequest(method, target.String(), &body)
	if err != nil {
		log.Printf("Failed to build request to %q: %v\n", address, err)
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := s.cli.Do(request)
	if err != nil {
		log.Printf("Failed to send request to %q: %v\n", address, err)
		return nil, err
	}

	// Parse the response
	if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
		log.Printf("Failed to parse response from %q: %v\n", address, err)
		return nil, err
	}

	return resp, nil
}
