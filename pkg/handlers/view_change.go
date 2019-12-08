package handlers

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"
)

const (
	SECONDARYREPLACE_ENDPOINT = "/kv-store/view-change/secondary-replace"
	PRIMARY_COLLECT_ENDPOINT   = "/kv-store/primary-collect"
	SECONDARY_COLLECT_ENDPOINT = "/kv-store/secondary-collect"
	VIEWCHANGE_ENDPOINT        = "/kv-store/view-change"
	KEYCOUNT_ENDPOINT          = "/kv-store/key-count"
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

// TODO collect from secondaries
// return our func (s *State) primaryCollect(in types.Input, res *types.Response) types.Response {
func (s *State) primaryCollect(in types.Input, res *types.Response) {
	replicas := s.hash.GetReplicas(s.hash.GetShardId(s.address))
	clockCh := make(chan clock.VectorClock)

	for i := range replicas {
		go func(addr string) {
			// Don't make a request if it's just ourselves
			if addr == s.address {
				context := s.store.Clock()
				clockCh <- context
				return
			}

			var response types.Response
			resp, err := s.sendHttp(http.MethodGet,
				addr,
				SECONDARY_COLLECT_ENDPOINT,
				nil,
				&response)
			if err != nil {
				clockCh <- clock.VectorClock{}
				log.Printf("Failed to send http to %q: %v\n", addr, err)
				return
			}

			if resp.StatusCode != http.StatusOK {
				clockCh <- clock.VectorClock{}
				log.Printf("Replica at %q returned %d clock\n", addr, resp.StatusCode)
				return
			}

			clockCh <- response.CausalCtx
		}(replicas[i])
	}
	waiting := clock.VectorClock{}
	for _ = range replicas {
		c := <-clockCh
		waiting.Max(c)
	}

	log.Println("Waiting for clock", waiting)
	err := s.store.WaitUntilCurrent(waiting)
	if err != nil {
		log.Println("Wait until current error", err)
		res.Status = http.StatusServiceUnavailable
		return
	}

	res.StorageState = s.store.AllEntries()
	log.Println("Up to date, sending the state...", res.StorageState)
}

func (s *State) primaryReplace(in types.Input, res *types.Response) {
	s.hash.TestAndSet(in.View)
	s.store.ReplaceEntries(in.StorageState)
	s.store.SetReplicas(in.View.Members)
	var wg sync.WaitGroup

	shardId := s.hash.GetShardId(s.address)
	log.Printf("Sending replacement batch to all replicas in shard %d\n", shardId)
	replicas := s.hash.GetReplicas(shardId)
	for _, replicaAddr := range replicas {
		if replicaAddr == s.address {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var response types.Response
			resp, err := s.sendHttp(http.MethodPut,
				addr,
				SECONDARYREPLACE_ENDPOINT,
				in,
				&response)
			if err != nil {
				log.Printf("Failed to send http to %q: %v\n", addr, err)
				return
			}

			if resp.StatusCode != http.StatusOK {
				log.Printf("Replica at %q failed to replace storage: %d", addr, resp.StatusCode)
				return
			}
		}(replicaAddr)
	}

	wg.Wait()
	return
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
