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
	"github.com/spencer-p/cse138/pkg/hash"
	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"
)

const (
	PRIMARY_COLLECT_ENDPOINT   = "/kv-store/view-change/primary-collect"
	SECONDARY_COLLECT_ENDPOINT = "/kv-store/view-change/secondary-collect"
	PRIMARY_REPLACE_ENDPOINT   = "/kv-store/view-change/primary-replace"
	SECONDARY_REPLACE_ENDPOINT = "/kv-store/view-change/secondary-replace"
	VIEWCHANGE_ENDPOINT        = "/kv-store/view-change"
	KEYCOUNT_ENDPOINT          = "/kv-store/key-count"
)

func (s *State) viewChange(in types.Input, res *types.Response) {
	if len(in.View.Members) == 0 || in.View.ReplFactor == 0 {
		res.Status = http.StatusBadRequest
		res.Error = msg.FailedToParse
		return
	}

	log.Printf("Received view change %#v, acting as coordinator\n", in.View)
	oldview := s.hash.GetView()
	nshards := len(oldview.Members) / oldview.ReplFactor
	storageCh := make(chan []store.Entry)

	// Retrieve a full storage object from each shard
	for i := 0; i < nshards; i++ {
		go func(replicas []string, shardId int) {
			// Try to reach a primary node on each shard in order
			var response types.Response
			for _, primary := range replicas {
				log.Println("Attempting to fetch shard", shardId, "state from", primary)
				httpResp, err := s.sendHttp(
					http.MethodGet,
					primary, PRIMARY_COLLECT_ENDPOINT,
					&in, &response)
				if err != nil {
					log.Printf("Failed to send collect from primary %q for shard %d: %v\n", primary, shardId, err)
					continue
				} else if httpResp.StatusCode != http.StatusOK {
					log.Printf("Failed to collect from primary %q for shard %d: %v\n", primary, shardId, err)
					continue
				}

				// The primary we tried returned a storage object.
				// Pass on the state and stop querying this shard.
				log.Println("Received state for shard", shardId)
				storageCh <- response.StorageState
				return
			}

			log.Println("All replicas in shard", shardId, "were unreachable. Ignoring shard.")
			storageCh <- []store.Entry{}
		}(oldview.Members[i*oldview.ReplFactor:(i+1)*oldview.ReplFactor], i+1)
	}

	// Accumulate all the states and remap them onto each shard
	statesByPrimary := make(map[string][]store.Entry)
	newhash := hash.New(in.View)
	for i := 0; i < nshards; i++ {
		state := <-storageCh
		for si := range state {
			shardId, err := newhash.GetKeyShardId(state[si].Key)
			if err != nil {
				log.Printf("Failed to get primary for key %q: %v", state[si].Key, err)
				log.Println("Ignoring key")
				continue
			}
			primary := newhash.GetReplicas(shardId)[0]
			statesByPrimary[primary] = append(statesByPrimary[primary], state[si])
		}
	}

	// Send all the new states to primary replace
	var wg sync.WaitGroup
	nshards = len(in.View.Members) / in.View.ReplFactor
	for i := 0; i < nshards; i++ {
		// Get the primary and the state we are sending to it
		primary := newhash.GetReplicas(i + 1)[0]
		state, ok := statesByPrimary[primary]
		if !ok {
			state = []store.Entry{}
		}

		// Dispatch the view change to said primary
		wg.Add(1)
		go func(primary string, state []store.Entry) {
			defer wg.Done()
			log.Println("Dispatching a state to new primary", primary)
			var response types.Response
			httpResp, err := s.sendHttp(
				http.MethodPut,
				primary, PRIMARY_REPLACE_ENDPOINT,
				&types.Input{View: in.View, StorageState: state}, &response)
			if err != nil {
				log.Printf("Failed to send state to primary %q: %v\n", primary, err)
				return
			} else if httpResp.StatusCode != http.StatusOK {
				log.Printf("Primary %q did not accept state: status code %d\n", primary, httpResp.StatusCode)
				return
			}

			log.Println("Primary at", primary, "accepted new state")
		}(primary, state)
	}
	wg.Wait()

	// Calculate all the shard info
	shards := make([]types.Shard, nshards)
	for i := 1; i <= nshards; i++ {
		replicas := newhash.GetReplicas(i)
		shards[i-1] = types.Shard{
			Id:       i,
			Replicas: replicas,
			KeyCount: func(entries []store.Entry) (count int) {
				// Key count is the number of not deleted keys
				for i := range entries {
					if !entries[i].Deleted {
						count += 1
					}
				}
				return
			}(statesByPrimary[replicas[0]]),
		}
	}
	res.Shards = shards

	// Set the final info!
	res.Message = msg.ViewChangeSuccess
	res.CausalCtx = s.store.Clock() // This is silly. This particular node's clock might be meaningless
}

// return our func (s *State) primaryCollect(in types.Input, res *types.Response) types.Response {
func (s *State) primaryCollect(in types.Input, res *types.Response) {
	replicas := s.hash.GetReplicas(s.hash.GetShardId(s.address))
	clockCh := make(chan clock.VectorClock)
	log.Println("Primary collect at shard clock", s.store.Clock().Subset(replicas))

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

	log.Println("Waiting for clock", waiting.Subset(replicas))
	err := s.store.WaitUntilCurrent(waiting)
	if err != nil {
		log.Println("Wait until current error", err)
		res.Status = http.StatusServiceUnavailable
		return
	}

	res.StorageState = s.store.AllEntries()
	log.Println("Up to date, sending the state with", len(res.StorageState), "entries")
}

func (s *State) primaryReplace(in types.Input, res *types.Response) {
	s.hash.TestAndSet(in.View)
	log.Println("Replacing storage with", len(in.StorageState), "entries")
	s.store.ReplaceEntries(in.StorageState)
	s.store.SetReplicas(s.hash.GetReplicas(s.hash.GetShardId(s.address)))
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
				SECONDARY_REPLACE_ENDPOINT,
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
	res.CausalCtx = s.store.Clock()
	log.Println("Secondary collect at shard clock", res.CausalCtx.Subset(s.hash.GetReplicas(s.hash.GetShardId(s.address))))
}

func (s *State) secondaryReplace(in types.Input, res *types.Response) {
	s.hash.TestAndSet(in.View)
	log.Println("Replacing storage with", len(in.StorageState), "entries")
	s.store.ReplaceEntries(in.StorageState)
	s.store.SetReplicas(s.hash.GetReplicas(s.hash.GetShardId(s.address)))
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
