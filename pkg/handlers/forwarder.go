// Package leader implements handlers for follower instances.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

const (
	// This has to be shorter than the http server read/write timeout so that we
	// don't get preempted by the http server dispatcher.
	CLIENT_TIMEOUT = 5 * time.Minute

	SHARD_ENDPOINT = "/kv-store/shards"
	ADDRESS_KEY    = "forwarding_address"
)

func (s *State) shouldForwardId(r *http.Request, rm *mux.RouteMatch) bool {
	id := path.Base(r.URL.Path)
	if id == strconv.Itoa(s.hash.GetShardId(s.address)) {
		log.Printf("Id %q is serviced by this node\n", id)
		return false
	} else {

		// Store the target node address in the http request context.

		id, err := strconv.Atoi(id)
		if err != nil {
			log.Printf("Failed to strconv id %q\n", id)
			return false
		}

		viewInfo := s.hash.GetView()
		view := viewInfo.Members
		replFactor := viewInfo.ReplFactor
		shardIndex := (len(view) / replFactor) * (id - 1)
		log.Printf("Id %q is serviced by %q\n", id, view[shardIndex])
		ctx := context.WithValue(r.Context(), ADDRESS_KEY, view[shardIndex])
		*r = *(r.WithContext(ctx))
		return true
	}
}

func (s *State) shouldForwardKey(r *http.Request, rm *mux.RouteMatch) bool {
	key := path.Base(r.URL.Path)
	nodeAddr, err := s.hash.Get(key)
	return s.shouldForwardToNode(r, key, nodeAddr, err)
}

func (s *State) shouldForwardRead(r *http.Request, rm *mux.RouteMatch) bool {
	key := path.Base(r.URL.Path)

	if keyBelongsOnShard, err := s.hash.GetKeyShardId(key); err != nil {
		log.Println("The state of the hash is broken:", err)
		return true // not our problem anymore :^)
	} else if keyBelongsOnShard == s.hash.GetShardId(s.address) {
		// If the key belongs to our shard, we should not forward it.
		return false
	}

	nodeAddr, err := s.hash.GetAny(key)
	return s.shouldForwardToNode(r, key, nodeAddr, err)
}

func (s *State) shouldForwardToNode(r *http.Request, key, nodeAddr string, err error) bool {
	if err != nil {
		log.Printf("Failed to get address for key %q: %v\n", key, err)
		log.Println("This node will handle the request")
		return false
	}
	if nodeAddr == s.address {
		return false
	} else {
		// Store the target node address in the http request context.
		ctx := context.WithValue(r.Context(), ADDRESS_KEY, nodeAddr)
		*r = *(r.WithContext(ctx))
		return true
	}
}

func (s *State) forwardMessage(w http.ResponseWriter, r *http.Request) {
	var result types.Response
	defer result.Serve(w, r)

	requestBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Failed to read body:", err)
		result.Status = http.StatusInternalServerError
		result.Error = msg.FailedToParse
		return
	}

	nodeAddr, ok := r.Context().Value(ADDRESS_KEY).(string)
	if !ok {
		log.Println("Forwarding address not set in req context")
		result.Status = http.StatusInternalServerError
		result.Error = msg.BadForwarding
		return
	}

	log.Printf("Forwarding req w/ %q to %q\n", mux.Vars(r)["key"], nodeAddr)

	target, err := url.Parse(util.CorrectURL(nodeAddr))
	if err != nil {
		log.Println("Bad forwarding address")
		result.Status = http.StatusInternalServerError
		result.Error = msg.BadForwarding
		return
	}

	target.Path = path.Join(target.Path, r.URL.Path)

	request, err := http.NewRequest(r.Method,
		target.String(),
		bytes.NewBuffer(requestBody))
	if err != nil {
		log.Println("Failed to make proxy request:", err)
		result.Status = http.StatusInternalServerError
		result.Error = msg.MainFailure // TODO better error message
		return
	}

	request.Header = r.Header.Clone()

	resp, err := s.cli.Do(request)
	if err != nil {
		log.Println("Failed to do proxy request:", err)
		// Presumably the leader is down.
		result.Status = http.StatusServiceUnavailable
		result.Error = msg.MainFailure
		return
	}

	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Println("Could not parse forwarded result:", err)
		result.Status = http.StatusInternalServerError
		result.Error = msg.MainFailure // TODO better error
		return
	}

	result.Status = resp.StatusCode
	result.Address = nodeAddr
	return
}

func (s *State) getShardInfo(view []string, CausalCtx clock.VectorClock) []types.Shard {
	replFactor := s.hash.GetReplicationFactor()
	shardTotal := len(view) / replFactor
	shards := make([]types.Shard, shardTotal)
	var wg sync.WaitGroup

	log.Println("Requesting key counts from the other shards")
	shardIndex := shardTotal
	for i := range shards {
		wg.Add(1)
		go func(addr string, shard *types.Shard) {
			defer wg.Done()

			//TODO Figure out why i starts out as 1
			log.Println("i is", i)
			// Set the address and an invalid value before we found out the
			// actual value

			shard.KeyCount = -1

			log.Println(addr)
			// Don't make a request if it's just ourselves
			shardIndex = s.hash.GetShardId(addr) //TODO Replace this with get ID from address method
			if addr == s.address {
				shard.Id = strconv.Itoa(s.hash.GetShardId(addr))
				err, KeyCount, CausalCtx := s.store.NumKeys(CausalCtx)
				if err != nil {
					log.Printf("Failed to get NumKeys for addr %q: %v", addr, err)
				}
				_ = CausalCtx
				shard.KeyCount = KeyCount
				return
			}

			// Dispatch a get request to the other node
			resp, err := s.cli.Get(util.CorrectURL(addr) + SHARD_ENDPOINT + "/" + strconv.Itoa(shardIndex))
			if err != nil {
				log.Printf("Failed to send a req to %q: %v\n", addr, err)
				return
			}
			if resp.StatusCode != http.StatusOK {
				log.Printf("Shard at %q returned %d for key count\n", addr, resp.StatusCode)
				return
			}

			// Parse the response
			var response types.Response
			if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
				log.Printf("Failed to parse response from %q: %v\n", addr, err)
				return
			}

			if response.KeyCount == nil {
				log.Printf("Response from %q does not have a key count\n", addr)
				return
			}

			// We actually got a response!
			shard.Id = response.ShardId
			shard.KeyCount = *response.KeyCount
		}(view[i*replFactor], &shards[i])
	}

	wg.Wait()
	return shards
}
