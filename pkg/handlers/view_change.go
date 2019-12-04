package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"
)

const (
	VIEWCHANGE_ENDPOINT = "/kv-store/view-change"
	KEYCOUNT_ENDPOINT   = "/kv-store/key-count"
)

func (s *State) viewChange(in types.Input, res *types.Response) {
	view := strings.Split(in.View, ",")
	if len(view) == 0 {
		res.Status = http.StatusBadRequest
		res.Error = msg.FailedToParse
		return
	}

	log.Printf("Received view change with addrs %v\n", view)

	viewIsNew := s.hash.TestAndSet(view)
	if viewIsNew {
		log.Println("This view is new information")
		// We just set a new view. We have keys that need to move to other
		// nodes.
		batches := s.getBatches()
		offloaded, err := s.dispatchBatches(view, batches)
		if err != nil {
			log.Println("Failed to offload some keys:", err)
		}
		s.deleteEntries(offloaded)
	}

	// Always apply the diff - we will get many of these
	log.Println("Applying a batch of", len(in.Batch), "keys")
	s.applyBatch(in.Batch)

	if !(!in.Internal && viewIsNew) {
		// We are not the first node to receive the view change - no further
		// action is required.
		res.Message = msg.PartialViewChangeSuccess
		return
	}

	// We're the node that initiated the view change -- return a meaningful
	// response to the oracle
	log.Println("Cluster's view change is committed to all nodes")
	res.Message = msg.ViewChangeSuccess
	res.Shards = s.getKeyCounts(view)
}

// getBatches retrieves all batches of keys that should be on other nodes and
// returns them.
func (s *State) getBatches() map[string][]types.Entry {
	batches := make(map[string][]types.Entry)
	s.store.For(func(key string, e store.Entry) store.IterAction {
		target, err := s.hash.Get(key)
		value := e.Value
		if err != nil {
			log.Printf("Invalid key %q=%q: %v. Dropping.\n", key, value, err)
		}

		if target != s.address {
			batches[target] = append(batches[target], types.Entry{
				Key:   key,
				Value: value,
			})
		}
		return store.CONTINUE
	})
	return batches
}

// deleteEntries removes the given batches from our own state.
func (s *State) deleteEntries(entries []types.Entry) {
	log.Println("Deleting", len(entries), "offloaded keys")
	for _ = range entries {
		// TODO Force these deletes somehow (or don't?)
		//s.store.Delete(clock.VectorClock{}, entries[i].Key)
	}
}

// dispatchBatches forwards a view change to all other nodes in the view.
// A list of batches that were successfully dispatched is returned with an error.
func (s *State) dispatchBatches(view []string, batches map[string][]types.Entry) ([]types.Entry, error) {
	var wg sync.WaitGroup
	var finalerr error
	deletech := make(chan []types.Entry)
	donech := make(chan struct{})

	// Spin up a goroutine to send each batch to each node
	for i := range view {
		wg.Add(1)
		go func(addr string, batch []types.Entry) {
			defer wg.Done()
			log.Printf("Sending view/batch with %d keys to %q\n", len(batch), addr)
			err := s.sendBatch(addr, types.Input{
				View:  strings.Join(view, ","),
				Batch: batch,
			})
			if err != nil {
				log.Printf("Failed to deliver batch to %q: %v\n", addr, err)
				finalerr = err
				return
			}
			deletech <- batch
		}(view[i], batches[view[i]])
	}

	// Wait for all the goroutines to terminate.
	// When the waitgroup is done, everything in the deletech has been
	// processed. We can then notify ourselves via donech and close both
	// channels.
	go func() {
		wg.Wait()
		donech <- struct{}{}
		close(donech)
		close(deletech)
	}()

	// Accumulate all the keys that have been successfully offloaded and return
	// them with a potential error when done
	var offloaded []types.Entry
	for {
		select {
		case <-donech:
			return offloaded, finalerr
		case todelete := <-deletech:
			log.Println(len(todelete), "keys offloaded")
			offloaded = append(offloaded, todelete...)
		}
	}
}

func (s *State) applyBatch(batch []types.Entry) {
	for _, _ = range batch {
		// TODO these need to be stored better
		//s.store.Set(e.Key, e.Value)
	}
}

// sendBatch sends a types.Input to the target node's address.
func (s *State) sendBatch(target string, payload types.Input) error {
	payload.Internal = true

	// Encode the payload to a buffer
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	err := enc.Encode(payload)
	if err != nil {
		return fmt.Errorf("failed to encode payload: %w", err)
	}

	// Build a request with our payload to the target address
	target = util.CorrectURL(target + VIEWCHANGE_ENDPOINT)
	req, err := http.NewRequest(http.MethodPut, target, &body)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send the request & return a useful result
	resp, err := s.cli.Do(req)
	if err != nil {
		return fmt.Errorf("failed to do view change request: %w", err)
	}

	// TODO Parse the response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-200 status code: %d", resp.StatusCode)
	}
	return nil
}

// getKeyCounts retrieves information about the shards for the given view.
func (s *State) getKeyCounts(view []string) []types.Shard {
	shards := make([]types.Shard, len(view))
	var wg sync.WaitGroup

	log.Println("Requesting key counts from the other nodes")
	for i := range view {
		wg.Add(1)
		go func(addr string, shard *types.Shard) {
			defer wg.Done()

			// Set the address and an invalid value before we found out the
			// actual value
			shard.Address = addr
			shard.KeyCount = -1

			// Don't make a request if it's just ourselves
			if addr == s.address {
				// TODO do something smarter
				_, shard.KeyCount, _ = s.store.NumKeys(clock.VectorClock{})
				return
			}

			// Dispatch a get request to the other node
			resp, err := s.cli.Get(util.CorrectURL(addr) + KEYCOUNT_ENDPOINT)
			if err != nil {
				log.Printf("Failed to send a req to %q: %v\n", addr, err)
				return
			}
			if resp.StatusCode != http.StatusOK {
				log.Printf("Node at %q returned %d for key count\n", addr, resp.StatusCode)
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
			shard.KeyCount = *response.KeyCount
		}(view[i], &shards[i])
	}

	wg.Wait()
	return shards
}
