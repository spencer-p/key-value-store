package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"
)

const (
	VIEWCHANGE_TIMEOUT  = 1 * time.Second
	VIEWCHANGE_ENDPOINT = "/kv-store/view-change"
	KEYCOUNT_ENDPOINT   = "/kv-store/key-count"
	CONTENTTYPE         = "application/json"
)

var (
	BatchRejected = errors.New("Non-200 code received")
)

func (s *State) viewChange(in types.Input, res *types.Response) {
	if len(in.View) == 0 {
		res.Status = http.StatusBadRequest
		res.Error = "TODO"
		return
	}

	log.Printf("Received view change with addrs %v\n", in.View)

	oldview := s.c.Members()
	viewIsNew := !viewEqual(oldview, in.View)
	if viewIsNew {
		log.Println("This view is new information")
		// If this view change is new:
		// 1. Apply it
		// 2. Send out all our diffs
		s.c.Set(in.View)
		batches := s.getBatches()
		offloaded, err := s.dispatchBatches(in.View, batches)
		if err != nil {
			log.Println("Failed to offload some keys:", err)
		}
		s.deleteEntries(offloaded)
	}

	// Always apply the diff - we will get many of these
	log.Println("Applying a batch of", len(in.Batch), "keys")
	s.applyBatch(in.Batch)

	// TODO - set something meaningful in the response
	// TODO - if this was an oracle request, fetch the key count from everybody
	if !(!in.Internal && viewIsNew) {
		// We are not the first node to receive the view change - no further
		// action is required.
		return
	}

	log.Println("Cluster's view change is committed to all nodes")
	res.Message = msg.ViewChangeSuccess
	res.Shards = s.getKeyCounts(in.View)
}

// getBatches retrieves all batches of keys that should be on other nodes and
// returns them.
func (s *State) getBatches() map[string][]types.Entry {
	batches := make(map[string][]types.Entry)
	s.store.For(func(key, value string) store.IterAction {
		target, err := s.c.Get(key)
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
	for i := range entries {
		s.store.Delete(entries[i].Key)
	}
}

// dispatchBatches forwards a view change to all other nodes in the view.
// A list of batches that were successfully dispatched is returned with an error.
func (s *State) dispatchBatches(view []string, batches map[string][]types.Entry) ([]types.Entry, error) {
	var wg sync.WaitGroup
	var finalerr error
	deletech := make(chan []types.Entry)
	donech := make(chan struct{})

	for i := range view {
		addr := view[i]
		batch := batches[addr]
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("Sending view/batch with %d keys to %q\n", len(batch), addr)
			err := s.postBatch(addr, types.Input{
				View:  view,
				Batch: batch,
			})
			if err != nil {
				log.Printf("Failed to deliver batch to %q: %v\n", addr, err)
				finalerr = err
				return
			}
			deletech <- batch
		}()
	}

	go func() {
		wg.Wait()
		donech <- struct{}{}
		close(donech)
		close(deletech)
	}()

	var offloaded []types.Entry
cleanup:
	for {
		select {
		case <-donech:
			break cleanup
		case todelete := <-deletech:
			log.Println(len(todelete), "keys offloaded")
			offloaded = append(offloaded, todelete...)
		}
	}

	return offloaded, finalerr
}

func (s *State) applyBatch(batch []types.Entry) {
	for _, e := range batch {
		s.store.Set(e.Key, e.Value)
	}
}

// viewEqual returns true iff the views are identical.
func viewEqual(v1 []string, v2 []string) bool {
	s1 := util.StringSet(v1)
	s2 := util.StringSet(v2)

	return util.SetEqual(s1, s2)
}

func (s *State) postBatch(target string, payload types.Input) error {
	payload.Internal = true

	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	err := enc.Encode(payload)
	if err != nil {
		log.Println("Failed to encode payload to send a view change:", err)
		return err
	}

	resp, err := s.cli.Post(util.CorrectURL(target+VIEWCHANGE_ENDPOINT), CONTENTTYPE, &body)
	if err != nil {
		log.Println("Failed to make view change POST:", err)
		return err
	}

	// TODO Parse the response
	if resp.StatusCode != http.StatusOK {
		return BatchRejected
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
				shard.KeyCount = s.store.NumKeys()
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
