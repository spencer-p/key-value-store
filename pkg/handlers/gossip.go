package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
)

const (
	RETRY_TIMEOUT     = 10 * time.Millisecond
	RETRY_TIMEOUT_MAX = 1 * time.Second
)

func (s *State) dispatchGossip(ctx context.Context, journal <-chan store.Entry) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-journal:
			members := s.hash.GetReplicas(s.hash.GetShardId(s.address))
			for i := range members {
				if _, visitedNode := e.NodeHistory[members[i]]; !visitedNode {
					go s.sendGossip(ctx, e, members[i])
				}
			}
		}
	}
}

func (s *State) receiveGossip(w http.ResponseWriter, r *http.Request) {
	var res types.GossipResponse
	defer func() {
		if err := json.NewEncoder(w).Encode(&res); err != nil {
			log.Println("Failed to encode gossip response:", err)
		}
	}()

	var e store.Entry
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		log.Println("Received malformed gossip:", err)
		return
	}
	log.Printf("Import gossip of %q\n", e.Key)

	imported, err := s.store.ImportEntry(e)
	if err != nil {
		log.Printf("Failed to import entry %v: %v\n", e, err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	res.Imported = imported
}

func (s *State) sendGossip(ctx context.Context, e store.Entry, node string) {
	log.Printf("Sending gossip of %v to %s\n", e, node)
	tout := RETRY_TIMEOUT
	for {
		var res types.GossipResponse
		resp, err := s.sendHttp(
			http.MethodPut,
			node, "/kv-store/gossip",
			&e, &res,
		)

		if gossipSucceeded(resp, err) {
			// Successfully gossipped.
			if res.Imported {
				// If they also imported it, we can count that as an event.
				s.store.BumpClockForNode(node)
			}
			return
		}

		log.Printf("Failed to gossip %v: %v\n", e, err)
		log.Println("Retrying after timeout")

		// Perform exponential backoff with a max of one second
		time.Sleep(tout)
		tout *= 2
		if tout > RETRY_TIMEOUT_MAX {
			tout = RETRY_TIMEOUT_MAX
		}
	}
}

func gossipSucceeded(resp *http.Response, err error) bool {
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		// Returned an error that does not have to do with the context being cancelled.
		return false
	} else if resp.StatusCode != http.StatusOK {
		// The target rejected it for some reason. This is OK
		return false
	}
	// There may have been a context error, which means the gossip did not succeed.
	// However, we should stop looping forever if the context is closed.
	// In the common case, this was no error and a 200 from the other node.
	return true
}
