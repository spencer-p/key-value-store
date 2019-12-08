package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
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
			go func() {
				var wg sync.WaitGroup
				members := s.hash.GetReplicas(s.hash.GetShardId(s.address))
				for i := range members {
					if s.address != members[i] /*_, visitedNode := e.NodeHistory[members[i]]; !visitedNode*/ {
						wg.Add(1)
						go func(addr string) {
							s.sendGossip(ctx, e, addr)
							wg.Done()
						}(members[i])
					}
				}
				wg.Wait()
				for i := range members {
					if s.address != members[i] {
						go s.sendIncrement(members[i], s.address)
					}
				}
			}()
		}
	}
}

func (s *State) sendIncrement(node string, origin string) {
	tout := RETRY_TIMEOUT
	for {
		var res types.GossipResponse
		resp, err := s.sendHttp(http.MethodPut,
			node, "/kv-store/gossip-increment",
			&types.IncrementInput{Origin: origin}, &res,
		)
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Println("Failed to send increment to", node, "because", err)
			log.Println("Trying again")
			time.Sleep(tout)
			tout *= 2
			if tout > RETRY_TIMEOUT_MAX {
				tout = RETRY_TIMEOUT_MAX
			}
			continue
		}
		break
	}
}

func (s *State) receiveIncrement(w http.ResponseWriter, r *http.Request) {
	var res types.GossipResponse
	defer func() {
		if err := json.NewEncoder(w).Encode(&res); err != nil {
			log.Println("Failed to encode gossip response:", err)
		}
	}()

	var in types.IncrementInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		log.Println("Failed to decode increment input:", err)
		return
	}

	replicas := s.hash.GetReplicas(s.hash.GetShardId(s.address))
	for i := range replicas {
		if replicas[i] == s.address || replicas[i] == in.Origin {
			continue
		}
		s.store.BumpClockForNode(replicas[i])
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
