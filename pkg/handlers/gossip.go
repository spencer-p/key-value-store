package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/util"
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
				if members[i] != s.address {
					go s.sendGossip(ctx, e, members[i])
				}
			}
		}
	}
}

func (s *State) receiveGossip(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(r.Body)
	var e store.Entry
	if err := dec.Decode(&e); err != nil {
		log.Println("Received malformed gossip:", err)
		return
	}
	log.Printf("Import gossip of %q\n", e.Key)

	if err := s.store.ImportEntry(e); err != nil {
		log.Printf("Failed to import entry %v: %v\n", e, err)
		http.Error(w, "cannot apply gossip", http.StatusServiceUnavailable)
	}
}

func (s *State) sendGossip(ctx context.Context, e store.Entry, node string) {
	log.Printf("Sending gossip of %v to %s\n", e, node)
	target := util.CorrectURL(node)
	tout := RETRY_TIMEOUT
	for {
		var body bytes.Buffer
		enc := json.NewEncoder(&body)
		if err := enc.Encode(&e); err != nil {
			// show stopping error - this could lock the entire system
			log.Println("Failed to encode entry:", err)
			break
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, target+"/kv-store/gossip", &body)
		if err != nil {
			// also show stopping
			log.Println("Could not create request to send entry:", err)
			break
		}
		if resp, err := s.cli.Do(request); (err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) ||
			resp.StatusCode != http.StatusOK {
			log.Printf("Failed to gossip %v: %v\n", e, err)
			log.Println("Retrying after timeout")
		} else {
			// Successfully gossipped.
			s.store.BumpClockForNode(node)
			return
		}

		// Perform exponential backoff with a max of one second
		time.Sleep(tout)
		tout *= 2
		if tout > RETRY_TIMEOUT_MAX {
			tout = RETRY_TIMEOUT_MAX
		}
	}
}
