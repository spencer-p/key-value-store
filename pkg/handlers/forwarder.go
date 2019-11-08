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
	"time"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

const (
	// This has to be shorter than the http server read/write timeout so that we
	// don't get preempted by the http server dispatcher.
	CLIENT_TIMEOUT = 2 * time.Second

	ADDRESS_KEY = "forwading_address"
)

func (s *State) shouldForward(r *http.Request, rm *mux.RouteMatch) bool {
	key := path.Base(r.URL.Path)
	nodeAddr, err := s.hash.Get(key)
	if err != nil {
		log.Printf("Failed to get address for key %q: %v\n", key, err)
		log.Println("This node will handle the request")
		return false
	}

	if nodeAddr == s.address {
		log.Printf("Key %q is serviced by this node\n", key)
		return false
	} else {
		log.Printf("Key %q is serviced by %q\n", key, nodeAddr)

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
	return
}
