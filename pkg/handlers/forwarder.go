// Package leader implements handlers for follower instances.
package handlers

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"
)

const (
	// This has to be shorter than the http server read/write timeout so that we
	// don't get preempted by the http server dispatcher.
	CLIENT_TIMEOUT = 2 * time.Second
)

func (s *State) forwardMessage(w http.ResponseWriter, r *http.Request) {
	requestBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Failed to read body:", err)
		http.Error(w, "Failed to read request", http.StatusInternalServerError)
		return
	}

	target, err := url.Parse(util.CorrectURL(s.address))

	result := types.Response{}
	if err != nil {
		log.Println("Bad forwarding address")
		result.Error = msg.BadForwarding
		result.Serve(w, r)
	}

	target.Path = path.Join(target.Path, r.URL.Path)

	request, err := http.NewRequest(r.Method,
		target.String(),
		bytes.NewBuffer(requestBody))
	if err != nil {
		log.Println("Failed to make proxy request:", err)
		http.Error(w, "Failed to make request", http.StatusInternalServerError)
		return
	}

	request.Header = r.Header.Clone()

	resp, err := s.cli.Do(request)
	if err != nil {
		log.Println("Failed to do proxy request:", err)
		// Presumably the leader is down.
		result.Status = http.StatusServiceUnavailable
		result.Error = msg.MainFailure
		result.Serve(w, request)
		return
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
