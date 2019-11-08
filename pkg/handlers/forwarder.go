// Package leader implements handlers for forwarder instances.
package handlers

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/gorilla/mux"
)

const (
	// This has to be shorter than the http server read/write timeout so that we
	// don't get preempted by the http server dispatcher.
	CLIENT_TIMEOUT = 2 * time.Second
)

// forwarder holds all state that a forwarder needs to operate.
type forwarder struct {
	client http.Client
	addr   *url.URL
}

func (f *forwarder) forwardMessage(w http.ResponseWriter, r *http.Request) {
	requestBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Failed to read body:", err)
		http.Error(w, "Failed to read request", http.StatusInternalServerError)
		return
	}

	target := *f.addr
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

	resp, err := f.client.Do(request)
	if err != nil {
		log.Println("Failed to do proxy request:", err)
		// Presumably the destination node is down.
		result := types.Response{
			Status: http.StatusServiceUnavailable,
			Error:  msg.MainFailure,
		}
		result.Serve(w, request)
		return
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func RouteForwarder(r *mux.Router, fwd string) error {
	if !strings.HasPrefix(fwd, "http://") {
		fwd = "http://" + fwd
	}

	addr, err := url.Parse(fwd)
	if err != nil {
		return fmt.Errorf("Bad forwarding address %q: %v\n", fwd, addr)
	}

	f := forwarder{
		client: http.Client{
			Timeout: CLIENT_TIMEOUT,
		},
		addr: addr,
	}

	r.PathPrefix("/").Handler(http.HandlerFunc(f.forwardMessage))
	return nil
}
