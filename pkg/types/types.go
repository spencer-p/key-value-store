package types

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/store"
)

type View struct {
	Members    []string `json:"view"`
	ReplFactor int      `json:"repl-factor"`
}

type Response struct {
	// The status code is not marshalled to JSON. The wrapper function uses this
	// to write the HTTP response body. Defaults to 200.
	Status int `json:"-"`

	// Standard information for key value storing
	Message  string `json:"message,omitempty"`
	Value    string `json:"value,omitempty"`
	Error    string `json:"error,omitempty"`
	Exists   *bool  `json:"doesExist,omitempty"`
	Replaced *bool  `json:"replaced,omitempty"`

	// Info about the state of shards
	Shards   interface{} `json:"shards,omitempty"`
	KeyCount *int        `json:"key-count,omitempty"`
	ShardId  *int        `json:"shard-id,omitempty"`
	Replicas []string    `json:"replicas,omitempty"`

	// Potential forwarding metadata
	Address string `json:"address,omitempty"`

	// Context for causal consistency
	CausalCtx clock.VectorClock `json:"causal-context"`

	// Internal view change data
	StorageState []store.Entry `json:"state,omitempty"`
}

type Shard struct {
	Id       int      `json:"shard-id"`
	Replicas []string `json:"replicas,omitempty"`
	KeyCount int      `json:"key-count"`
}

// Input stores arguments to each api request
type Input struct {
	Entry `json:",inline"`

	// A View and StorageState is only used for view change requests.
	View         `json:",inline"`
	StorageState []store.Entry `json:"state"`

	// Context the request thinks is current
	CausalCtx clock.VectorClock `json:"causal-context"`
}

// An Entry is a key value pair.
type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GossipResponse struct {
	Imported bool `json:"imported"`
}

type IncrementInput struct {
	Origin string `json:"origin"`
}

// WrapHTTP wraps an method that processes Inputs and writes a Response as an http
// handler.
func WrapHTTP(next func(Input, *Response)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := &Response{
			// Default to OK status
			Status: http.StatusOK,
		}

		// Parse input.
		var in Input
		if success, err := ParseInput(r, &in); !success {
			// Failed to process request input -- return error immediately
			result.Error = err
			result.Status = http.StatusBadRequest
			result.Serve(w, r)
			return
		}

		// default the causal context
		if in.CausalCtx == nil {
			in.CausalCtx = clock.VectorClock{}
		}

		next(in, result)
		result.Serve(w, r)
	}
}

// ParseInput fills out a input struct from an http request. Returns ok if it
// succeeded; otherwise the string contains an error message.
func ParseInput(r *http.Request, in *Input) (ok bool, err string) {
	params := mux.Vars(r)
	in.Key = params["key"]

	dec := json.NewDecoder(r.Body)
	if r.ContentLength > 0 {
		// Ignore body if there is none
		if err := dec.Decode(&in); err != nil {
			log.Println("Could not decode JSON:", err)
			return false, msg.FailedToParse
		}
	}

	return true, ""
}

// ValidateKey catches invalid keys and returns an invalid request. If the key
// is valid, the handler passes through.
func ValidateKey(next func(Input, *Response)) func(Input, *Response) {
	return func(in Input, res *Response) {
		if in.Key == "" {
			res.Error = msg.KeyMissing
			res.Status = http.StatusBadRequest
			return
		} else if len(in.Key) > 50 {
			res.Error = msg.KeyTooLong
			res.Status = http.StatusBadRequest
			return
		}

		next(in, res)
	}
}

// Serve writes a response struct to an http response.
func (result *Response) Serve(w http.ResponseWriter, r *http.Request) {
	// Set header and error text if necessary
	w.WriteHeader(result.Status)
	if result.Status >= 400 && result.Message == "" {
		result.Message = "Error in " + r.Method
		log.Println(result.Error)
	}
	log.Printf("%d %s\n", result.Status, result.Message)

	// Encode the result as JSON and send back
	enc := json.NewEncoder(w)
	if err := enc.Encode(result); err != nil {
		log.Println("Failed to marshal JSON response:", err)
		// response writer is likely fubar at this point.
		return
	}
}
