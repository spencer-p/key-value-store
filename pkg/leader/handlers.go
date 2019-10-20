// Package leader implements all behavior specific to a leader instance.
package leader

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

const (
	PutSuccess    = "Added successfully"
	UpdateSuccess = "Updated successfully"
	GetSuccess    = "Retrieved successfully"

	FailedToParse = "Failed to parse request body"
	KeyDNE        = "Key does not exist"
	KeyTooLong    = "Key is too long"
	ValueMissing  = "Value is missing"
	KeyMissing    = "Key is missing"
)

type Response struct {
	// The status code is not marshalled to JSON. The wrapper function uses this
	// to write the HTTP response body. Defaults to 200.
	status int `json:"-"`

	Message  string `json:"message,omitempty"`
	Value    string `json:"value,omitempty"`
	Error    string `json:"error,omitempty"`
	Exists   *bool  `json:"doesExist,omitempty"`
	Replaced *bool  `json:"replaced,omitempty"`
}

// Input stores arguments to each api request
type Input struct {
	Key   string
	Value string `json:"value"`
}

func (s *storage) getHandler(in Input, res *Response) {
	value, exists := s.Read(in.Key)

	res.Exists = &exists
	if exists {
		res.Message = GetSuccess
		res.Value = value
	} else {
		res.Error = KeyDNE
		res.status = http.StatusNotFound
	}
}

func (s *storage) putHandler(in Input, res *Response) {
	if in.Value == "" {
		res.Error = ValueMissing
		res.status = http.StatusBadRequest
		return
	}

	replaced := s.Set(in.Key, in.Value)

	res.Replaced = &replaced
	res.Message = PutSuccess
	if replaced {
		res.Message = UpdateSuccess
	}
}

func withJSON(next func(Input, *Response)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := &Response{
			// Default to OK status
			status: http.StatusOK,
		}

		// Parse input.
		var in Input
		if success, err := parseInput(r, &in); !success {
			// Failed to process request input -- return error immediately
			result.Error = err
			result.status = http.StatusBadRequest
			writeResponse(w, r, result)
			return
		}

		next(in, result)
		writeResponse(w, r, result)
	}
}

// parseInput fills out a input struct from an http request. Returns ok if it
// succeeded; otherwise the string contains an error message.
func parseInput(r *http.Request, in *Input) (ok bool, err string) {
	params := mux.Vars(r)
	in.Key = params["key"]

	dec := json.NewDecoder(r.Body)
	if r.ContentLength > 0 {
		// Ignore body if there is none
		if err := dec.Decode(&in); err != nil {
			log.Println("Could not decode JSON:", err)
			return false, FailedToParse
		}
	}

	if in.Key == "" {
		return false, KeyMissing
	} else if len(in.Key) > 50 {
		return false, KeyTooLong
	}

	return true, ""
}

func writeResponse(w http.ResponseWriter, r *http.Request, result *Response) {
	// Set header and error text if necessary
	w.WriteHeader(result.status)
	if result.status >= 400 && result.Message == "" {
		result.Message = "Error in " + r.Method
		log.Println(result.Error)
	}
	log.Printf("%d %s\n", result.status, result.Message)

	// Encode the result as JSON and send back
	enc := json.NewEncoder(w)
	if err := enc.Encode(result); err != nil {
		log.Println("Failed to marshal JSON response:", err)
		// response writer is likely fubar at this point.
		return
	}
}

func Route(r *mux.Router) {
	s := newStorage()

	r.HandleFunc("/kv-store/{key:.*}", withJSON(s.putHandler)).Methods(http.MethodPut)
	r.HandleFunc("/kv-store/{key:.*}", withJSON(s.getHandler)).Methods(http.MethodGet)
}
