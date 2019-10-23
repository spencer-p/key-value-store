package types

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

const (
	FailedToParse = "Failed to parse request body"
	KeyMissing    = "Key is missing"
	KeyTooLong    = "Key is too long"
)

type Response struct {
	// The status code is not marshalled to JSON. The wrapper function uses this
	// to write the HTTP response body. Defaults to 200.
	Status int `json:"-"`

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
