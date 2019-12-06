package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/spencer-p/cse138/pkg/msg"
	"github.com/spencer-p/cse138/pkg/ptr"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/google/go-cmp/cmp"
	"github.com/gorilla/mux"
)

const (
	FAKE_ADDRESS = "1.1.1.1"
)

func kv(key, value string) types.Input {
	return types.Input{Entry: types.Entry{
		Key:   key,
		Value: value,
	}}
}

func k(key string) types.Input {
	return types.Input{Entry: types.Entry{
		Key: key,
	}}
}

func TestPut(t *testing.T) {
	tests := map[string][]struct {
		method   string
		in       types.Input
		want     types.Response
		wantCode int
	}{
		"repeated PUT requests": {{
			method: "PUT",
			in:     kv("mykey", "myvalue"),
			want: types.Response{
				Message:  msg.PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}, {
			method: "PUT",
			in:     kv("mykey", "value2"),
			want: types.Response{
				Message:  msg.UpdateSuccess,
				Replaced: ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "PUT",
			in:     kv("a-new-key", "myvalue"),
			want: types.Response{
				Message:  msg.PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}},
		"bad inputs": {{
			method: "PUT",
			in:     kv("12345678901234567890123456789012345679012345678901234567890", "myvalue"),
			want: types.Response{
				Error:   msg.KeyTooLong,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}, {
			method: "PUT",
			in:     kv("", "myvalue"),
			want: types.Response{
				Error:   msg.KeyMissing,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}, {
			method: "PUT",
			in:     kv("abc", ""),
			want: types.Response{
				Error:   msg.ValueMissing,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}},
		"canonical example": {{
			method: "PUT",
			in:     kv("x", "1"),
			want: types.Response{
				Message:  msg.PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}, {
			method: "GET",
			in:     k("y"),
			want: types.Response{
				Error:   msg.KeyDNE,
				Message: "Error in GET",
				Exists:  ptr.Bool(false),
			},
			wantCode: 404,
		}, {
			method: "GET",
			in:     k("x"),
			want: types.Response{
				Message: msg.GetSuccess,
				Value:   "1",
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "PUT",
			in:     kv("x", "2"),
			want: types.Response{
				Message:  msg.UpdateSuccess,
				Replaced: ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "GET",
			in:     k("x"),
			want: types.Response{
				Message: msg.GetSuccess,
				Value:   "2",
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "DELETE",
			in:     k("x"),
			want: types.Response{
				Message: msg.DeleteSuccess,
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "GET",
			in:     k("x"),
			want: types.Response{
				Error:   msg.KeyDNE,
				Message: "Error in GET",
				Exists:  ptr.Bool(false),
			},
			wantCode: 404,
		}},
	}

	for name, requests := range tests {
		t.Run(name, func(t *testing.T) {

			// Create one server per set of requests
			r := mux.NewRouter()
			s := NewState(context.Background(), FAKE_ADDRESS, types.View{
				Members:    []string{FAKE_ADDRESS},
				ReplFactor: 1,
			})
			s.Route(r)

			for i, test := range requests {

				// Run each request as its own test for observability
				t.Run(fmt.Sprintf("%d %s %s", i, test.method, test.in.Key), func(t *testing.T) {
					req := httptest.NewRequest(test.method,
						"/kv-store/keys/"+test.in.Key,
						bytes.NewBufferString(`{"value":"`+test.in.Value+`","causal-context":{}}`))

					resp := httptest.NewRecorder()

					r.ServeHTTP(resp, req)

					var got types.Response
					if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
						t.Errorf("Failed to parse response: %v", err)
					}

					// ignore the clock
					got.CausalCtx = nil

					if diff := cmp.Diff(&got, &test.want); diff != "" {
						t.Errorf("Got bad body (-got, +want): %s", diff)
					}

					if resp.Code != test.wantCode {
						t.Errorf("Got bad status code, expected %d, got %d", test.wantCode, resp.Code)
					}
				})
			}
		})
	}
}
