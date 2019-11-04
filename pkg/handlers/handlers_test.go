package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/spencer-p/cse138/pkg/ptr"
	"github.com/spencer-p/cse138/pkg/types"

	"github.com/google/go-cmp/cmp"
	"github.com/gorilla/mux"
)

func TestPut(t *testing.T) {
	tests := map[string][]struct {
		method   string
		in       types.Input
		want     types.Response
		wantCode int
	}{
		"repeated PUT requests": {{
			method: "PUT",
			in: types.Input{
				Key:   "mykey",
				Value: "myvalue",
			},
			want: types.Response{
				Message:  PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}, {
			method: "PUT",
			in: types.Input{
				Key:   "mykey",
				Value: "value2",
			},
			want: types.Response{
				Message:  UpdateSuccess,
				Replaced: ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "PUT",
			in: types.Input{
				Key:   "a-new-key",
				Value: "myvalue",
			},
			want: types.Response{
				Message:  PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}},
		"bad inputs": {{
			method: "PUT",
			in: types.Input{
				Key:   "12345678901234567890123456789012345679012345678901234567890",
				Value: "myvalue",
			},
			want: types.Response{
				Error:   KeyTooLong,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}, {
			method: "PUT",
			in: types.Input{
				Key:   "",
				Value: "myvalue",
			},
			want: types.Response{
				Error:   KeyMissing,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}, {
			method: "PUT",
			in: types.Input{
				Key:   "abc",
				Value: "",
			},
			want: types.Response{
				Error:   ValueMissing,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}},
		"canonical example": {{
			method: "PUT",
			in: types.Input{
				Key:   "x",
				Value: "1",
			},
			want: types.Response{
				Message:  PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}, {
			method: "GET",
			in: types.Input{
				Key: "y",
			},
			want: types.Response{
				Error:   KeyDNE,
				Message: "Error in GET",
				Exists:  ptr.Bool(false),
			},
			wantCode: 404,
		}, {
			method: "GET",
			in: types.Input{
				Key: "x",
			},
			want: types.Response{
				Message: GetSuccess,
				Value:   "1",
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "PUT",
			in: types.Input{
				Key:   "x",
				Value: "2",
			},
			want: types.Response{
				Message:  UpdateSuccess,
				Replaced: ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "GET",
			in: types.Input{
				Key: "x",
			},
			want: types.Response{
				Message: GetSuccess,
				Value:   "2",
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "DELETE",
			in: types.Input{
				Key: "x",
			},
			want: types.Response{
				Message: DeleteSuccess,
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "GET",
			in: types.Input{
				Key: "x",
			},
			want: types.Response{
				Error:   KeyDNE,
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
			Route(r)

			for i, test := range requests {

				// Run each request as its own test for observability
				t.Run(fmt.Sprintf("%d %s %s", i, test.method, test.in.Key), func(t *testing.T) {
					req := httptest.NewRequest(test.method,
						"/kv-store/"+test.in.Key,
						bytes.NewBufferString(`{"value":"`+test.in.Value+`"}`))

					resp := httptest.NewRecorder()

					r.ServeHTTP(resp, req)

					var got types.Response
					if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
						t.Errorf("Failed to parse response: %v", err)
					}

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
