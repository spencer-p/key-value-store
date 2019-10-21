package leader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/spencer-p/cse138/pkg/ptr"

	"github.com/google/go-cmp/cmp"
	"github.com/gorilla/mux"
)

func TestPut(t *testing.T) {
	tests := map[string][]struct {
		method   string
		in       Input
		want     Response
		wantCode int
	}{
		"repeated PUT requests": {{
			method: "PUT",
			in: Input{
				Key:   "mykey",
				Value: "myvalue",
			},
			want: Response{
				Message:  PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}, {
			method: "PUT",
			in: Input{
				Key:   "mykey",
				Value: "value2",
			},
			want: Response{
				Message:  UpdateSuccess,
				Replaced: ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "PUT",
			in: Input{
				Key:   "a-new-key",
				Value: "myvalue",
			},
			want: Response{
				Message:  PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}},
		"bad inputs": {{
			method: "PUT",
			in: Input{
				Key:   "12345678901234567890123456789012345679012345678901234567890",
				Value: "myvalue",
			},
			want: Response{
				Error:   KeyTooLong,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}, {
			method: "PUT",
			in: Input{
				Key:   "",
				Value: "myvalue",
			},
			want: Response{
				Error:   KeyMissing,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}, {
			method: "PUT",
			in: Input{
				Key:   "abc",
				Value: "",
			},
			want: Response{
				Error:   ValueMissing,
				Message: "Error in PUT",
			},
			wantCode: 400,
		}},
		"canonical example": {{
			method: "PUT",
			in: Input{
				Key:   "x",
				Value: "1",
			},
			want: Response{
				Message:  PutSuccess,
				Replaced: ptr.Bool(false),
			},
			wantCode: 201,
		}, {
			method: "GET",
			in: Input{
				Key: "y",
			},
			want: Response{
				Error:   KeyDNE,
				Message: "Error in GET",
				Exists:  ptr.Bool(false),
			},
			wantCode: 404,
		}, {
			method: "GET",
			in: Input{
				Key: "x",
			},
			want: Response{
				Message: GetSuccess,
				Value:   "1",
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "PUT",
			in: Input{
				Key:   "x",
				Value: "2",
			},
			want: Response{
				Message:  UpdateSuccess,
				Replaced: ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "GET",
			in: Input{
				Key: "x",
			},
			want: Response{
				Message: GetSuccess,
				Value:   "2",
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "DELETE",
			in: Input{
				Key: "x",
			},
			want: Response{
				Message: DeleteSuccess,
				Exists:  ptr.Bool(true),
			},
			wantCode: 200,
		}, {
			method: "GET",
			in: Input{
				Key: "x",
			},
			want: Response{
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

					var got Response
					if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
						t.Errorf("Failed to parse response: %v", err)
					}

					if diff := cmp.Diff(&got, &test.want,
						cmp.AllowUnexported(Response{})); diff != "" {
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
