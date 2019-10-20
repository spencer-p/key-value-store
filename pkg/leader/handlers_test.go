package leader

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spencer-p/cse138/pkg/ptr"

	"github.com/google/go-cmp/cmp"
	"github.com/gorilla/mux"
)

func TestPut(t *testing.T) {
	// TODO(spencer-p) make this a table test
	r := mux.NewRouter()
	Route(r)

	req := httptest.NewRequest("PUT",
		"/kv-store/mykey",
		bytes.NewBufferString(`{"value":"myvalue"}`))

	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	want := &Response{
		Message:  PutSuccess,
		Replaced: ptr.Bool(false),
	}

	var got Response
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Errorf("Failed to parse response: %v", err)
	}

	if diff := cmp.Diff(&got, want,
		cmp.AllowUnexported(Response{})); diff != "" {
		t.Errorf("Got bad body (-want, +got): %s", diff)
	}

	if resp.Code != http.StatusOK {
		t.Errorf("Got bad status code, expected %d, got %d", http.StatusOK, resp.Code)
	}
}
