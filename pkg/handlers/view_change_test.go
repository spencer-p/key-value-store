package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

func TestDeleteBatches(t *testing.T) {
	s := &State{
		store: store.New(),
	}

	s.store.Set("x", "1")
	s.store.Set("y", "2")

	s.deleteEntries([]types.Entry{{
		Key: "x",
	}})

	v, ok := s.store.Read("x")
	if ok {
		t.Errorf("Got x=%q, wanted it to be missing", v)
	}

	v, ok = s.store.Read("y")
	if v != "2" || !ok {
		t.Errorf("Expected read y to return \"2\", ok; got %q and %t", v, ok)
	}
}

func ServerWithPort(port string) *http.Server {
	r := mux.NewRouter()
	r.Use(util.WithLog)
	return &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:" + port,
	}
}

func TestViewChange(t *testing.T) {
	var srv [3]*http.Server
	var st [3]*State
	srv[0] = ServerWithPort("9090")
	srv[1] = ServerWithPort("9091")
	srv[2] = ServerWithPort("9092")

	view := []string{
		"127.0.0.1:9090",
		"127.0.0.1:9091",
		"127.0.0.1:9092",
	}

	st[0] = NewState("127.0.0.1:9090", view[:2])
	st[1] = NewState("127.0.0.1:9091", view[:2])
	st[2] = NewState("127.0.0.1:9092", []string{})

	st[0].Route((srv[0].Handler).(*mux.Router))
	st[1].Route(srv[1].Handler.(*mux.Router))
	st[2].Route(srv[2].Handler.(*mux.Router))

	nkeys := 60
	for i := 0; i < nkeys; i++ {
		st[i%2].store.Set(fmt.Sprintf("key%x", i), "v")
	}

	for i := 0; i < 3; i++ {
		i := i
		go func() {
			log.Println(srv[i].ListenAndServe())
		}()
	}

	var res types.Response
	res.Status = 200
	st[2].viewChange(types.Input{
		View: strings.Join(view, ","),
	}, &res)
	if res.Status != 200 {
		t.Error("Non-200 code from view change")
	}

	for i := 0; i < 3; i++ {
		l := st[i].store.NumKeys()
		if l <= 0 || l >= (nkeys/3)+(nkeys/10) {
			t.Errorf("Wanted between about 1/3rd of keys in %d, got %d", i, l)
		}
		t.Logf("Store of %d has %d keys\n", i, l)
	}

	for i := 0; i < 3; i++ {
		st[i].store.For(func(key, value string) store.IterAction {
			target, _ := st[i].hash.Get(key)
			if target != st[i].address {
				t.Errorf("Key %q is on %q but should be on %q", key, st[i].address, target)
			}
			return store.CONTINUE
		})
	}

	for i := 0; i < 3; i++ {
		srv[i].Shutdown(context.Background())
	}
}
