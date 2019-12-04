package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

const (
	Alice = "127.0.0.1:9090"
	Bob   = "127.0.0.1:9091"
	Carol = "127.0.0.1:9092"
)

func ServerWithPort(port string) *http.Server {
	r := mux.NewRouter()
	r.Use(util.WithLog)
	return &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:" + port,
	}
}

func TestViewChange(t *testing.T) {
	// TODO fix the view change
	return

	var srv [3]*http.Server
	var st [3]*State
	srv[0] = ServerWithPort("9090")
	srv[1] = ServerWithPort("9091")
	srv[2] = ServerWithPort("9092")

	view := []string{
		Alice,
		Bob,
		Carol,
	}

	st[0] = NewState(Alice, view[:2], store.NopJournal())
	st[1] = NewState(Bob, view[:2], store.NopJournal())
	st[2] = NewState(Carol, []string{}, store.NopJournal())

	st[0].Route((srv[0].Handler).(*mux.Router))
	st[1].Route(srv[1].Handler.(*mux.Router))
	st[2].Route(srv[2].Handler.(*mux.Router))

	nkeys := 60
	for i := 0; i < nkeys; i++ {
		st[i%2].store.Write(clock.VectorClock{}, fmt.Sprintf("key%x", i), "v")
	}

	for i := 0; i < 3; i++ {
		i := i
		go func() {
			log.Println(srv[i].ListenAndServe())
		}()
	}

	var res types.Response
	res.Status = 200
	st[2].viewChange(types.Input{View: strings.Join(view, ",")}, &res)
	if res.Status != 200 {
		t.Error("Non-200 code from view change")
	}

	for i := 0; i < 3; i++ {
		_, l, _ := st[i].store.NumKeys(clock.VectorClock{})
		if l <= 0 || l >= (nkeys/3)+(nkeys/10) {
			t.Errorf("Wanted between about 1/3rd of keys in %d, got %d", i, l)
		}
		t.Logf("Store of %d has %d keys\n", i, l)
	}

	for i := 0; i < 3; i++ {
		st[i].store.For(func(key string, e store.Entry) store.IterAction {
			if e.Deleted {
				return store.CONTINUE
			}
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
