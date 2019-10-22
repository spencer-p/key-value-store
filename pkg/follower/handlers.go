// Package leader implements handlers for follower instances.
package follower

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
  "io/ioutil"
  //"encoding/json"
  "bytes"

	"github.com/gorilla/mux"
)

const (
	TIMEOUT = 5 * time.Second
)

// TODO adam, vineet ?

// follower holds all state that a follower needs to operate.
type follower struct {
	client http.Client
	addr   *url.URL
}

func (f *follower) indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "My forwarding address is %s", f.addr)

  requestBody, err := ioutil.ReadAll(r.Body)
  if err != nil {
    log.Println("error")
    return
  }

  r.Body = ioutil.NopCloser(bytes.NewReader(requestBody))

  request, err := http.NewRequest(r.Method, "http://" + f.addr.String() + "/kv-store/{key:.*}", bytes.NewBuffer(requestBody))

  if err != nil {
    log.Fatalln(err)
  }

  request.Header = make(http.Header)

  for key, value := range r.Header {
      request.Header[key] = value
  }

  resp, err := f.client.Do(request)

  if err != nil {
    log.Fatalln(err)
  }

  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Fatalln(err)
  }

  log.Println(string(body))

  // TODO i think use http.NewRequest and f.client.Do
}

func Route(r *mux.Router, fwd string) {
	addr, err := url.Parse(fwd)
	if err != nil {
		// TODO return an error instead of fataling
		log.Fatalf("Bad forwarding address %q: %v\n", fwd, addr)
	}

	f := follower{
		client: http.Client{
			Timeout: TIMEOUT,
		},
		addr: addr,
	}

	r.PathPrefix("/").Handler(http.HandlerFunc(f.indexHandler))
}
