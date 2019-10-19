package main

import (
	"fmt"
	"encoding/json"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"io/ioutil"
	"os/signal"
	"syscall"
	"time"

	"github.com/spencer-p/cse138/pkg/follower"
	"github.com/spencer-p/cse138/pkg/leader"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
)

type event struct {
        KEY     string `json:KEY`
	VALUE   string `json:VALUE`
}

type allEvents []event

var events = allEvents{}

const (
	TIMEOUT = 5 * time.Second
)

type Config struct {
	// Port to serve HTTP on
	Port string `envconfig:"PORT" required:"true"`

	// If empty, this is the main instance.
	// Otherwise, act as a proxy to the address provided.
	ForwardingAddr string `envconfig:"FORWARDING_ADDRESS"`
}

func putRequest(w http.ResponseWriter, r *http.Request) {
	eventKEY := mux.Vars(r)["KEY"]
	var putRequestEvent event

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "invalid format error")
  }
	json.Unmarshal(reqBody, &putRequestEvent)

	for i, singleEvent := range events {
		if singleEvent.KEY == eventKEY {
			singleEvent.KEY = putRequestEvent.KEY
			singleEvent.VALUE = putRequestEvent.VALUE
			events = append(events[:i], singleEvent)
			json.NewEncoder(w).Encode(singleEvent)
			return
		}
	}

  fmt.Fprintf(w, "New event added through put is key: %s\n", putRequestEvent.Key)

	events = append(events, putRequestEvent)
	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(putRequestEvent)

}

func main() {
	var env Config
	envconfig.MustProcess("", &env)
	log.Printf("Configured: %+v\n", env)

	// Create a mux and route handlers
	r := mux.NewRouter()
	r.Use(util.WithLog)

  r.HandleFunc("/", putRequest).Methods("PUT")
  /*
	r.HandleFunc("/{KEY}", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("TODO"))
	})*/
	if env.ForwardingAddr == "" {
		log.Println("Configured as a main instance")
		leader.Route(r)
	} else {
		log.Println("Configured as a follower")
		follower.Route(r, env.ForwardingAddr)
	}

	srv := &http.Server{
		Handler:      r,
		Addr:         "0.0.0.0:" + env.Port,
		ReadTimeout:  TIMEOUT,
		WriteTimeout: TIMEOUT,
	}

	// Run the server watching for errors
	go func() {
		log.Println("Starting server")
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// Wait for signals to stop the server
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	log.Println("Shutdown signal received, exiting...")

	srv.Shutdown(context.Background())
}
