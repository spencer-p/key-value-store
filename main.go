package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
)

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

func main() {
	var env Config
	envconfig.MustProcess("", &env)
	log.Printf("Configured: %+v\n", env)

	// Create a mux and route a handler
	r := mux.NewRouter()
	r.Use(util.WithLog)
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("TODO"))
	})

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
