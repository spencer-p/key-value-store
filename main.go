package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spencer-p/cse138/pkg/handlers"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
)

const (
	TIMEOUT = 10 * time.Minute
)

type Config struct {
	// Config VIEW and ADDRESS
	Port    string `envconfig:"PORT" required:"true"`
	View    string `envconfig:"VIEW" required:"true"`
	Address string `envconfig:"ADDRESS" required:"true"`
}

func main() {
	var env Config
	envconfig.MustProcess("", &env)
	log.Printf("Configured: %+v\n", env)

	// Create a mux and route handlers
	r := mux.NewRouter()
	r.Use(util.WithLog)
	// TODO this is what i would hope to see:
	// 1. make some sort of gossip manager
	// 2. ask the gossip manager for a channel we can send to
	// 3. give that channel to the handlers
	journal := make(chan store.Entry, 10)
	go func() { log.Println("Journaling", <-journal) }()
	handlers.InitNode(r, env.Address, strings.Split(env.View, ","), journal)

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
