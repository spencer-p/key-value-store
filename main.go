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
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
)

const (
	TIMEOUT = 10 * time.Minute
)

type Config struct {
	// Config VIEW and ADDRESS
	Port       string `envconfig:"PORT" required:"true"`
	View       string `envconfig:"VIEW" required:"true"`
	Address    string `envconfig:"ADDRESS" required:"true"`
	ReplFactor int    `envconfig:"REPL_FACTOR" required:"true"`
}

func main() {
	var env Config
	envconfig.MustProcess("", &env)
	log.Printf("Configured: %+v\n", env)

	// Create a cancelable context so we can kill processes
	ctx, cancel := context.WithCancel(context.Background())

	// Create a mux and route handlers
	r := mux.NewRouter()
	r.Use(util.WithLog)
	handlers.NewState(ctx, env.Address, types.View{
		Members:    strings.Split(env.View, ","),
		ReplFactor: env.ReplFactor,
	}).Route(r)

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

	cancel()
	srv.Shutdown(context.Background())
}
