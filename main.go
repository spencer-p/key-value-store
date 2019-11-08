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
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
)

const (
	TIMEOUT = 5 * time.Second
)

type Config struct {
	// Config VIEW and ADDRESS
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
	handlers.InitNode(r, env.Address, strings.Split(env.View, ","))

	port := strings.Split(env.Address, ":")
	srv := &http.Server{
		Handler:      r,
		Addr:         "0.0.0.0:" + port[1],
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
