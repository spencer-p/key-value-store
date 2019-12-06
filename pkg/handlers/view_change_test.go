package handlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

const (
	Alice = "127.0.0.1:9090"
	Bob   = "127.0.0.1:9091"
	Carol = "127.0.0.1:9092"
)

var (
	ctx = context.Background()
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
	// TODO Test the view change.
}
