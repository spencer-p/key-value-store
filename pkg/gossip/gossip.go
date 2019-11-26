package gossip

import (
	"log"
	"net/http"
	"time"

	"github.com/spencer-p/cse138/pkg/store"

	"github.com/gorilla/mux"
)

type Manager struct {
	// stuff that the gossip manager needs to gossip
	store *store.Store
}

func NewManager(s map[string]string) *Manager {
	m := &Manager{
		store: s,
	}
}

// gossips to other replicas periodically
func (m *Manager) relayGossip() {
	timer := time.NewTime(2 * time.Second)

}

// finds stuff in the store to send to other replicas
func (m *Manager) findGossip() {

}

func (m *Manager) Receive(w http.ResponseWriter, r *http.Request) {

}

func (m *Manager) Route() {

}
