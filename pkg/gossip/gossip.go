package gossip

import (
	"fmt"
	"log"
	"net/http"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/store"

	"github.com/gorilla/mux"
)

type Manager struct {
	// stuff that the gossip manager needs to gossip
	state    *store.Store
	replicas []string
}

func NewManager(s *store.Store, replicas []string) *Manager {
	m := &Manager{
		state:    s,
		replicas: replicas,
	}
}

// gossips to other replicas periodically
func (m *Manager) Gossip() {

}

// finds stuff in the store to send to other replicas
func (m *Manager) gossip() {
	// loops through every key in the store
	for key, val := range m.state.store {
		// loop through key's vector clock
		for i, value := range val.vc {

		}
	}
}

func (m *Manager) Receive(w http.ResponseWriter, r *http.Request) {

}

func (m *Manager) Route() {

}
