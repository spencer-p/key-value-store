package gossip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

type Manager struct {
	// stuff that the gossip manager needs to gossip
	vc       *clock.VectorClock
	store    *store.Store
	replicas []string
	address  string
}

func NewManager(s *store.Store, replicas []string) *Manager {
	m := &Manager{
		store:    s,
		replicas: replicas,
	}
	return m
}

// gossips to other replicas periodically
func (m *Manager) relayGossip() {
	jsonVector, err := json.Marshal(m.vc)

	//defer result somewhere
	if err != nil {
		fmt.Println(err)
	}

	replicaPath := "/kv-store/gossip"
	for nodeAddr, events := range len(m.vc) {
		if nodeAddr == m.address {
			continue
		}
		target, err := url.Parse(util.CorrectURL(nodeAddr))
		if err != nil {
			log.Println("Bad gossip address", nodeAddr)
			continue
		}
		target.Path = path.Join(target.Path, replicaPath)

		request, err := http.NewRequest(http.MethodPut,
			target.String(),
			bytes.NewBuffer(jsonVector))

		if err != nil {
			log.Println("Failed to delivery gossip to ", nodeAddr)
			continue
		}

		request.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(request)
		if err != nil {
			log.Fatalln(err)
		}

		//write some ack response bullshit with the vector clock
	}
}

// finds stuff in the store to send to other replicas
func (m *Manager) findGossip() {
	// loops through every key in the store
	for key, val := range m.store {
		// loop through key's vector clock
		for i, value := range m.store.vc {
			if value.vc == m.store.vc {
				//add it to the vector clock
			}
		}
	}
}

func (m *Manager) Receive(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	// dec := json.NewDecoder(r.Body)
	// if r.ContentLength > 0 {
	// 	if err := dec.Dec

}

func (m *Manager) Route(r *mux.Router) {

	r.HandleFunc("/kv-store/gossip", m.Receive).Methods(http.MethodPut)
}
