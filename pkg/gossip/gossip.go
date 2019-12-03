package gossip

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

type Manager struct {
	// stuff that the gossip manager needs to gossip
	state   *store.Store
	repFact int
	address string
}

type GossipPayload struct {
	KeyVals    map[string]*store.KeyInfo
	SenderAddr string
}

func newGossipPayload(senderAddr string) *GossipPayload {
	gp := &GossipPayload{
		KeyVals:    make(map[string]*store.KeyInfo),
		SenderAddr: senderAddr,
	}
	return gp
}

func NewManager(s *store.Store, address string, repFact int) *Manager {
	m := &Manager{
		state:   s,
		repFact: repFact,
		address: address,
	}
	return m
}

// gossips to other replicas periodically
func (m *Manager) relayGossip() {
	//defer result somewhere
	var result types.Response
	replicaPath := "/kv-store/gossip"

	for _, nodeAddr := range m.state.Replicas {
		if nodeAddr == m.address {
			continue
		}

		log.Println("Relay Gossip")
		gp := m.findGossip(nodeAddr)

		if len(gp.KeyVals) == 0 {
			return
		}

		jsonGossip, err := json.Marshal(gp)
		log.Println("Gossip payload:", jsonGossip)
		if err != nil {
			log.Fatalln("Failed to marshal GossipPayload")
		}

		target, err := url.Parse(util.CorrectURL(nodeAddr))
		if err != nil {
			log.Println("Bad gossip address", nodeAddr)
			continue
		}
		target.Path = path.Join(target.Path, replicaPath)

		request, err := http.NewRequest(http.MethodPut,
			target.String(),
			bytes.NewBuffer(jsonGossip))

		if err != nil {
			log.Println("Failed to delivery gossip to ", nodeAddr)
			continue
		}

		request.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(request)
		if err != nil {
			log.Println(err)
			continue
		}
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			log.Println("Could not parse gossip response:", err)
		}
	}
}

// finds stuff in the store to send to other replicas
func (m *Manager) findGossip(replicaAddress string) *GossipPayload {
	gp := newGossipPayload(m.address)

	log.Println("Find Gossip")
	// loop through all keys in the store
	for key, val := range m.state.Store {
		nodeClock := (*val.Vec)[m.address]
		replicaClock := (*val.Vec)[replicaAddress]
		if nodeClock > replicaClock {
			gp.KeyVals[key] = val
			(*val.Vec)[replicaAddress] = replicaClock + 1
		}
	}
	return gp
}

func (m *Manager) Receive(w http.ResponseWriter, r *http.Request) {
	log.Println("Received some gossip")
	var in GossipPayload
	dec := json.NewDecoder(r.Body)
	if r.ContentLength > 0 {
		if err := dec.Decode(&in); err != nil {
			log.Println("Could not decode gossip JSON:", err)
		}
	}
	log.Println(in)

	// loop through key-value pairs that were sent and apply updates
	for key, val := range in.KeyVals {
		log.Println("Going through this key...", key)
		m.state.SetGossip(key, m.address, in.SenderAddr, val)
	}
}

func (m *Manager) Gossip(ticker *time.Ticker) {
	for _ = range ticker.C {
		// t := time.Now()
		// log.Println("Relaying gossip at")
		m.relayGossip()
	}
}

func (m *Manager) Route(r *mux.Router) {

	r.HandleFunc("/kv-store/gossip", m.Receive).Methods(http.MethodPut)
}
