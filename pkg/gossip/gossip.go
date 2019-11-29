package gossip

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"path"

	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

type Manager struct {
	// stuff that the gossip manager needs to gossip
	state    *store.Store
	replicas []string
	repFact  int
	address  string
}

type KeyData struct {
	Value       string
	SenderClock uint64
}

type GossipPayload struct {
	KeyVals    map[string]*KeyData
	SenderAddr string
}

func newGossipPayload(senderAddr string) *GossipPayload {
	gp := &GossipPayload{
		KeyVals:    make(map[string]*KeyData),
		SenderAddr: senderAddr,
	}
	return gp
}

func newKeyData(value string, clock uint64) *KeyData {
	kd := &KeyData{
		Value:       value,
		SenderClock: clock,
	}
	return kd
}

func NewManager(s *store.Store, replicas []string, address string, repFact int) *Manager {
	m := &Manager{
		state:    s,
		replicas: replicas,
		repFact:  repFact,
		address:  address,
	}
	return m
}

// gossips to other replicas periodically
func (m *Manager) relayGossip() {

	var result types.Response
	replicaPath := "/kv-store/gossip"

	for _, nodeAddr := range m.replicas {
		if nodeAddr == m.address {
			continue
		}

		gp := m.findGossip(nodeAddr)

		jsonVector, err := json.Marshal(gp)
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
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			log.Println("Could not parse gossip response:", err)
		}
	}
}

// finds stuff in the store to send to other replicas
func (m *Manager) findGossip(replicaAddress string) *GossipPayload {
	gp := newGossipPayload(m.address)

	// loop through all keys in the store
	for key, val := range m.state.Store {
		nodeClock := (*val.Vec)[m.address]
		replicaClock := (*val.Vec)[replicaAddress]
		if nodeClock > replicaClock {
			kd := newKeyData(val.Value, nodeClock)
			gp.KeyVals[key] = kd
		}
	}

	return gp
}

func (m *Manager) Receive(w http.ResponseWriter, r *http.Request) {
	var in types.Input
	//params := mux.Vars(r)
	dec := json.NewDecoder(r.Body)
	if r.ContentLength > 0 {
		if err := dec.Decode(in); err != nil {
			log.Println("Could not decode gossip JSON:", err)
		}
	}

	log.Println(in)

}

func InitManager(r *mux.Router, repFact int, s *store.Store, replicas []string, address string) {
	m := NewManager(s, replicas, address, repFact)
	m.Route(r)
}

func (m *Manager) Route(r *mux.Router) {

	log.Println("Received some gossip")
	r.HandleFunc("/kv-store/gossip", m.Receive).Methods(http.MethodPut)
}
