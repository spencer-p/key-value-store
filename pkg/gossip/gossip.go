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

type GossipPayload struct {
	senderAddr    string             `json:"senderAddr,omitempty"`
	key           string             `json:"key,omitempty"`
	value         string             `json:"value,omitempty"`
	senderClock   *clock.VectorClock `json:"senderClock,omitempty"`
	receiverClock *clock.VectorClock `json:"receiverClock,omitempty"`
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
func (m *Manager) relayGossip( /*some map buffer?*/ ) {
	jsonVector, err := json.Marshal( /*stuff we return from find gossip*/ m.state.Store[m.address].Vec)
	var result types.Response
	//defer result somewhere
	if err != nil {
		fmt.Println(err)
	}

	replicaPath := "/kv-store/gossip"
	for _, nodeAddr := range m.replicas {
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
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			log.Println("Could not parse gossip response:", err)
		}

		return
		//write some ack response bullshit with the vector clock
	}
}

// finds stuff in the store to send to other replicas
func (m *Manager) findGossip() {
	gossip := &GossipPayload{}
	for key, val := range m.state.Store {
		// loop through key's vector clock
		nodeClock := (*val.Vec)[m.address] //is m.address supposed to be key
		for nodeAddr, count := range *val.Vec {
			if nodeAddr != m.address && nodeClock < count {
				gossip.key = key
				gossip.value = val.Value
				gossip.senderClock = val.Vec
				break
				// need to gossip
			}
		}
	}
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
