package gossip

import (
	"log"
	"net/http"
	"net/url"
	"encoding/json"
	"bytes"
	"path"
	"mime/multipart"

	"github.com/spencer-p/cse138/pkg/store"
	"github.com/spencer-p/cse138/pkg/causal"
	"github.com/spencer-p/cse138/pkg/util"

	"github.com/gorilla/mux"
)

type Manager struct {
	// stuff that the gossip manager needs to gossip
	address string
	store *store.Store
	replicas []string
	vc *causal.VectorClock
}

func NewManager(s *store.Store) *Manager {
	m := &Manager{
		store: s,
	}
}
// gossips to other replicas periodically
func (m *Manager) relayGossip() {
	jsonVector, err := json.Marshal(m.vc)

	//defer result somewhere
	if err != nil {
		fmt.printf(err)
	}

	string replicaPath = "/kv-store/gossip"
	for nodeAddr, events := range len(m.vc)/*replication factor*/ {
		if (nodeAddr == m.address) {
			continue
		}
		target, err := url.Parse(util.CorrectURL(nodeAddr))
		if err != nil {
			log.Println("Bad gossip address", nodeAddr)
			continue
		}
		target.Path = path.Join(target.Path, replicaPath)

		request, err := http.NewRequest(http.Put,
		    target.string(),
				bytes.NewBuffer(jsonVector)

		if err != nil {
			log.Println("Failed to delivery gossip to ", nodeAddr)
			continue
		}

		request.Header.Set("Content-Type", multiPartWriter.FormDataContentType()) 
		
		client := &http.Client{}
		resp, err := client.Do(request)
		if err != nil {
			log.Fataln(err)
		}

		//write some ack response bullshit with the vector clock
	}
}

// finds stuff in the store to send to other replicas
func (m *Manager) findGossip() {

}

func (m *Manager) Receive(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	dec := json.NewDecoder(r.Body)
	if r.ContentLength > 0 {
		if err := dec.Dec
}

func (m *Manager) Route(r *mux.Router) {

	r.handlefunc("/kv-store/gossip", m.Receiver).methods(http.methodPut)
}
