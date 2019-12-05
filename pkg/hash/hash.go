package hash

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"math/rand"
	"sync"

	"github.com/spencer-p/cse138/pkg/types"
	"github.com/spencer-p/cse138/pkg/util"
)

var (
	ErrNoElements = errors.New("No elements to hash to")
)

// Hash implements simple modulo hashing.
type Hash struct {
	elts       []string
	replFactor int
	fnv        hash.Hash32
	mtx        sync.Mutex // TODO Is this lock necessary?
}

func New(view types.View) *Hash {
	return &Hash{
		elts:       view.Members,
		fnv:        fnv.New32(),
		replFactor: view.ReplFactor,
	}
}

// Get returns the address of the node that should store the given key.
func (m *Hash) Get(key string) (string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	n := len(m.elts)
	if n == 0 {
		return "", ErrNoElements
	}

	m.fnv.Reset()
	fmt.Fprintf(m.fnv, key)
	i := int(m.fnv.Sum32())
	shardCount := n / m.replFactor
	shardId := i % shardCount
	replicaId := i % m.replFactor
	return m.elts[m.replFactor*shardId+replicaId], nil
}

func (m *Hash) GetAny(key string) (string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	n := len(m.elts)
	if n == 0 {
		return "", ErrNoElements
	}

	m.fnv.Reset()
	fmt.Fprintf(m.fnv, key)
	i := int(m.fnv.Sum32())
	shardCount := n / m.replFactor
	shardId := i % shardCount
	replicaId := rand.Intn(m.replFactor)
	return m.elts[m.replFactor*shardId+replicaId], nil
}

// GetReplicas returns the set of members in the given shard.
func (m *Hash) GetReplicas(id int) []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	res := make([]string, m.replFactor)
	copy(res, m.elts[id*m.replFactor:(id+1)*m.replFactor])
	return res
}

// GetShardId returns the shard ID of the given member.
func (m *Hash) GetShardId(member string) int {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	i := 0
	for ; i < len(m.elts); i++ {
		if m.elts[i] == member {
			break
		}
	}
	return i / m.replFactor
}

// Members returns the list of nodes in this hash.
func (m *Hash) Members() []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.elts
}

// Test and set performs an atomic Set operation iff the new member slice is
// different than the old. Returns true if the member slice changed.
func (m *Hash) TestAndSet(view types.View) bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	viewIsNew := !eltsEqual(view.Members, m.elts) || view.ReplFactor != m.replFactor
	if viewIsNew {
		m.elts = view.Members
		m.replFactor = view.ReplFactor
	}
	return viewIsNew
}

// eltsEqual returns true iff the elts are the same set-wise.
func eltsEqual(e1 []string, e2 []string) bool {
	s1 := util.StringSet(e1)
	s2 := util.StringSet(e2)

	return util.SetEqual(s1, s2)
}
