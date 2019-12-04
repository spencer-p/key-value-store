package hash

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"sync"

	"github.com/spencer-p/cse138/pkg/util"
)

var (
	ErrNoElements = errors.New("No elements to hash to")
)

// Modulo implements simple modulo hashing.
type Modulo struct {
	elts   []string
	scount uint32
	fnv    hash.Hash32
	mtx    sync.Mutex // TODO Is this lock necessary?
}

func NewModulo() *Modulo {
	return &Modulo{
		elts:   []string{},
		scount: 0,
		fnv:    fnv.New32(),
	}
}

// Get returns the address of the node that should store the given key.
func (m *Modulo) Get(key string) (string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	n := uint32(len(m.elts))
	if n == 0 {
		return "", ErrNoElements
	}

	m.fnv.Reset()
	fmt.Fprintf(m.fnv, key)
	i := m.fnv.Sum32()
	return m.getAddress(i), nil
}

func (m *Modulo) getAddress(h uint32) string {
	perShard := uint32(len(m.elts)) / m.scount

	shardIndex := h % m.scount
	replIndex := h % perShard
	log.Printf("replica %d in shard %d\n", replIndex, shardIndex)

	index := shardIndex*perShard + replIndex

	if index >= uint32(len(m.elts)) {
		index = uint32(len(m.elts)) - 1
	}

	return m.elts[index]
}

// Members returns the list of nodes in this hash.
func (m *Modulo) Members() []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.elts
}

// Set atomically assigns the member nodes to its arguments.
func (m *Modulo) Set(elts []string, repFact int) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.elts = elts
	m.scount = uint32(len(elts) / repFact)
}

// Test and set performs an atomic Set operation iff the new member slice is
// different than the old. Returns true if the member slice changed.
func (m *Modulo) TestAndSet(elts []string) bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	viewIsNew := !eltsEqual(elts, m.elts)
	if viewIsNew {
		m.elts = elts
	}
	return viewIsNew
}

// eltsEqual returns true iff the elts are the same set-wise.
func eltsEqual(e1 []string, e2 []string) bool {
	s1 := util.StringSet(e1)
	s2 := util.StringSet(e2)

	return util.SetEqual(s1, s2)
}
