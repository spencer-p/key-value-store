package hash

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"sync"
)

var (
	ErrNoElements = errors.New("No elements to hash to")
)

// Modulo implements simple modulo hashing.
type Modulo struct {
	elts []string
	h    hash.Hash32
	mtx  sync.Mutex // TODO Is this lock necessary?
}

func NewModulo() *Modulo {
	return &Modulo{
		elts: []string{},
		h:    crc32.New(crc32.IEEETable),
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

	m.h.Reset()
	fmt.Fprintf(m.h, key)
	i := m.h.Sum32() % n
	return m.elts[i], nil

}

// Members returns the list of nodes in this hash.
func (m *Modulo) Members() []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.elts
}

// Set atomically assigns the member nodes to its arguments.
func (m *Modulo) Set(elts []string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.elts = elts
}
