package hash

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
)

// Modulo implements simple modulo hashing.
type Modulo struct {
	elts []string
	fnv  hash.Hash32
	mtx  sync.Mutex
}

func NewModulo() *Modulo {
	return &Modulo{
		elts: []string{},
		fnv:  fnv.New32(),
	}
}

func (m *Modulo) Get(key string) (string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.fnv.Reset()
	fmt.Fprintf(m.fnv, key)
	i := m.fnv.Sum32() % uint32(len(m.elts))
	return m.elts[i], nil

}
func (m *Modulo) Members() []string {
	return m.elts
}

func (m *Modulo) Set(elts []string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.elts = elts
}
