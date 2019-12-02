package store

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/spencer-p/cse138/pkg/clock"
)

// Store represents a volatile key value store.
type Store struct {
	addr   string
	store  map[string]string
	m      *sync.RWMutex
	vc     clock.VectorClock
	vcCond *sync.Cond
}

// New constructs an empty store.
func New(selfAddr string) *Store {
	var mtx sync.RWMutex
	return &Store{
		addr:   selfAddr,
		store:  make(map[string]string),
		m:      &mtx,
		vc:     clock.VectorClock{},
		vcCond: sync.NewCond(&mtx),
	}
}

func (s *Store) Write(ctx context.Context, tcausal clock.VectorClock, key, value string) (
	committed bool,
	replaced bool,
	currentClock clock.VectorClock) {

	log.Println("for", key, "get write mtx..")
	s.m.Lock()
	defer s.m.Unlock()
	log.Println("for", key, "got it")

	fromOtherNode := false
	otherNode := ""

	for {
		if len(tcausal) == 0 {
			log.Println("empty context. advancing requester's history")
			tcausal = s.vc
			break
		} else if comp := tcausal.Compare(s.vc); comp == clock.Equal {
			log.Printf("can apply since %v == %v", tcausal, s.vc)
			break
		} else if fromOtherNode, otherNode = tcausal.OneUpExcept(s.addr, s.vc); fromOtherNode {
			log.Printf("can apply since %v +1 == %v", tcausal, s.vc)
			break
		}

		log.Println(key, "is waiting with clock", tcausal)
		s.vcCond.Wait()
		log.Println(key, "got a wakeup")
	}

	_, replaced = s.store[key]
	s.store[key] = value
	committed = true
	s.vc.Increment(s.addr)
	if fromOtherNode {
		s.vc.Increment(otherNode)
	}

	currentClock = s.vc
	s.vc = currentClock

	log.Println("committed", key, "and the clock is", s.vc)

	s.vcCond.Broadcast()
	return
}

// Set sets key=value and returns true iff the value replaced an old value.
func (s *Store) Set(key, value string) bool {
	s.m.Lock()
	defer s.m.Unlock()

	old, updating := s.store[key]
	s.store[key] = value

	log.Printf("Set %q=%q", key, value)
	if updating {
		log.Printf("Old value was %q", old)
	}

	return updating
}

// Delete removes a key.
func (s *Store) Delete(key string) {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.store, key)

	log.Printf("Deleted %q\n", key)
}

// Read returns the value for a key in the Store.
func (s *Store) Read(key string) (string, bool) {
	s.m.RLock()
	defer s.m.RUnlock()

	value, ok := s.store[key]

	log.Printf("Reading %q=%q\n", key, value)

	return value, ok
}

// NumKeys returns the number of keys in the store.
func (s *Store) NumKeys() int {
	s.m.RLock()
	defer s.m.RUnlock()

	return len(s.store)
}

func (s *Store) String() string {
	s.m.RLock()
	defer s.m.RUnlock()
	return fmt.Sprintf("%+v", s.store)
}
