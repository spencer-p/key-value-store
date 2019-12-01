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
	log.Println("for", key, "got it")
	defer s.m.Unlock()

blocker:
	for {
		if len(tcausal) == 0 {
			log.Println("empty context. advancing history")
			tcausal = s.vc
		}

		switch tcausal.Compare(s.vc) {
		case clock.NoRelation:
			log.Printf("no compare between %v and %v\n", tcausal, s.vc)
			// TODO
			log.Println("ignoring no cmp.")
			return
		case clock.Less:
			log.Printf("incoming < current, ignoring: %v < %v", tcausal, s.vc)
			return
		case clock.Equal:
			log.Printf("can apply since %v = %v\n", tcausal, s.vc)
			break blocker // can apply
		case clock.Greater: //nop
			log.Printf("waiting for more events since %v > %v", tcausal, s.vc)
		}

		log.Println(key, "is waiting")
		s.vcCond.Wait()
		log.Println(key, "got a wakeup")
	}

	_, replaced = s.store[key]
	s.store[key] = value
	committed = true
	s.vc.Increment(s.addr)
	currentClock = s.vc
	s.vc = currentClock

	log.Println("committed", key)

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
