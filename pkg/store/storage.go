package store

import (
	"fmt"
	"log"
	"sync"

	"github.com/spencer-p/cse138/pkg/clock"
)

// Store represents a volatile key value store.

type KeyInfo struct {
	Value string
	Vec   *clock.VectorClock
}

type Store struct {
	Store    map[string]*KeyInfo
	Replicas []string
	m        sync.RWMutex
}

func NewKeyInfo(value string, replicas []string) *KeyInfo {
	clock := make(clock.VectorClock)
	for _, nodeAddr := range replicas {
		clock[nodeAddr] = 0
	}
	return &KeyInfo{
		Value: value,
		Vec:   &clock,
	}
}

// New constructs an empty store.
func New() *Store {
	return &Store{
		Store: make(map[string]*KeyInfo),
		// Note that the zero value for a mutex is unlocked.
	}
}

// set gossip applies the incoming gossip
func (s *Store) SetGossip(key, address, senderAddr string, senderInfo *KeyInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	old, updating := s.Store[key]

	// if key is up to date in store, don't apply gossip
	if updating && (*senderInfo.Vec)[senderAddr] <= (*s.Store[key].Vec)[address] {
		return
	}

	if updating {
		s.Store[key].Value = senderInfo.Value

		log.Printf("Set %q=%q from gossip", key, s.Store[key].Value)
		log.Printf("Old value was %q", old.Value)
	} else {
		// key DNE in receiver's store
		keyInfo := NewKeyInfo(senderInfo.Value, s.Replicas)
		s.Store[key] = keyInfo

		log.Printf("Set %q=%q from gossip", key, s.Store[key].Value)
	}

	// merge the sender's vector clock into the receiving node's vector clock
	for nodeAddr, clock := range *s.Store[key].Vec {
		if nodeAddr == address {
			continue
		}
		senderClock := (*senderInfo.Vec)[nodeAddr]
		if clock < senderClock {
			(*s.Store[key].Vec)[nodeAddr] = senderClock
		}
	}
	(*s.Store[key].Vec)[address] = (*senderInfo.Vec)[senderAddr]

	return
}

// Set sets key=value and returns true iff the value replaced an old value.
func (s *Store) Set(key, value, address string) bool {
	s.m.Lock()
	defer s.m.Unlock()

	old, updating := s.Store[key]

	if updating {
		s.Store[key].Value = value

		log.Printf("Set %q=%q", key, value)
		log.Printf("Old value was %q", old.Value)
	} else {
		// new key, create KeyInfo object, VectorClock starts at 0 for all nodes
		keyInfo := NewKeyInfo(value, s.Replicas)
		s.Store[key] = keyInfo

		log.Printf("Set %q=%q", key, value)
	}
	s.Store[key].Vec.Increment(address)

	return updating
}

// Delete removes a key.
func (s *Store) Delete(key string) {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.Store, key)

	log.Printf("Deleted %q\n", key)
}

// Read returns the value for a key in the Store.
func (s *Store) Read(key string) (string, bool) {
	s.m.RLock()
	defer s.m.RUnlock()

	value := s.Store[key].Value

	var ok = false
	if value != "" {
		ok = true
	}
	log.Printf("Reading %q=%q\n", key, value)

	return value, ok
}

// NumKeys returns the number of keys in the store.
func (s *Store) NumKeys() int {
	s.m.RLock()
	defer s.m.RUnlock()

	return len(s.Store)
}

func (s *Store) String() string {
	s.m.RLock()
	defer s.m.RUnlock()
	return fmt.Sprintf("%+v", s.Store)
}
