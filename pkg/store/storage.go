package store

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/spencer-p/cse138/pkg/clock"
)

var (
	ErrCannotApply = errors.New("Cannot apply operation")
)

type Entry struct {
	Value   string
	Clock   clock.VectorClock
	Deleted bool
}

// Store represents a volatile key value store.
type Store struct {
	addr   string
	store  map[string]Entry
	m      *sync.RWMutex
	vc     clock.VectorClock
	vcCond *sync.Cond
}

// New constructs an empty store.
func New(selfAddr string) *Store {
	var mtx sync.RWMutex
	return &Store{
		addr:   selfAddr,
		store:  make(map[string]Entry),
		m:      &mtx,
		vc:     clock.VectorClock{},
		vcCond: sync.NewCond(&mtx),
	}
}

func (s *Store) Write(tcausal clock.VectorClock, key, value string) (
	err error,
	replaced bool,
	currentClock clock.VectorClock) {

	// Acquire access to the store
	s.m.Lock()
	defer s.m.Unlock()

	// Wait for a state that can accept our write
	if err = s.waitUntilCurrent(tcausal); err != nil {
		return
	}

	// Perform the write
	replaced = s.commitWrite(key, Entry{
		Value:   value,
		Deleted: false,
	})
	currentClock = s.vc.Copy()
	return
}

func (s *Store) ImportEntry(key string, e Entry) error {
	return nil
}

func (s *Store) commitWrite(key string, e Entry) (replaced bool) {
	// Check if the entry previously existed
	oldentry, exists := s.store[key]
	replaced = exists && oldentry.Deleted != true

	// Update the clock in anticipation of the event
	s.vc.Increment(s.addr)
	s.vcCond.Broadcast() // let others know this update happened once we release the lock

	// Perform the write
	e.Clock = s.vc.Copy()
	s.store[key] = e
	log.Printf("Committing %q=%q at t=%v\n", key, e.Value, s.vc)

	return replaced
}

// Read returns the value for a key in the Store.
func (s *Store) Read(tcausal clock.VectorClock, key string) (
	err error,
	e Entry,
	ok bool,
	currentClock clock.VectorClock) {

	s.m.Lock()
	defer s.m.Unlock()

	if err = s.waitUntilCurrent(tcausal); err != nil {
		return
	}

	e, ok = s.store[key]
	currentClock = s.vc.Copy()
	return
}

// NumKeys returns the number of keys in the store.
func (s *Store) NumKeys(tcausal clock.VectorClock) (error, int) {
	s.m.Lock()
	defer s.m.Unlock()

	return nil, len(s.store)
}

func (s *Store) String() string {
	s.m.Lock()
	defer s.m.Unlock()
	return fmt.Sprintf("%+v", s.store)
}
