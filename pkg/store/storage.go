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

// Entry describes a full data point in the store.
// The value is required. Other fields are semi-optional depending on the context.
type Entry struct {
	Key, Value string
	Deleted    bool
	Clock      clock.VectorClock
}

// Store represents a volatile key value store.
type Store struct {
	addr    string
	shard   []string
	store   map[string]Entry
	m       *sync.RWMutex
	vc      clock.VectorClock
	vcCond  *sync.Cond
	journal chan<- Entry
}

// New constructs an empty store that resides at the given address or unique ID.
// The callback channel is issued all new modifications to the store (like a journal).
func New(selfAddr string, shard []string, callback chan<- Entry) *Store {
	var mtx sync.RWMutex
	return &Store{
		addr:    selfAddr,
		shard:   shard,
		store:   make(map[string]Entry),
		m:       &mtx,
		vc:      clock.VectorClock{},
		vcCond:  sync.NewCond(&mtx),
		journal: callback,
	}
}

// Write performs a new write to the store. It will block until the write can be applied
// according to the vector clock passed.
func (s *Store) Write(tcausal clock.VectorClock, key, value string) (
	err error,
	replaced bool,
	currentClock clock.VectorClock) {

	// Acquire access to the store
	s.m.Lock()
	defer s.m.Unlock()

	// Remember to copy the clock before returning
	defer s.copyClock(&currentClock)

	// Wait for a state that can accept our write
	if err = s.waitUntilCurrent(tcausal); err != nil {
		return
	}

	// Perform the write
	replaced = s.commitWrite(key, Entry{
		Value:   value,
		Deleted: false,
	})
	return
}

// ImportEntry imports an existing entry from another store.
func (s *Store) ImportEntry(key string, e Entry) error {
	s.m.Lock()
	defer s.m.Unlock()

	if err := s.waitForGossip(e.Clock); err != nil {
		return err
	}

	s.vc.Max(e.Clock)
	s.commitWrite(key, e)

	return nil
}

// Delete deletes a key, returning true if it was deleted.
func (s *Store) Delete(tcausal clock.VectorClock, key string) (
	err error,
	deleted bool,
	currentClock clock.VectorClock) {
	s.m.Lock()
	defer s.m.Unlock()
	defer s.copyClock(&currentClock)
	if err = s.waitUntilCurrent(tcausal); err != nil {
		return
	}

	// Don't perform a delete on a key/value that doesn't exist
	entry, exists := s.store[key]
	if !exists || entry.Deleted {
		return
	}

	// Perform the delete if we have the object
	deleted = s.commitWrite(key, Entry{Deleted: true})
	return
}

func (s *Store) commitWrite(key string, e Entry) (replaced bool) {
	// Check if the entry previously existed
	oldentry, exists := s.store[key]
	replaced = exists && oldentry.Deleted != true

	// Update the clock in anticipation of the event
	s.vc.Increment(s.addr)
	s.vcCond.Broadcast() // let others know this update happened once we release the lock

	// Mark the clock
	e.Clock = s.vc.Copy()

	// Double check the key is good
	if e.Key == "" {
		e.Key = key
	}

	// Perform the write
	s.store[key] = e
	if !e.Deleted {
		log.Printf("Committed %q=%q at t=%v\n", key, e.Value, s.vc)
	} else {
		log.Printf("Committed delete of %q at t=%v\n", key, s.vc)
	}

	// Make a copy that cannot interact with the store's copy;
	// then send it to the journal
	e.Clock = e.Clock.Copy()
	s.journal <- e

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
	defer s.copyClock(&currentClock)

	if err = s.waitUntilCurrent(tcausal); err != nil {
		return
	}

	// Perform the read. Act like it doesn't exist if it was deleted.
	e, ok = s.store[key]
	if e.Deleted {
		ok = false
	}
	return
}

// NumKeys returns the number of keys in the store.
func (s *Store) NumKeys(tcausal clock.VectorClock) (
	err error,
	count int,
	currentClock clock.VectorClock) {
	s.m.Lock()
	defer s.m.Unlock()
	defer s.copyClock(&currentClock)

	if err = s.waitUntilCurrent(tcausal); err != nil {
		return
	}

	// count only not deleted keys
	for k := range s.store {
		if !s.store[k].Deleted {
			count += 1
		}
	}
	return
}

// SetShard replaces the current shard member list this store thinks it is on.
func (s *Store) SetShard(members []string) {
	s.m.Lock()
	defer s.m.Unlock()
	s.shard = members
}

func (s *Store) String() string {
	s.m.Lock()
	defer s.m.Unlock()
	return fmt.Sprintf("%v %+v", s.vc, s.store)
}

// waitUntilCurrent returns a function that stalls until the waiting vector
// clock is not causally from the future.  the write mutex must be held on the
// store.
func (s *Store) waitUntilCurrent(incoming clock.VectorClock) error {
	incoming = incoming.Subset(s.shard)
	for {
		// As long as this clock is not from the future, we can apply it.
		if cmp := incoming.Compare(s.vc.Subset(s.shard)); cmp != clock.Greater {
			return nil
		}
		s.vcCond.Wait()
	}
}

func (s *Store) waitForGossip(incoming clock.VectorClock) error {
	incoming = incoming.Subset(s.shard)
	for {
		if canApply, _ := incoming.OneUpExcept(s.addr, s.vc.Subset(s.shard)); canApply {
			// One atomic update from another node.
			return nil
		}
		s.vcCond.Wait()
	}
}

func (s *Store) copyClock(c *clock.VectorClock) {
	*c = s.vc.Copy()
}
