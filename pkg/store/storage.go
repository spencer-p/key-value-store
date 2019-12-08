package store

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/spencer-p/cse138/pkg/clock"
	"github.com/spencer-p/cse138/pkg/uuid"
)

var (
	ErrCannotApply = errors.New("Cannot apply operation")
)

// Entry describes a full data point in the store.
// The value is required. Other fields are semi-optional depending on the context.
type Entry struct {
	Key     string            `json:"key"`
	Value   string            `json:"value"`
	Deleted bool              `json:"deleted"`
	Clock   clock.VectorClock `json:"clock"`
	Version uuid.UUID         `json:"version"`
	//NodeHistory map[string]bool   `json:"history"`
}

type Store struct {
	addr     string
	replicas []string
	store    map[string]Entry
	m        *sync.RWMutex
	vc       clock.VectorClock
	vcCond   *sync.Cond
	journal  chan<- Entry
	version  uuid.UUID
}

// New constructs an empty store that resides at the given address or unique ID.
// The callback channel is issued all new modifications to the store (like a journal).
func New(selfAddr string, replicas []string, callback chan<- Entry) *Store {
	var mtx sync.RWMutex
	return &Store{
		addr:     selfAddr,
		replicas: replicas[:],
		store:    make(map[string]Entry),
		m:        &mtx,
		vc:       clock.VectorClock{},
		vcCond:   sync.NewCond(&mtx),
		journal:  callback,
		version:  uuid.New(selfAddr),
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
	s.vc.Max(tcausal)
	s.version = s.version.Next()
	replaced = s.commitWrite(Entry{
		Key:     key,
		Value:   value,
		Deleted: false,
		Version: s.version,
	}, true)
	return
}

// ImportEntry imports an existing entry from another store.
func (s *Store) ImportEntry(e Entry) (imported bool, err error) {
	s.m.Lock()
	defer s.m.Unlock()

	// If we already have it, we are good
	if existing, ok := s.store[e.Key]; ok && existing.Version.Equal(e.Version) {
		log.Printf("Import of %q already exists on this node. ACKing", e.Key)
		return true, nil
	}

	if err = s.waitForGossip(e.Clock); err != nil {
		return false, err
	}

	// If we already have it, we are good
	if existing, ok := s.store[e.Key]; ok && existing.Version.Equal(e.Version) {
		log.Printf("Import of %q already exists on this node. ACKing", e.Key)
		return true, nil
	}

	// if i receive gossip. from the past.  and i do not have a more recent
	// entry for said entry.  then.  i may. commit. said entry.
	if e.Clock.Subset(s.replicas).Compare(s.vc.Subset(s.replicas)) == clock.Less {
		if existing, ok := s.store[e.Key]; ok {
			if !(existing.Clock.Subset(s.replicas).Compare(s.vc.Subset(s.replicas)) != clock.Greater) {
				return false, nil
			}
		}
	}

	s.vc.Max(e.Clock)
	s.commitWrite(e, false)

	return true, nil
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
	s.vc.Max(tcausal)
	s.version = s.version.Next()
	deleted = s.commitWrite(Entry{Key: key, Deleted: true, Version: s.version}, true)
	return
}

func (s *Store) commitWrite(e Entry, shouldJournal bool) (replaced bool) {
	// Check if the entry previously existed
	oldentry, exists := s.store[e.Key]
	replaced = exists && oldentry.Deleted != true

	// Update the clock & version in anticipation of the event
	s.vc.Increment(s.addr)
	s.vcCond.Broadcast() // let others know this update happened once we release the lock

	// Mark the clock
	e.Clock = s.vc.Copy()
	/*if e.NodeHistory == nil {
		e.NodeHistory = make(map[string]bool)
	}
	e.NodeHistory[s.addr] = true*/

	// Perform the write
	s.store[e.Key] = e
	if !e.Deleted {
		log.Printf("Committed %q=%q at t=%v\n", e.Key, e.Value, s.vc)
	} else {
		log.Printf("Committed delete of %q at t=%v\n", e.Key, s.vc)
	}

	// send the update to the journal
	if shouldJournal {
		s.journal <- e
	}

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

// SetReplicas replaces the current replicas list this store thinks it is on.
func (s *Store) SetReplicas(nodes []string) {
	s.m.Lock()
	defer s.m.Unlock()
	s.replicas = nodes[:]
}

// BumpClockForNode informs the store that another node has processed an event of ours.
func (s *Store) BumpClockForNode(node string) {
	s.m.Lock()
	defer s.m.Unlock()
	s.vc.Increment(node)
	s.vcCond.Broadcast()
}

func (s *Store) String() string {
	s.m.Lock()
	defer s.m.Unlock()
	return fmt.Sprintf("%v %+v", s.vc, s.store)
}

// AllEntries returns a slice of all entries in this store.
func (s *Store) AllEntries() []Entry {
	s.m.Lock()
	defer s.m.Unlock()
	entries := make([]Entry, len(s.store))
	i := 0
	for _, e := range s.store {
		entries[i] = e
		i++
	}
	return entries
}

// ReplaceEntries atomically replaces all the entries with the given slice.
func (s *Store) ReplaceEntries(entries []Entry) {
	s.m.Lock()
	defer s.m.Unlock()
	s.store = make(map[string]Entry)
	s.vc = clock.VectorClock{}
	for _, e := range entries {
		s.store[e.Key] = e
		s.vc.Max(e.Clock)
	}
}

// Clock returns the current vector clock.
func (s *Store) Clock() clock.VectorClock {
	s.m.Lock()
	defer s.m.Unlock()
	return s.vc.Copy()
}

// WaitUntilCurrent stalls until the waiting vector is not from the future.
func (s *Store) WaitUntilCurrent(incoming clock.VectorClock) error {
	s.m.Lock()
	defer s.m.Unlock()
	return s.waitUntilCurrent(incoming)
}

// waitUntilCurrent stalls until the waiting vector clock is not causally from
// the future.  the write mutex must be held on the store.
func (s *Store) waitUntilCurrent(incoming clock.VectorClock) error {
	incoming = incoming.Subset(s.replicas)
	for {
		// As long as this clock is not from the future, we can apply it.
		if cmp := incoming.Compare(s.vc.Subset(s.replicas)); cmp != clock.Greater {
			return nil
		}
		log.Printf("Current shard clock is %v, incoming is at %v", s.vc.Subset(s.replicas), incoming)
		s.vcCond.Wait()
	}
}

func (s *Store) waitForGossip(incoming clock.VectorClock) error {
	incoming = incoming.Subset(s.replicas)
	for {
		subs := s.vc.Subset(s.replicas)
		if canApply, _ := incoming.OneUpExcept(s.addr, subs); canApply {
			// One atomic update from another node.
			return nil
		} else if cmp := incoming.Compare(subs); cmp != clock.Greater {
			// This gossip is not from the future! we might be able to apply it.
			return nil
		}
		log.Printf("Current shard clock is %v, gossip is at %v", s.vc.Subset(s.replicas), incoming)
		s.vcCond.Wait()
	}
}

func (s *Store) copyClock(c *clock.VectorClock) {
	*c = s.vc.Copy()
}
