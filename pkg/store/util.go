package store

// JournalFanout returns a single journal channel that will play journaled
// entries onto many channels passed to it.
func JournalFanout(channels ...chan<- Entry) chan<- Entry {
	j := make(chan Entry, len(channels))
	go func() {
		var e Entry
		for {
			e = <-j
			for i := range channels {
				channels[i] <- e
			}
		}
	}()
	return j
}

// NopJournal returns a journal channel that consumes entries and does nothing.
func NopJournal() chan<- Entry {
	return JournalFanout()
}
