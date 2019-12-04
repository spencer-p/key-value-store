package store

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

func NopJournal() chan<- Entry {
	return JournalFanout()
}
