package asyncdb

import (
	"sync"
	"time"
)

type KeyExistsError struct{}
type KeyNotExistsError struct{}
type TickerCB struct {
	t    *time.Ticker
	done chan bool
	cb   func(chan time.Time, chan bool)
}
type AsyncDB[K comparable, V any] struct {
	m        sync.Map
	g_mutex  sync.RWMutex // GLOBAL lock for the db map
	tickerCB *TickerCB
}

type truthyFunc[V any] func(V) bool // helper function for SELECT, DELETE

func (m *KeyExistsError) Error() string {
	return "Key exists in database"
}

func (m *KeyNotExistsError) Error() string {
	return "Key does not exist in database"
}

func MakeTicker(dur time.Duration, cb func()) *TickerCB {
	return &TickerCB{
		t:    time.NewTicker(dur),
		done: make(chan bool),
		cb: func(save chan time.Time, done chan bool) {
			for {
				select {
				case <-save:
					cb()
				case <-done:
					return
				}
			}
		},
	}
}

func MakeDB[K comparable, V any](t ...time.Duration) *AsyncDB[K, V] {
	dur := time.Duration(time.Minute * 10)
	if len(t) != 0 {
		dur = time.Duration(time.Minute * t[0])
	}
	db := &AsyncDB[K, V]{}
	db.m = sync.Map{}
	db.tickerCB = MakeTicker(dur, func() {
		ExportToFile(db, "database.bin")
	})
	return db
}

func DeleteDB[K comparable, V any](db *AsyncDB[K, V]) {
	db.tickerCB.t.Stop()
	db.tickerCB.done <- true
}
