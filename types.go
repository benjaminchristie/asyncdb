package asyncdb

import (
	"sync"
)

type KeyExistsError struct{}
type KeyNotExistsError struct{}
type AsyncDB[K comparable, V any] struct {
	M       sync.Map
	g_mutex sync.RWMutex        // GLOBAL lock for the db map
}

type truthyFunc[V any] func(V) bool // helper function for SELECT, DELETE

func (m *KeyExistsError) Error() string {
	return "Key exists in database"
}

func (m *KeyNotExistsError) Error() string {
	return "Key does not exist in database"
}

func MakeDB[K comparable, V any]() *AsyncDB[K, V] {
	db := &AsyncDB[K, V]{}
	db.M = sync.Map{}
	return db
}
