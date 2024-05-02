package asyncdb

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"time"
)


// SELECT
func SelectFrom[K comparable, V any](db *AsyncDB[K, V], f truthyFunc[V]) []K {
	db.g_mutex.RLock()
	defer db.g_mutex.RUnlock()
	keys := make([]K, 0)
	db.m.Range(func(key any, value any) bool {
		if f(value.(V)) {
			keys = append(keys, key.(K))
		}
		return true
	})
	return keys
}

// UPDATE
func Update[K comparable, V any](db *AsyncDB[K, V], key K, value V) error {
	return ChangeItem(db, key, value)
}

// IMPORTANT: this delete function assumes that whenever
// a user checks if a key exists in the table,
// they check, then activate the i_mutex, then checks again
func DeleteKey[K comparable, V any](db *AsyncDB[K, V], key K) {
	db.m.Delete(key)
}

// INSERT INTO
func InsertInto[K comparable, V any](db *AsyncDB[K, V], key K, value V) error {
	return AddItem(db, key, value)
}

// CREATE INDEX
func CreateIndex[K comparable, V any](db *AsyncDB[K, V], key K) error {
	var v V
	return InsertInto(db, key, v)
}

func AddItem[K comparable, V any](db *AsyncDB[K, V], key K, value V) error {
	_, exists := db.m.Load(key)
	if exists {
		return &KeyExistsError{}
	}
	db.m.Store(key, value)
	return nil
}


func GetValueFromKey[K comparable, V any](db *AsyncDB[K, V], key K) (V, error) {
	e, exists := db.m.Load(key)
	if !exists {
		return e.(V), &KeyNotExistsError{}
	}
	return e.(V), nil
}

func ChangeItem[K comparable, V any](db *AsyncDB[K, V], key K, value V) error {
	_, exists := db.m.LoadOrStore(key, value)
	if !exists {
		return &KeyNotExistsError{}
	}
	return nil
}

func itemInSlice(arr []any, item any) bool {
	for s := range arr {
		if s == item {
			return true
		}
	}
	return false
}

func GetItems[K comparable, V any](db *AsyncDB[K, V], keys ...any) ([]V, error) {
	values := make([]V, len(keys))
	db.m.Range(func(key any, value any) bool {
		if itemInSlice(keys, key) {
			values = append(values, value.(V))
		}
		return true
	})
	return values, nil
}

func ExportToFile[K comparable, V any](db *AsyncDB[K, V], filename string) error {
	db.g_mutex.RLock()
	defer db.g_mutex.RUnlock()
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	keyValues := make(map[K]V)
	db.m.Range(func(key any, value any) bool {
		keyValues[key.(K)] = value.(V)
		return true
	})
	err := enc.Encode(keyValues)
	if err != nil {
		log.Print("Enc failed")
		return err
	}
	err = os.WriteFile(filename, buf.Bytes(), 0644)
	if err != nil {
		log.Print("Write failed")
		os.Remove(filename)
	}
	return err
}

func ImportFromFile[K comparable, V any](filename string, dur ...time.Duration) (*AsyncDB[K, V], error) {
	db := MakeDB[K, V](dur...)
	fh, err := os.Open(filename)
	defer fh.Close()
	if err != nil {
		log.Print("Couldn't open file")
		return db, err
	}
	fh_info, err := fh.Stat()
	if err != nil {
		log.Print("Error reading file stat")
		return db, err
	}
	raw_bytes := make([]byte, fh_info.Size())
	if _, err = fh.Read(raw_bytes); err != nil {
		log.Print("Error reading bytes")
		return db, err
	}
	buf := bytes.NewBuffer(raw_bytes)
	dec := gob.NewDecoder(buf)
	keyValues := make(map[K]V)
	if err = dec.Decode(&keyValues); err != nil {
		log.Print("Import failed")
	}
	for k, v := range keyValues {
		db.m.Store(k, v)
	}
	return db, err
}
