package db

import (
	"errors"
	"sync"
)

type Entry struct {
	Key   string
	Value []byte
}

type Db struct {
	store map[string]Entry
	mu    sync.RWMutex
}

func NewDb() *Db {
	return &Db{
		store: make(map[string]Entry),
	}
}

func (d *Db) Put(entry Entry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.store[entry.Key] = entry
}

// Get retrieves an Entry from the database based on the provided key.
// Returns an error if the key does not exist.
func (d *Db) Get(key string) (Entry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, exists := d.store[key]
	if !exists {
		return Entry{}, errors.New("entry not found")
	}
	return entry, nil
}
