package db

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

type Options struct {
	MemtableThreshold int
	SstableMgr        SSTableManager
	Logger            *log.Logger
}

type DB interface {
	Put(entry Entry) error
	Get(key string) (Entry, error)
}

type LSM struct {
	Memtable   map[string]Entry
	Sstables   []string
	threshold  int
	mu         sync.RWMutex
	sstableMgr SSTableManager
	logger     *log.Logger
}

func NewDb(opts Options) *LSM {
	return &LSM{
		Memtable:   make(map[string]Entry),
		threshold:  opts.MemtableThreshold,
		Sstables:   []string{},
		sstableMgr: opts.SstableMgr,
		logger:     opts.Logger,
	}
}

func (db *LSM) Put(entry Entry) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Memtable[entry.Key] = entry
	db.logger.Printf("Added entry with key: %s to memtable", entry.Key)
	if len(db.Memtable) > db.threshold-1 {
		return db.flushMemtableToDisk()
	}
	return nil
}

func (db *LSM) flushMemtableToDisk() error {
	filename := fmt.Sprintf("sstable_%d.sst", len(db.Sstables))
	data := []Entry{}
	for _, value := range db.Memtable {
		data = append(data, value)
	}

	err := db.sstableMgr.Write(filename, data)
	if err != nil {
		db.logger.Printf("Error in writing sstable to disk: %v", err)
		return err
	}
	db.Memtable = make(map[string]Entry) // Clear the memtable
	db.Sstables = append(db.Sstables, filename)
	db.logger.Printf("Flushed to disk: %s", filename)
	return nil
}

func (db *LSM) Get(key string) (Entry, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	entry, exists := db.Memtable[key]
	if exists {
		db.logger.Printf("Found entry with key: %s in memtable", key)
		return entry, nil
	}

	for i := len(db.Sstables) - 1; i >= 0; i-- {
		entry, exists = db.searchInSSTable(i, key)
		if exists {
			db.logger.Printf("Found entry with key: %s in SSTable %d", key, i)
			return entry, nil
		}
	}

	db.logger.Printf("Entry with key: %s not found", key)
	return Entry{}, errors.New("entry not found")
}

func (db *LSM) searchInSSTable(idx int, key string) (Entry, bool) {
	filename := fmt.Sprintf("sstable_%d.sst", idx)
	entry, err := db.sstableMgr.FindKey(filename, key)
	if err != nil {
		db.logger.Printf("Error in reading sstable %s: %v", filename, err)
		return Entry{}, false
	}
	return entry, true
}
