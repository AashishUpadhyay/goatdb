package db

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/AashishUpadhyay/goatdb/src/wal"
)

type Options struct {
	MemtableThreshold int
	SstableMgr        SSTableManager
	Logger            *log.Logger
	WalDir            string
	WalConfig         struct {
		SegmentSize    int64
		RetentionPolicy *wal.RetentionPolicy
	}
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
	walManager *wal.Manager
}

func NewDb(opts Options) (*LSM, error) {
	walManager, err := wal.NewManager(opts.WalDir, opts.WalConfig.SegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL manager: %w", err)
	}

	if opts.WalConfig.RetentionPolicy != nil {
		walManager.SetRetentionPolicy(opts.WalConfig.RetentionPolicy)
	}

	db := &LSM{
		Memtable:   make(map[string]Entry),
		threshold:  opts.MemtableThreshold,
		Sstables:   []string{},
		sstableMgr: opts.SstableMgr,
		logger:     opts.Logger,
		walManager: walManager,
	}

	if err := db.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return db, nil
}

func (db *LSM) Put(entry Entry) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	walEntry := &wal.Entry{
		Type:  wal.EntryPut,
		Key:   []byte(entry.Key),
		Value: entry.Value,
	}

	if err := db.walManager.Append(walEntry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	db.Memtable[entry.Key] = entry
	db.logger.Printf("Added entry with key: %s to memtable", entry.Key)
	if len(db.Memtable) > db.threshold-1 {
		return db.flushMemtableToDisk()
	}
	return nil
}

func (db *LSM) flushMemtableToDisk() error {
	filename := fmt.Sprintf("sstable_%d.sst", len(db.Sstables))
	data := make([]Entry, 0, len(db.Memtable))
	for _, value := range db.Memtable {
		data = append(data, value)
	}

	if err := db.sstableMgr.Write(filename, data); err != nil {
		db.logger.Printf("Error in writing sstable to disk: %v", err)
		return err
	}

	if err := db.walManager.RemoveOldSegments(); err != nil {
		db.logger.Printf("Warning: failed to cleanup WAL segments: %v", err)
	}

	db.Memtable = make(map[string]Entry)
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

func (db *LSM) recoverFromWAL() error {
	entries, err := db.walManager.ReadAll()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.Type == wal.EntryPut {
			db.Memtable[string(entry.Key)] = Entry{
				Key:   string(entry.Key),
				Value: entry.Value,
			}
		}
	}

	return nil
}
