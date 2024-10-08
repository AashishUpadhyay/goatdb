package db

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Entry struct {
	Key   string
	Value []byte
}

type Options struct {
	MemtableThreshold int
	SstableMgr        SSTableManager
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
}

func NewDb(opts Options) *LSM {
	return &LSM{
		Memtable:   make(map[string]Entry),
		threshold:  opts.MemtableThreshold,
		Sstables:   []string{},
		sstableMgr: opts.SstableMgr,
	}
}

func (db *LSM) Put(entry Entry) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Memtable[entry.Key] = entry
	if len(db.Memtable) > db.threshold-1 {
		return db.flushMemtableToDisk()
	}
	return nil
}

func (db *LSM) flushMemtableToDisk() error {
	filename := fmt.Sprintf("sstable_%d.sst", len(db.Sstables))
	data := []string{}
	for key, value := range db.Memtable {
		valueB64, err := serializeToBase64(value)
		if err != nil {
			fmt.Println("Error in serializing entry when writing to SSTable file", err)
			return err
		}
		data = append(data, fmt.Sprintf("%s,%s\n", key, valueB64))
	}

	err := db.sstableMgr.WriteStrings(filename, data)
	if err != nil {
		fmt.Println("Error in writing sstable to disk!", err)
		return err
	}
	db.Memtable = make(map[string]Entry) // Clear the memtable
	db.Sstables = append(db.Sstables, filename)
	fmt.Printf("Flushed to disk: %s\n", filename)
	return nil
}

func (db *LSM) Get(key string) (Entry, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	entry, exists := db.Memtable[key]
	if exists {
		return entry, nil
	}

	for i := len(db.Sstables) - 1; i >= 0; i-- {
		entry, exists = db.searchInSSTable(i, key)
		if exists {
			return entry, nil
		}
	}

	return Entry{}, errors.New("entry not found")
}

func (db *LSM) searchInSSTable(idx int, key string) (Entry, bool) {
	filename := fmt.Sprintf("sstable_%d.sst", idx)
	fileData, err := db.sstableMgr.ReadAll(filename)
	if err != nil {
		fmt.Println("Error in reading sstable!", err)
		return Entry{}, false
	}
	for _, fd := range fileData {
		parts := strings.Split(fd, ",")
		deseralizedEntry, err := deserializeFromBase64(parts[1])
		if err != nil {
			fmt.Println("Error deserializing value after reading from SSTable", err)
			return Entry{}, false
		}
		if len(parts) == 2 && parts[0] == key {
			return deseralizedEntry, true
		}
	}
	return Entry{}, false
}

func serializeToBase64(entry Entry) (string, error) {
	// Marshal the Entry struct to JSON
	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		return "", err
	}

	// Encode the JSON bytes to base64
	base64Str := base64.StdEncoding.EncodeToString(jsonBytes)

	return base64Str, nil
}

func deserializeFromBase64(base64Str string) (Entry, error) {
	// Decode the base64-encoded string
	jsonBytes, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return Entry{}, err
	}

	// Unmarshal the JSON bytes into an Entry struct
	var entry Entry
	err = json.Unmarshal(jsonBytes, &entry)
	if err != nil {
		return Entry{}, err
	}

	return entry, nil
}
