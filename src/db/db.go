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
	dataDirLocation   string
	memtableThreshold int
	sstableMgr        SSTableManager
}

type Db struct {
	memtable   map[string]Entry
	sstables   []string
	threshold  int
	mu         sync.RWMutex
	dataDir    string
	sstableMgr SSTableManager
}

func NewDb(opts Options) Db {
	return Db{
		memtable:   make(map[string]Entry),
		threshold:  opts.memtableThreshold,
		sstables:   []string{},
		dataDir:    opts.dataDirLocation,
		sstableMgr: opts.sstableMgr,
	}
}

func (d *Db) Put(entry Entry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.memtable[entry.Key] = entry
	if len(d.memtable) > d.threshold - 1 {
		return d.FlushMemtableToDisk()
	}
	return nil
}

func (db *Db) FlushMemtableToDisk() error {
	filename := fmt.Sprintf("sstable_%d.sst", len(db.sstables))
	data := []string{}
	for key, value := range db.memtable {
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
	db.memtable = make(map[string]Entry) // Clear the memtable
	db.sstables = append(db.sstables, filename)
	fmt.Printf("Flushed to disk: %s\n", filename)
	return nil
}

func (d *Db) Get(key string) (Entry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, exists := d.memtable[key]
	if exists {
		return entry, nil
	}

	for i := len(d.sstables) - 1; i >= 0; i-- {
		entry, exists = d.SearchInSSTable(i, key)
		if exists {
			return entry, nil
		}
	}

	return Entry{}, errors.New("entry not found")
}

func (db *Db) SearchInSSTable(idx int, key string) (Entry, bool) {
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
