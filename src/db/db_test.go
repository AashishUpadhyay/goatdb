package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
)

var sstablemockstore = []string{}

func TestPutAndGet(t *testing.T) {
	// Create a logger for testing
	logger := log.New(os.Stdout, "DB_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	// Create a new instance of the Db
	database := NewDb(Options{
		MemtableThreshold: 1000,
		SstableMgr:        &MockSSTableManager{},
		Logger:            logger,
	})

	// Test data to put into the database
	key := "user1"
	value := []byte("Hello, World!")

	// Create an entry
	entry := Entry{
		Key:   key,
		Value: value,
	}

	// Put the entry into the database
	database.Put(entry)

	// Now, try to get the entry back
	retrievedEntry, err := database.Get(key)

	// Test for errors in retrieving the entry
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Test that the retrieved key is correct
	if retrievedEntry.Key != key {
		t.Errorf("expected key %s, got %s", key, retrievedEntry.Key)
	}

	// Test that the retrieved value is correct
	if !bytes.Equal(retrievedEntry.Value, value) {
		t.Errorf("expected value %s, got %s", value, retrievedEntry.Value)
	}
}

func TestGetNonExistentKey(t *testing.T) {
	// Create a logger for testing
	logger := log.New(os.Stdout, "DB_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	// Create a new instance of the Db
	database := NewDb(Options{
		MemtableThreshold: 1000,
		SstableMgr:        &MockSSTableManager{},
		Logger:            logger,
	})

	// Try to get an entry that does not exist
	_, err := database.Get("nonexistent")

	// Expecting an error for a missing key
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	expectedError := "entry not found"
	if err.Error() != expectedError {
		t.Errorf("expected error message: %s, got: %s", expectedError, err.Error())
	}
}

func TestConcurrency(t *testing.T) {
	// Create a logger for testing
	logger := log.New(os.Stdout, "DB_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	// Create a new instance of the Db
	var database *LSM = NewDb(Options{
		MemtableThreshold: 10,
		SstableMgr:        &MockSSTableManager{},
		Logger:            logger,
	})
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			var key = "testkey_" + strconv.Itoa(idx)
			var val = convertToBytes(int16(idx))
			database.Put(Entry{
				Key:   key,
				Value: val,
			})
			wg.Done()
		}(i)
	}
	wg.Wait()

	if len(database.Sstables) != 10 {
		t.Fatalf("expected %d, got: %d", 10, len(database.Sstables))
	}

	if len(database.Memtable) != 0 {
		t.Fatalf("expected %d, got: %d", 0, len(database.Memtable))
	}

	for i := 0; i < iterations; i++ {
		var key = "testkey_" + strconv.Itoa(i)
		retrievedEntry, err := database.Get(key)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		retrievedInt := convertBytesToInt(retrievedEntry.Value)
		if retrievedInt != int16(i) {
			t.Fatalf("expected %d, got %d", i, retrievedInt)
		}
	}
}

func TestFlushMemtableToDisk(t *testing.T) {
	logger := log.New(os.Stdout, "DB_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	database := NewDb(Options{
		MemtableThreshold: 3,
		SstableMgr:        &MockSSTableManager{},
		Logger:            logger,
	})

	// Add entries to trigger flush
	for i := 0; i < 3; i++ {
		err := database.Put(Entry{Key: fmt.Sprintf("key%d", i), Value: []byte(fmt.Sprintf("value%d", i))})
		if err != nil {
			t.Fatalf("Failed to put entry: %v", err)
		}
	}

	// Check if memtable was flushed
	if len(database.Memtable) != 0 {
		t.Errorf("Expected empty memtable, got %d entries", len(database.Memtable))
	}

	// Check if SSTable was created
	if len(database.Sstables) != 1 {
		t.Errorf("Expected 1 SSTable, got %d", len(database.Sstables))
	}

	// Add one more entry to check if new memtable works
	err := database.Put(Entry{Key: "key3", Value: []byte("value3")})
	if err != nil {
		t.Fatalf("Failed to put entry after flush: %v", err)
	}

	if len(database.Memtable) != 1 {
		t.Errorf("Expected 1 entry in memtable after flush, got %d", len(database.Memtable))
	}
}

func convertToBytes(num int16) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, num)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}

func convertBytesToInt(buf []byte) int16 {
	var retVal int16
	reader := bytes.NewReader(buf)
	binary.Read(reader, binary.BigEndian, &retVal)
	return retVal
}

type MockSSTableManager struct {
}

func (ffd *MockSSTableManager) WriteStrings(fileName string, data []string) error {
	sstablemockstore = append(sstablemockstore, data...)
	return nil
}

func (ffd *MockSSTableManager) ReadAll(fileName string) ([]string, error) {
	return sstablemockstore, nil
}

func (ffd *MockSSTableManager) ReadBlock(fileName string, offset uint64) ([]string, error) {
	return nil, nil
}

func (ffd *MockSSTableManager) FindKey(fileName string, key string) (string, error) {
	return "", nil
}

func TestSerializeDeserialize(t *testing.T) {
	originalEntry := Entry{
		Key:   "testKey",
		Value: []byte("testValue"),
	}

	serialized, err := serializeToBase64(originalEntry)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	deserialized, err := deserializeFromBase64(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if deserialized.Key != originalEntry.Key {
		t.Errorf("Expected key %s, got %s", originalEntry.Key, deserialized.Key)
	}

	if !bytes.Equal(deserialized.Value, originalEntry.Value) {
		t.Errorf("Expected value %v, got %v", originalEntry.Value, deserialized.Value)
	}
}

func TestSearchInSSTable(t *testing.T) {
	logger := log.New(os.Stdout, "DB_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	mockSSTableMgr := &MockSSTableManager{}
	database := NewDb(Options{
		MemtableThreshold: 3,
		SstableMgr:        mockSSTableMgr,
		Logger:            logger,
	})

	// Add entries to trigger flush
	for i := 0; i < 3; i++ {
		err := database.Put(Entry{Key: fmt.Sprintf("key%d", i), Value: []byte(fmt.Sprintf("value%d", i))})
		if err != nil {
			t.Fatalf("Failed to put entry: %v", err)
		}
	}

	// Search for existing key
	entry, exists := database.searchInSSTable(0, "key1")
	if !exists {
		t.Errorf("Expected to find key1 in SSTable")
	}
	if string(entry.Value) != "value1" {
		t.Errorf("Expected value1, got %s", string(entry.Value))
	}

	// Search for non-existing key
	_, exists = database.searchInSSTable(0, "nonexistent")
	if exists {
		t.Errorf("Expected not to find nonexistent key in SSTable")
	}
}

func TestConcurrentGet(t *testing.T) {
	logger := log.New(os.Stdout, "DB_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	database := NewDb(Options{
		MemtableThreshold: 1000,
		SstableMgr:        &MockSSTableManager{},
		Logger:            logger,
	})

	// Add some entries
	for i := 0; i < 100; i++ {
		err := database.Put(Entry{Key: fmt.Sprintf("key%d", i), Value: []byte(fmt.Sprintf("value%d", i))})
		if err != nil {
			t.Fatalf("Failed to put entry: %v", err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			entry, err := database.Get(key)
			if err != nil {
				t.Errorf("Failed to get key %s: %v", key, err)
			}
			if string(entry.Value) != fmt.Sprintf("value%d", i) {
				t.Errorf("Expected value%d, got %s", i, string(entry.Value))
			}
		}(i)
	}
	wg.Wait()
}

func TestErrorHandling(t *testing.T) {
	logger := log.New(os.Stdout, "DB_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	// Test SSTableManager write error
	errorMgr := &ErrorMockSSTableManager{writeError: fmt.Errorf("write error")}
	database := NewDb(Options{
		MemtableThreshold: 2,
		SstableMgr:        errorMgr,
		Logger:            logger,
	})

	err := database.Put(Entry{Key: "key1", Value: []byte("value1")})
	if err != nil {
		t.Fatalf("Failed to put first entry: %v", err)
	}

	err = database.Put(Entry{Key: "key2", Value: []byte("value2")})
	if err == nil {
		t.Errorf("Expected error on second put, got nil")
	}

	// Test SSTableManager read error
	errorMgr = &ErrorMockSSTableManager{readError: fmt.Errorf("read error")}
	database = NewDb(Options{
		MemtableThreshold: 2,
		SstableMgr:        errorMgr,
		Logger:            logger,
	})

	database.Put(Entry{Key: "key1", Value: []byte("value1")})
	database.Put(Entry{Key: "key2", Value: []byte("value2")})

	_, err = database.Get("key1")
	if err == nil {
		t.Errorf("Expected error on get, got nil")
	}
}

// ErrorMockSSTableManager is a mock SSTableManager that can return errors
type ErrorMockSSTableManager struct {
	MockSSTableManager
	writeError error
	readError  error
}

func (m *ErrorMockSSTableManager) WriteStrings(fileName string, data []string) error {
	if m.writeError != nil {
		return m.writeError
	}
	return m.MockSSTableManager.WriteStrings(fileName, data)
}

func (m *ErrorMockSSTableManager) ReadAll(fileName string) ([]string, error) {
	if m.readError != nil {
		return nil, m.readError
	}
	return m.MockSSTableManager.ReadAll(fileName)
}
