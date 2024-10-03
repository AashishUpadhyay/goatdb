package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

var sstablemockstore = []string{}

func TestPutAndGet(t *testing.T) {
	// Create a new instance of the Db
	database := NewDb(Options{
		memtableThreshold: 1000,
		dataDirLocation:   "",
		sstableMgr:        &MockSSTableManager{},
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
	// Create a new instance of the Db
	database := NewDb(Options{
		memtableThreshold: 1000,
		dataDirLocation:   "",
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
	// Create a new instance of the Db
	database := NewDb(Options{
		memtableThreshold: 10,
		dataDirLocation:   "",
		sstableMgr:        &MockSSTableManager{},
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

	if len(database.sstables) != 10 {
		t.Fatalf("expected %d, got: %d", 10, len(database.sstables))
	}

	if len(database.memtable) != 0 {
		t.Fatalf("expected %d, got: %d", 0, len(database.memtable))
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
