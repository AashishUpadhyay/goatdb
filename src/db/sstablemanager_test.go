package db

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestReadAfterWrite(t *testing.T) {
	currentTestDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting current test directory: %s", err)
	}
	dataDir := currentTestDir + "/.sstablemanagertestdir/"
	fileName := "sstable1.sst"
	deleteDirectoryIfExists(dataDir)

	// Create a logger for testing
	logger := log.New(os.Stdout, "SSTABLE_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	ssm, err := NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager: %s", err)
	}

	err = ssm.Write(fileName, []Entry{
		{Key: "ASDF", Value: []byte("ASDF")},
		{Key: "QWERTY", Value: []byte("QWERTY")},
		{Key: "ZXCVB", Value: []byte("ZXCVB")},
	})
	if err != nil {
		t.Fatalf("error writing strings: %s", err)
	}

	dataRead, err := ssm.ReadAll(fileName)
	if err != nil {
		t.Fatalf("error reading file: %s", err)
	}

	if len(dataRead) != 3 {
		t.Fatalf("expected data length %d, got: %d", 3, len(dataRead))
	}
	deleteDirectoryIfExists(dataDir)
}

func TestNewFileManager(t *testing.T) {
	currentTestDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting current test directory: %s", err)
	}
	dataDir := filepath.Join(currentTestDir, ".testNewFileManager")
	defer deleteDirectoryIfExists(dataDir)

	logger := log.New(os.Stdout, "SSTABLE_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	// Test creating a new directory
	_, err = NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager: %s", err)
	}

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Fatalf("expected directory to be created, but it doesn't exist")
	}

	// Test with existing directory
	_, err = NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager with existing directory: %s", err)
	}
}

func TestWriteStringsError(t *testing.T) {
	currentTestDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting current test directory: %s", err)
	}
	dataDir := filepath.Join(currentTestDir, ".testWriteStringsError")
	defer deleteDirectoryIfExists(dataDir)

	logger := log.New(os.Stdout, "SSTABLE_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	ssm, err := NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager: %s", err)
	}

	// Test writing to a non-existent subdirectory
	err = ssm.Write("nonexistent/file.sst", []Entry{
		{Key: "ASDF", Value: []byte("value1")},
	})
	if err == nil {
		t.Fatalf("expected error when writing to non-existent subdirectory, but got nil")
	}
}

func TestReadAllError(t *testing.T) {
	currentTestDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting current test directory: %s", err)
	}
	dataDir := filepath.Join(currentTestDir, ".testReadAllError")
	defer deleteDirectoryIfExists(dataDir)

	logger := log.New(os.Stdout, "SSTABLE_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	ssm, err := NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager: %s", err)
	}

	// Test reading a non-existent file
	_, err = ssm.ReadAll("nonexistent.sst")
	if err == nil {
		t.Fatalf("expected error when reading non-existent file, but got nil")
	}
}

func TestLargeFileWriteAndRead(t *testing.T) {
	currentTestDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting current test directory: %s", err)
	}
	dataDir := filepath.Join(currentTestDir, ".testLargeFile")
	defer deleteDirectoryIfExists(dataDir)

	logger := log.New(os.Stdout, "SSTABLE_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	ssm, err := NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager: %s", err)
	}

	// Create a large dataset
	largeData := make([]Entry, 100000)
	for i := range largeData {
		largeData[i] = Entry{Key: fmt.Sprintf("data_%d", i), Value: []byte(fmt.Sprintf("value_%d", i))}
	}

	fileName := "large_file.sst"
	err = ssm.Write(fileName, largeData)
	if err != nil {
		t.Fatalf("error writing large file: %s", err)
	}

	readData, err := ssm.ReadAll(fileName)
	if err != nil {
		t.Fatalf("error reading large file: %s", err)
	}

	if len(readData) != len(largeData) {
		t.Fatalf("expected %d items, got %d", len(largeData), len(readData))
	}

	for i, item := range readData {
		if item.Key != largeData[i].Key || !bytes.Equal(item.Value, largeData[i].Value) {
			t.Fatalf("mismatch at index %d: expected %s, got %s", i, largeData[i], item)
		}
	}
}

func TestFileWriteAndFindKey(t *testing.T) {
	currentTestDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting current test directory: %s", err)
	}
	dataDir := filepath.Join(currentTestDir, ".testLargeFile")
	defer deleteDirectoryIfExists(dataDir)

	logger := log.New(os.Stdout, "SSTABLE_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	ssm, err := NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager: %s", err)
	}

	// Create a large dataset
	largeData := make([]Entry, 1000)
	for i := range largeData {
		largeData[i] = Entry{Key: fmt.Sprintf("data_%d", i), Value: []byte(fmt.Sprintf("value_%d", i))}
	}

	fileName := "large_file.sst"
	err = ssm.Write(fileName, largeData)
	if err != nil {
		t.Fatalf("error writing large file: %s", err)
	}

	returnedValue, err := ssm.FindKey(fileName, "data_100")
	if err != nil {
		t.Fatalf("error reading large file: %s", err)
	}

	if returnedValue.Key != "data_100" || !bytes.Equal(returnedValue.Value, []byte("value_100")) {
		t.Fatalf("expected %s, got %s", "data_100", returnedValue)
	}
}

func TestFileWriteAndFindKeyForNonExistingKey(t *testing.T) {
	currentTestDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("error getting current test directory: %s", err)
	}
	dataDir := filepath.Join(currentTestDir, ".testLargeFile")
	defer deleteDirectoryIfExists(dataDir)

	logger := log.New(os.Stdout, "SSTABLE_TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	ssm, err := NewFileManager(dataDir, logger)
	if err != nil {
		t.Fatalf("error creating file manager: %s", err)
	}

	// Create a large dataset
	largeData := make([]Entry, 1000)
	for i := range largeData {
		largeData[i] = Entry{Key: fmt.Sprintf("data_%d", i), Value: []byte(fmt.Sprintf("value_%d", i))}
	}

	fileName := "large_file.sst"
	err = ssm.Write(fileName, largeData)
	if err != nil {
		t.Fatalf("error writing large file: %s", err)
	}

	_, err = ssm.FindKey(fileName, "asdf")
	if err == nil {
		t.Fatalf("expecting error!")
	}

	if err.Error() != "key not found: asdf" {
		t.Fatalf("expecting error: key not found: asdf, got: %s", err)
	}
}

func deleteDirectoryIfExists(dirPath string) error {
	err := os.RemoveAll(dirPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error deleting directory: %w", err)
	}
	return nil
}
