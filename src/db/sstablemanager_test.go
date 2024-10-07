package db

import (
	"fmt"
	"os"
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
	ssm, _ := NewFileManager(dataDir)
	ssm.WriteStrings(fileName, []string{
		"ASDF",
		"QWERTY",
		"ZXCVB",
	})
	dataRead, _ := ssm.ReadAll(fileName)
	if len(dataRead) != 3 {
		t.Fatalf("expected data length %d, got: %d", 3, len(dataRead))
	}
	deleteDirectoryIfExists(dataDir)
}

func deleteDirectoryIfExists(dirPath string) error {
	err := os.RemoveAll(dirPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error deleting directory: %w", err)
	}
	return nil
}
