package db

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

type SSTableManager interface {
	WriteStrings(fileName string, data []string) error
	ReadAll(fileName string) ([]string, error)
}

type SSTableFileSystemManager struct {
	dataDir string
}

func NewFileManager(dataDir string) (SSTableManager, error) {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err = os.MkdirAll(dataDir, os.ModePerm)
		if err != nil {
			return &SSTableFileSystemManager{}, fmt.Errorf("error creating directory: %w", err)
		}
		fmt.Printf("Directory created: %s\n", dataDir)
	} else {
		fmt.Printf("Directory already exists: %s\n", dataDir)
	}
	return &SSTableFileSystemManager{
		dataDir: dataDir,
	}, nil
}

func (ssm *SSTableFileSystemManager) WriteStrings(fileName string, data []string) error {
	fullFilePath := filepath.Join(ssm.dataDir, fileName)
	file, err := os.Create(fullFilePath)
	if err != nil {
		fmt.Println("Error creating SSTable file:", err)
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, item := range data {
		_, err = writer.WriteString(fmt.Sprintf("%s\n", item))
		if err != nil {
			fmt.Println("Error writing to SSTable file:", err)
			return err
		}
	}
	writer.Flush()
	return nil
}

func (ssm *SSTableFileSystemManager) ReadAll(fileName string) ([]string, error) {
	fullFilePath := filepath.Join(ssm.dataDir, fileName)
	file, err := os.Open(fullFilePath)
	if err != nil {
		fmt.Println("Error opening SSTable file:", err)
		return []string{}, err
	}
	defer file.Close()

	var returnValue []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		returnValue = append(returnValue, line)
	}

	return returnValue, nil
}
