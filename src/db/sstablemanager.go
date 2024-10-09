package db

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type SSTableManager interface {
	WriteStrings(fileName string, data []string) error
	ReadAll(fileName string) ([]string, error)
}

type SSTableFileSystemManager struct {
	DataDir string
	Logger  *log.Logger
}

func NewFileManager(dataDir string, logger *log.Logger) (SSTableManager, error) {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err = os.MkdirAll(dataDir, os.ModePerm)
		if err != nil {
			logger.Printf("Error creating directory: %v", err)
			return &SSTableFileSystemManager{}, fmt.Errorf("error creating directory: %w", err)
		}
		logger.Printf("Directory created: %s", dataDir)
	} else {
		logger.Printf("Directory already exists: %s", dataDir)
	}
	return &SSTableFileSystemManager{
		DataDir: dataDir,
		Logger:  logger,
	}, nil
}

func (ssm SSTableFileSystemManager) WriteStrings(fileName string, data []string) error {
	fullFilePath := filepath.Join(ssm.DataDir, fileName)
	file, err := os.Create(fullFilePath)
	if err != nil {
		ssm.Logger.Printf("Error creating SSTable file %s: %v", fileName, err)
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, item := range data {
		_, err = writer.WriteString(fmt.Sprintf("%s\n", item))
		if err != nil {
			ssm.Logger.Printf("Error writing to SSTable file %s: %v", fileName, err)
			return err
		}
	}
	writer.Flush()
	ssm.Logger.Printf("Successfully wrote to SSTable file: %s", fileName)
	return nil
}

func (ssm SSTableFileSystemManager) ReadAll(fileName string) ([]string, error) {
	fullFilePath := filepath.Join(ssm.DataDir, fileName)
	file, err := os.Open(fullFilePath)
	if err != nil {
		ssm.Logger.Printf("Error opening SSTable file %s: %v", fileName, err)
		return []string{}, err
	}
	defer file.Close()

	var returnValue []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		returnValue = append(returnValue, line)
	}

	ssm.Logger.Printf("Successfully read SSTable file: %s", fileName)
	return returnValue, nil
}
