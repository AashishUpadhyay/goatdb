package db

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"time"
)

// FileHeader represents the fixed-size header at the beginning of each SSTable file
type FileHeader struct {
	Version           uint32
	CreationTimestamp int64
	EntryCount        uint32
	IndexOffset       uint64
	BlockSize         uint32
}

// BlockHeader represents the header for each data block
type BlockHeader struct {
	EntryCount      uint32
	CompressedSize  uint32
	Checksum        uint32
	NextBlockOffset uint64
}

// Index entry for faster lookups
type IndexEntry struct {
	KeyLength   uint32
	Key         string
	BlockOffset uint64
}

const (
	BlockHeaderSize   = 20 // 4 + 4 + 4 + 8 bytes
	MinIndexEntrySize = 12 // 4 (KeyLength) + 8 (BlockOffset) bytes, not including key
)

// Modified interface to support the new format
type SSTableManager interface {
	WriteStrings(fileName string, data []string) error
	ReadAll(fileName string) ([]string, error)
	ReadBlock(fileName string, offset uint64) ([]string, error)
	FindKey(fileName string, key string) (string, error)
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

	// Write file header
	header := FileHeader{
		Version:           1,
		CreationTimestamp: time.Now().Unix(),
		EntryCount:        uint32(len(data)),
		BlockSize:         4096, // 4KB blocks
	}

	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Initialize index
	var index []IndexEntry
	currentOffset, _ := file.Seek(0, 1)

	// Write data blocks
	blockEntries := make([]string, 0, 100)
	for _, item := range data {
		blockEntries = append(blockEntries, item)

		if len(blockEntries) == 100 || item == data[len(data)-1] {
			// Compress block data
			var compressed bytes.Buffer
			compressor := gzip.NewWriter(&compressed)
			for _, entry := range blockEntries {
				compressor.Write([]byte(entry + "\n"))
			}
			compressor.Close()

			// Calculate checksum
			checksum := crc32.ChecksumIEEE(compressed.Bytes())

			// Write block header
			blockHeader := BlockHeader{
				EntryCount:      uint32(len(blockEntries)),
				CompressedSize:  uint32(compressed.Len()),
				Checksum:        checksum,
				NextBlockOffset: uint64(currentOffset + int64(compressed.Len()) + 20), // 20 is block header size
			}

			binary.Write(file, binary.BigEndian, &blockHeader)
			file.Write(compressed.Bytes())

			// Add first key of block to index
			index = append(index, IndexEntry{
				KeyLength:   uint32(len(blockEntries[0])),
				Key:         blockEntries[0],
				BlockOffset: uint64(currentOffset),
			})

			currentOffset = int64(blockHeader.NextBlockOffset)
			blockEntries = blockEntries[:0]
		}
	}

	// Write index
	indexOffset, _ := file.Seek(0, 1)

	// First write the number of index entries
	indexCount := uint32(len(index))
	if err := binary.Write(file, binary.BigEndian, indexCount); err != nil {
		return fmt.Errorf("failed to write index count: %w", err)
	}

	// Then write each index entry
	for _, entry := range index {
		if err := binary.Write(file, binary.BigEndian, entry.KeyLength); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}
		if _, err := file.Write([]byte(entry.Key)); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}
		if err := binary.Write(file, binary.BigEndian, entry.BlockOffset); err != nil {
			return fmt.Errorf("failed to write block offset: %w", err)
		}
	}

	// Update header with index offset
	file.Seek(0, 0)
	header.IndexOffset = uint64(indexOffset)
	binary.Write(file, binary.BigEndian, &header)

	ssm.Logger.Printf("Successfully wrote to SSTable file: %s", fileName)
	return nil
}

func (ssm SSTableFileSystemManager) ReadAll(fileName string) ([]string, error) {
	fullFilePath := filepath.Join(ssm.DataDir, fileName)
	file, err := os.Open(fullFilePath)
	if err != nil {
		ssm.Logger.Printf("Error opening SSTable file %s: %v", fileName, err)
		return nil, err
	}
	defer file.Close()

	// Read file header
	var header FileHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	var results []string
	currentOffset := int64(binary.Size(header))

	// Read all blocks until we reach the index
	for currentOffset < int64(header.IndexOffset) {
		blockData, err := ssm.readBlockAt(file, uint64(currentOffset))
		if err != nil {
			return nil, err
		}
		results = append(results, blockData...)

		// Move to next block
		var blockHeader BlockHeader
		file.Seek(currentOffset, 0)
		binary.Read(file, binary.BigEndian, &blockHeader)
		currentOffset = int64(blockHeader.NextBlockOffset)
	}

	ssm.Logger.Printf("Successfully read SSTable file: %s", fileName)
	return results, nil
}

func (ssm SSTableFileSystemManager) ReadBlock(fileName string, offset uint64) ([]string, error) {
	fullFilePath := filepath.Join(ssm.DataDir, fileName)
	file, err := os.Open(fullFilePath)
	if err != nil {
		ssm.Logger.Printf("Error opening SSTable file %s: %v", fileName, err)
		return nil, err
	}
	defer file.Close()

	return ssm.readBlockAt(file, offset)
}

// Helper function to read a single block
func (ssm SSTableFileSystemManager) readBlockAt(file *os.File, offset uint64) ([]string, error) {
	// Read block header
	var blockHeader BlockHeader
	file.Seek(int64(offset), 0)
	if err := binary.Read(file, binary.BigEndian, &blockHeader); err != nil {
		return nil, fmt.Errorf("failed to read block header: %w", err)
	}

	// Read compressed data
	compressedData := make([]byte, blockHeader.CompressedSize)
	if _, err := file.Read(compressedData); err != nil {
		return nil, fmt.Errorf("failed to read compressed data: %w", err)
	}

	// Verify checksum
	if crc32.ChecksumIEEE(compressedData) != blockHeader.Checksum {
		return nil, fmt.Errorf("block checksum mismatch at offset %d", offset)
	}

	// Decompress data
	reader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer reader.Close()

	// Read decompressed data
	scanner := bufio.NewScanner(reader)
	var results []string
	for scanner.Scan() {
		results = append(results, scanner.Text())
	}

	return results, nil
}

func (ssm SSTableFileSystemManager) FindKey(fileName string, searchKey string) (string, error) {
	fullFilePath := filepath.Join(ssm.DataDir, fileName)
	file, err := os.Open(fullFilePath)
	if err != nil {
		ssm.Logger.Printf("Error opening SSTable file %s: %v", fileName, err)
		return "", err
	}
	defer file.Close()

	// Read file header
	var header FileHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return "", fmt.Errorf("failed to read header: %w", err)
	}

	// Jump to index and read index count
	file.Seek(int64(header.IndexOffset), 0)
	var indexCount uint32
	if err := binary.Read(file, binary.BigEndian, &indexCount); err != nil {
		return "", fmt.Errorf("failed to read index count: %w", err)
	}

	// Binary search through the index
	left, right := uint32(0), indexCount-1
	var targetOffset uint64

	for left <= right {
		mid := (left + right) / 2

		// Calculate position of middle entry
		entryPos := int64(header.IndexOffset) + 4 // Skip index count

		// Skip to the middle entry
		for i := uint32(0); i < mid; i++ {
			// Read key length
			var keyLen uint32
			file.Seek(entryPos, 0)
			binary.Read(file, binary.BigEndian, &keyLen)
			// Skip key and block offset
			entryPos += int64(4 + keyLen + 8)
		}

		// Read middle entry
		var keyLen uint32
		file.Seek(entryPos, 0)
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			return "", fmt.Errorf("failed to read key length at index: %w", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := file.Read(keyBytes); err != nil {
			return "", fmt.Errorf("failed to read key at index: %w", err)
		}
		indexKey := string(keyBytes)

		var blockOffset uint64
		if err := binary.Read(file, binary.BigEndian, &blockOffset); err != nil {
			return "", fmt.Errorf("failed to read block offset at index: %w", err)
		}

		// Compare and adjust search range
		if indexKey == searchKey {
			targetOffset = blockOffset
			break
		} else if indexKey < searchKey {
			targetOffset = blockOffset // Remember this offset as it might contain our key
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if targetOffset == 0 {
		return "", fmt.Errorf("key not found: %s", searchKey)
	}

	// Read the target block
	entries, err := ssm.readBlockAt(file, targetOffset)
	if err != nil {
		return "", fmt.Errorf("failed to read block: %w", err)
	}

	// Binary search within the block
	blockLeft, blockRight := 0, len(entries)-1
	for blockLeft <= blockRight {
		blockMid := (blockLeft + blockRight) / 2
		if entries[blockMid] == searchKey {
			return entries[blockMid], nil
		} else if entries[blockMid] < searchKey {
			blockLeft = blockMid + 1
		} else {
			blockRight = blockMid - 1
		}
	}

	return "", fmt.Errorf("key not found: %s", searchKey)
}
