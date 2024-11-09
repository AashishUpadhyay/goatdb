package db

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Entry struct {
	Key   string
	Value []byte
}

// FileHeader represents the fixed-size header at the beginning of each SSTable file
type FileHeader struct {
	Version           int32
	CreationTimestamp int64
	EntryCount        int32
	IndexOffset       uint64
	BlockSize         int32
}

// BlockHeader represents the header for each data block
type BlockHeader struct {
	EntryCount      int32
	CompressedSize  int32
	Checksum        uint32
	NextBlockOffset uint64
}

// Index entry for faster lookups
type IndexEntry struct {
	StartKeyLength int32
	StartKey       string
	EndKeyLength   int32
	EndKey         string
	BlockOffset    uint64
}

const (
	BlockHeaderSize   = 20 // 4 + 4 + 4 + 8 bytes
	MinIndexEntrySize = 12 // 4 (KeyLength) + 8 (BlockOffset) bytes, not including key
)

// Modified interface to support the new format
type SSTableManager interface {
	Write(fileName string, data []Entry) error
	ReadAll(fileName string) ([]Entry, error)
	ReadBlock(fileName string, offset uint64) ([]Entry, error)
	FindKey(fileName string, key string) (Entry, error)
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

func (ssm SSTableFileSystemManager) Write(fileName string, data []Entry) error {
	sort.Slice(data, func(i, j int) bool {
		return data[i].Key < data[j].Key
	})
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
		EntryCount:        int32(len(data)),
		BlockSize:         4096, // 4KB blocks
	}

	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Initialize index
	var index []IndexEntry
	currentOffset, _ := file.Seek(0, 1)

	// Write data blocks
	blockSize := 100
	if blockSize > len(data) {
		blockSize = len(data)
	}
	blockEntries := make([]string, 0, blockSize)
	for idx, item := range data {
		serializedEntry, err := serializeToBase64(item)
		if err != nil {
			return fmt.Errorf("failed to serialize entry: %w", err)
		}
		blockEntries = append(blockEntries, fmt.Sprintf("%s,%s", item.Key, serializedEntry))

		if len(blockEntries) == 100 || item.Key == data[len(data)-1].Key {
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
				EntryCount:      int32(len(blockEntries)),
				CompressedSize:  int32(compressed.Len()),
				Checksum:        checksum,
				NextBlockOffset: uint64(currentOffset + int64(compressed.Len()) + 20), // 20 is block header size
			}

			binary.Write(file, binary.BigEndian, &blockHeader)
			file.Write(compressed.Bytes())

			// Add first key of block to index
			index = append(index, IndexEntry{
				StartKeyLength: int32(len(data[idx-blockSize+1].Key)),
				StartKey:       data[idx-blockSize+1].Key,
				EndKeyLength:   int32(len(data[idx].Key)),
				EndKey:         data[idx].Key,
				BlockOffset:    uint64(currentOffset),
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
		indexOffset, _ := file.Seek(0, 1)
		ssm.Logger.Printf("index offset start key len: %d", indexOffset)
		if err := binary.Write(file, binary.BigEndian, entry.StartKeyLength); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}
		indexOffset, _ = file.Seek(0, 1)
		ssm.Logger.Printf("index offset start key: %d", indexOffset)
		if _, err := file.Write([]byte(entry.StartKey)); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}
		indexOffset, _ = file.Seek(0, 1)
		ssm.Logger.Printf("index offset end key len: %d", indexOffset)
		if err := binary.Write(file, binary.BigEndian, entry.EndKeyLength); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}
		indexOffset, _ = file.Seek(0, 1)
		ssm.Logger.Printf("index offset end key: %d", indexOffset)
		if _, err := file.Write([]byte(entry.EndKey)); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}
		indexOffset, _ = file.Seek(0, 1)
		ssm.Logger.Printf("index block offset: %d", indexOffset)
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

func (ssm SSTableFileSystemManager) ReadAll(fileName string) ([]Entry, error) {
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

	var results []Entry
	currentOffset := int64(binary.Size(header))

	// Read all blocks until we reach the index
	for currentOffset < int64(header.IndexOffset) {
		blockData, err := ssm.readBlockAt(file, uint64(currentOffset))
		if err != nil {
			return nil, err
		}

		for _, entry := range blockData {
			entryParts := strings.Split(entry, ",")
			decodedEntry, err := deserializeFromBase64(entryParts[1])
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize entry: %w", err)
			}
			results = append(results, decodedEntry)
		}

		// Move to next block
		var blockHeader BlockHeader
		file.Seek(currentOffset, 0)
		binary.Read(file, binary.BigEndian, &blockHeader)
		currentOffset = int64(blockHeader.NextBlockOffset)
	}

	ssm.Logger.Printf("Successfully read SSTable file: %s", fileName)
	return results, nil
}

func (ssm SSTableFileSystemManager) ReadBlock(fileName string, offset uint64) ([]Entry, error) {
	fullFilePath := filepath.Join(ssm.DataDir, fileName)
	file, err := os.Open(fullFilePath)
	if err != nil {
		ssm.Logger.Printf("Error opening SSTable file %s: %v", fileName, err)
		return nil, err
	}
	defer file.Close()

	blockData, err := ssm.readBlockAt(file, uint64(offset))
	if err != nil {
		return nil, err
	}

	var results []Entry

	for _, entry := range blockData {
		decodedEntry, err := deserializeFromBase64(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize entry: %w", err)
		}
		results = append(results, decodedEntry)
	}

	return results, nil
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

func (ssm SSTableFileSystemManager) FindKey(fileName string, searchKey string) (Entry, error) {
	fullFilePath := filepath.Join(ssm.DataDir, fileName)
	file, err := os.Open(fullFilePath)
	if err != nil {
		ssm.Logger.Printf("Error opening SSTable file %s: %v", fileName, err)
		return Entry{}, err
	}
	defer file.Close()

	// Read file header
	var header FileHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return Entry{}, fmt.Errorf("failed to read header: %w", err)
	}

	// Jump to index and read index count
	file.Seek(int64(header.IndexOffset), 0)
	var indexCount uint32
	if err := binary.Read(file, binary.BigEndian, &indexCount); err != nil {
		return Entry{}, fmt.Errorf("failed to read index count: %w", err)
	}

	ssm.Logger.Printf("index count = %d", indexCount)
	// Binary search through the index
	left, right := int32(0), int32(indexCount-1)
	var targetOffset uint64

	for left <= right {
		mid := (left + right) / 2

		// Calculate position of middle entry
		entryPos := int64(header.IndexOffset) + 4 // Skip index count

		// Skip to the middle entry
		for i := int32(0); i < mid; i++ {
			// Read key length
			var startKeyLength uint32
			file.Seek(entryPos, 0)
			indexOffset, _ := file.Seek(0, 1)
			ssm.Logger.Printf("read index offset start key len: %d", indexOffset)
			binary.Read(file, binary.BigEndian, &startKeyLength)
			file.Seek(entryPos+int64(4)+int64(startKeyLength), 0)
			indexOffset, _ = file.Seek(0, 1)
			ssm.Logger.Printf("read index offset end key len: %d", indexOffset)
			var endKeyLength uint32
			binary.Read(file, binary.BigEndian, &endKeyLength)
			// Skip key and block offset
			entryPos += int64(4 + startKeyLength + 4 + endKeyLength + 8)
		}

		// Read middle entry
		var startKeyLength uint32
		file.Seek(entryPos, 0)
		if err := binary.Read(file, binary.BigEndian, &startKeyLength); err != nil {
			return Entry{}, fmt.Errorf("failed to read key length at index: %w", err)
		}

		keyBytes := make([]byte, startKeyLength)
		if _, err := file.Read(keyBytes); err != nil {
			return Entry{}, fmt.Errorf("failed to read key at index: %w", err)
		}
		startIndexKey := string(keyBytes)
		ssm.Logger.Printf("index key: %s", startIndexKey)

		var endKeyLength uint32
		if err := binary.Read(file, binary.BigEndian, &endKeyLength); err != nil {
			return Entry{}, fmt.Errorf("failed to read key length at index: %w", err)
		}
		keyBytes = make([]byte, endKeyLength)
		if _, err := file.Read(keyBytes); err != nil {
			return Entry{}, fmt.Errorf("failed to read key at index: %w", err)
		}
		endIndexKey := string(keyBytes)
		ssm.Logger.Printf("index key: %s", endIndexKey)

		var blockOffset uint64
		if err := binary.Read(file, binary.BigEndian, &blockOffset); err != nil {
			return Entry{}, fmt.Errorf("failed to read block offset at index: %w", err)
		}

		// Compare and adjust search range
		if startIndexKey == searchKey || endIndexKey == searchKey || (startIndexKey < searchKey && endIndexKey > searchKey) {
			targetOffset = blockOffset
			break
		} else if endIndexKey < searchKey {
			targetOffset = blockOffset // Remember this offset as it might contain our key
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if targetOffset == 0 {
		return Entry{}, fmt.Errorf("key not found: %s", searchKey)
	}

	// Read the target block
	entries, err := ssm.readBlockAt(file, targetOffset)
	if err != nil {
		return Entry{}, fmt.Errorf("failed to read block: %w", err)
	}

	// Binary search within the block
	blockLeft, blockRight := 0, len(entries)-1
	for blockLeft <= blockRight {
		blockMid := (blockLeft + blockRight) / 2
		blockMidParts := strings.Split(entries[blockMid], ",")
		if blockMidParts[0] == searchKey {
			return deserializeFromBase64(blockMidParts[1])
		} else if entries[blockMid] < searchKey {
			blockLeft = blockMid + 1
		} else {
			blockRight = blockMid - 1
		}
	}

	return Entry{}, fmt.Errorf("key not found: %s", searchKey)
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
