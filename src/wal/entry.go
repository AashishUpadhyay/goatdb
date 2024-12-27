package wal

import (
    "encoding/binary"
    "hash/crc32"
)

type EntryType byte

const (
    EntryPut EntryType = iota + 1
    EntryDelete
)

// Entry format:
// | CRC (4) | Type (1) | KeyLen (4) | ValueLen (4) | Key | Value |
type Entry struct {
    Type  EntryType
    Key   []byte
    Value []byte
}

func (e *Entry) Encode() ([]byte, error) {
    keyLen := len(e.Key)
    valueLen := len(e.Value)
    
    // Calculate total size: CRC + Type + KeyLen + ValueLen + Key + Value
    totalLen := 4 + 1 + 4 + 4 + keyLen + valueLen
    buf := make([]byte, totalLen)
    
    // Skip CRC for now (first 4 bytes)
    offset := 4
    
    // Write type
    buf[offset] = byte(e.Type)
    offset++
    
    // Write key length
    binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
    offset += 4
    
    // Write value length
    binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
    offset += 4
    
    // Write key
    copy(buf[offset:], e.Key)
    offset += keyLen
    
    // Write value
    copy(buf[offset:], e.Value)
    
    // Calculate and write CRC
    crc := crc32.ChecksumIEEE(buf[4:])
    binary.BigEndian.PutUint32(buf[0:], crc)
    
    return buf, nil
}

func DecodeEntry(buf []byte) (*Entry, error) {
    if len(buf) < 13 { // Minimum size: CRC + Type + KeyLen + ValueLen
        return nil, ErrCorruptedEntry
    }
    
    // Verify CRC
    storedCRC := binary.BigEndian.Uint32(buf[0:])
    computedCRC := crc32.ChecksumIEEE(buf[4:])
    if storedCRC != computedCRC {
        return nil, ErrChecksumMismatch
    }
    
    offset := 4
    
    // Read type
    entryType := EntryType(buf[offset])
    offset++
    
    // Read key length
    keyLen := binary.BigEndian.Uint32(buf[offset:])
    offset += 4
    
    // Read value length
    valueLen := binary.BigEndian.Uint32(buf[offset:])
    offset += 4
    
    // Validate lengths
    if len(buf) < offset+int(keyLen)+int(valueLen) {
        return nil, ErrCorruptedEntry
    }
    
    // Read key
    key := make([]byte, keyLen)
    copy(key, buf[offset:offset+int(keyLen)])
    offset += int(keyLen)
    
    // Read value
    value := make([]byte, valueLen)
    copy(value, buf[offset:offset+int(valueLen)])
    
    return &Entry{
        Type:  entryType,
        Key:   key,
        Value: value,
    }, nil
} 