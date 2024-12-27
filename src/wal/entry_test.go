package wal

import (
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"testing"
)

func TestEncodeAndDecode(t *testing.T) {
	entry := &Entry{
		Type:  EntryPut,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("failed to encode entry: %v", err)
	}

	decoded, err := DecodeEntry(encoded)
	if err != nil {
		t.Fatalf("failed to decode entry: %v", err)
	}

	if !reflect.DeepEqual(entry, decoded) {
		t.Fatalf("decoded entry does not match original entry")
	}
}

func TestDecodeEntryErrors(t *testing.T) {
	encoded := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
	_, err := DecodeEntry(encoded)
	if err != ErrCorruptedEntry {
		t.Fatalf("expected ErrCorruptedEntry, got %v", err)
	}

	encoded = []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
	_, err = DecodeEntry(encoded)
	if err != ErrChecksumMismatch {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}

	// corrupted entry where the key and value are incomplete
	corrupted_buf := []byte{
		0xAA, 0xBB, 0xCC, 0xDD, // CRC (4 bytes)
		0x01,                   // Type (1 byte)
		0x00, 0x00, 0x00, 0x06, // KeyLen = 6 (4 bytes)
		0x00, 0x00, 0x00, 0x0A, // ValueLen = 10 (4 bytes)
		0x6B, 0x65, 0x79, // Partial Key (3 bytes)
		0x76, 0x61, // Partial Value (2 bytes)
	}

	crc := crc32.ChecksumIEEE(corrupted_buf[4:])
	encoded_copy := append([]byte{}, corrupted_buf...)
	binary.BigEndian.PutUint32(encoded_copy[0:4], crc)
	_, err = DecodeEntry(encoded_copy)
	if err != ErrCorruptedEntry {
		t.Fatalf("expected ErrCorruptedEntry, got %v", err)
	}
}
