package wal

import (
    "bufio"
    "encoding/binary"
    "io"
    "os"
    "sync"
)

type segment struct {
    file       *os.File
    writer     *bufio.Writer
    mu         sync.Mutex
    offset     int64
    maxSize    int64
}

func openSegment(path string, maxSize int64) (*segment, error) {
    file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
    if err != nil {
        return nil, &WalError{Op: "open_segment", Err: err}
    }

    // Get current file size
    info, err := file.Stat()
    if err != nil {
        file.Close()
        return nil, &WalError{Op: "stat_segment", Err: err}
    }

    return &segment{
        file:    file,
        writer:  bufio.NewWriter(file),
        offset:  info.Size(),
        maxSize: maxSize,
    }, nil
}

func (s *segment) append(entry *Entry) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    data, err := entry.Encode()
    if err != nil {
        return &WalError{Op: "encode_entry", Err: err}
    }

    // Write entry size first
    sizeBuf := make([]byte, 4)
    binary.BigEndian.PutUint32(sizeBuf, uint32(len(data)))

    if _, err := s.writer.Write(sizeBuf); err != nil {
        return &WalError{Op: "write_size", Err: err}
    }

    if _, err := s.writer.Write(data); err != nil {
        return &WalError{Op: "write_entry", Err: err}
    }

    s.offset += int64(len(sizeBuf) + len(data))
    return nil
}

func (s *segment) sync() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if err := s.writer.Flush(); err != nil {
        return &WalError{Op: "flush", Err: err}
    }

    if err := s.file.Sync(); err != nil {
        return &WalError{Op: "sync", Err: err}
    }

    return nil
}

func (s *segment) isFull() bool {
    return s.offset >= s.maxSize
}

func (s *segment) close() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if err := s.writer.Flush(); err != nil {
        return &WalError{Op: "flush", Err: err}
    }

    return s.file.Close()
}

func (s *segment) read() ([]*Entry, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if _, err := s.file.Seek(0, 0); err != nil {
        return nil, &WalError{Op: "seek", Err: err}
    }

    reader := bufio.NewReader(s.file)
    var entries []*Entry

    for {
        // Read entry size
        sizeBuf := make([]byte, 4)
        _, err := io.ReadFull(reader, sizeBuf)
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, &WalError{Op: "read_size", Err: err}
        }

        size := binary.BigEndian.Uint32(sizeBuf)
        
        // Read entry data
        data := make([]byte, size)
        if _, err := io.ReadFull(reader, data); err != nil {
            return nil, &WalError{Op: "read_entry", Err: err}
        }

        entry, err := DecodeEntry(data)
        if err != nil {
            return nil, &WalError{Op: "decode_entry", Err: err}
        }

        entries = append(entries, entry)
    }

    return entries, nil
} 