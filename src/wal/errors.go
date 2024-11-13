package wal

import "fmt"

type WalError struct {
    Op  string
    Err error
}

func (e *WalError) Error() string {
    return fmt.Sprintf("wal %s error: %v", e.Op, e.Err)
}

func (e *WalError) Unwrap() error {
    return e.Err
}

var (
    ErrCorruptedEntry   = fmt.Errorf("corrupted wal entry")
    ErrChecksumMismatch = fmt.Errorf("checksum mismatch")
    ErrSegmentNotFound  = fmt.Errorf("segment not found")
) 