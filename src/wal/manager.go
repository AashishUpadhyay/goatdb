package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type RetentionPolicy struct {
	MaxSegments int           // Maximum number of segments to keep
	MaxAge      time.Duration // Maximum age of segments
}

type WalManager struct {
	Dir             string
	MaxSegSize      int64
	activeSegment   *segment
	segments        []*segment
	mu              sync.RWMutex
	RetentionPolicy *RetentionPolicy
}

type IWalManager interface {
	Append(entry *Entry) error
	ReadAll() ([]*Entry, error)
	Close() error
	RemoveOldSegments() error
	ApplyRetentionPolicy() error
}

func NewManager(dir string, maxSegSize int64, retentionPolicy *RetentionPolicy) (*WalManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, &WalError{Op: "create_dir", Err: err}
	}

	m := &WalManager{
		Dir:             dir,
		MaxSegSize:      maxSegSize,
		RetentionPolicy: retentionPolicy,
	}

	if err := m.recover(); err != nil {
		return nil, err
	}

	return m, nil
}

func (wm *WalManager) Append(entry *Entry) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.activeSegment == nil || wm.activeSegment.isFull() {
		if err := wm.rotateSegment(); err != nil {
			return err
		}
	}

	if err := wm.activeSegment.append(entry); err != nil {
		return err
	}

	return wm.activeSegment.sync()
}

func (wm *WalManager) rotateSegment() error {
	if wm.activeSegment != nil {
		if err := wm.activeSegment.sync(); err != nil {
			return err
		}
	}

	segmentName := fmt.Sprintf("%020d.wal", time.Now().UnixNano())
	path := filepath.Join(wm.Dir, segmentName)

	segment, err := openSegment(path, wm.MaxSegSize)
	if err != nil {
		return err
	}

	if wm.activeSegment != nil {
		wm.segments = append(wm.segments, wm.activeSegment)
	}
	wm.activeSegment = segment

	return nil
}

func (wm *WalManager) recover() error {
	files, err := os.ReadDir(wm.Dir)
	if err != nil {
		return &WalError{Op: "read_dir", Err: err}
	}

	var segmentFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".wal") {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}

	sort.Strings(segmentFiles)

	for _, filename := range segmentFiles {
		path := filepath.Join(wm.Dir, filename)
		segment, err := openSegment(path, wm.MaxSegSize)
		if err != nil {
			return err
		}
		wm.segments = append(wm.segments, segment)
	}

	if len(wm.segments) > 0 {
		wm.activeSegment = wm.segments[len(wm.segments)-1]
		wm.segments = wm.segments[:len(wm.segments)-1]
	} else {
		return wm.rotateSegment()
	}

	return nil
}

func (wm *WalManager) ReadAll() ([]*Entry, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var allEntries []*Entry

	// Read from all inactive segments
	for _, segment := range wm.segments {
		entries, err := segment.read()
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, entries...)
	}

	// Read from active segment
	if wm.activeSegment != nil {
		entries, err := wm.activeSegment.read()
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

func (wm *WalManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for _, segment := range wm.segments {
		if err := segment.close(); err != nil {
			return err
		}
	}

	if wm.activeSegment != nil {
		return wm.activeSegment.close()
	}

	return nil
}

// RemoveOldSegments removes all segments except the active one
func (wm *WalManager) RemoveOldSegments() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Close and remove old segments
	for _, seg := range wm.segments {
		segPath := seg.file.Name()

		// Close segment
		if err := seg.close(); err != nil {
			return &WalError{Op: "close_segment", Err: err}
		}

		// Remove file
		if err := os.Remove(segPath); err != nil {
			return &WalError{Op: "remove_segment", Err: err}
		}
	}

	// Clear segments slice
	wm.segments = nil
	return nil
}

func (wm *WalManager) ApplyRetentionPolicy() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.RetentionPolicy == nil {
		return nil
	}

	// Remove segments based on count
	if wm.RetentionPolicy.MaxSegments > 0 {
		for len(wm.segments) > wm.RetentionPolicy.MaxSegments {
			oldestSeg := wm.segments[0]
			if err := wm.removeSegment(oldestSeg); err != nil {
				return err
			}
			wm.segments = wm.segments[1:]
		}
	}

	// Remove segments based on age
	if wm.RetentionPolicy.MaxAge > 0 {
		cutoff := time.Now().Add(-wm.RetentionPolicy.MaxAge)
		for len(wm.segments) > 0 {
			oldestSeg := wm.segments[0]
			info, err := oldestSeg.file.Stat()
			if err != nil {
				return &WalError{Op: "stat_segment", Err: err}
			}

			if info.ModTime().Before(cutoff) {
				if err := wm.removeSegment(oldestSeg); err != nil {
					return err
				}
				wm.segments = wm.segments[1:]
			} else {
				break
			}
		}
	}

	return nil
}

func (m *WalManager) removeSegment(seg *segment) error {
	segPath := seg.file.Name()

	if err := seg.close(); err != nil {
		return &WalError{Op: "close_segment", Err: err}
	}

	if err := os.Remove(segPath); err != nil {
		return &WalError{Op: "remove_segment", Err: err}
	}

	return nil
}
