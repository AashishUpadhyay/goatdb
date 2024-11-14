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

type Manager struct {
	dir             string
	maxSegSize      int64
	activeSegment   *segment
	segments        []*segment
	mu              sync.RWMutex
	retentionPolicy *RetentionPolicy
}

func NewManager(dir string, maxSegSize int64) (*Manager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, &WalError{Op: "create_dir", Err: err}
	}

	m := &Manager{
		dir:        dir,
		maxSegSize: maxSegSize,
	}

	if err := m.recover(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Manager) Append(entry *Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSegment == nil || m.activeSegment.isFull() {
		if err := m.rotateSegment(); err != nil {
			return err
		}
	}

	if err := m.activeSegment.append(entry); err != nil {
		return err
	}

	return m.activeSegment.sync()
}

func (m *Manager) rotateSegment() error {
	if m.activeSegment != nil {
		if err := m.activeSegment.sync(); err != nil {
			return err
		}
	}

	segmentName := fmt.Sprintf("%020d.wal", time.Now().UnixNano())
	path := filepath.Join(m.dir, segmentName)

	segment, err := openSegment(path, m.maxSegSize)
	if err != nil {
		return err
	}

	if m.activeSegment != nil {
		m.segments = append(m.segments, m.activeSegment)
	}
	m.activeSegment = segment

	return nil
}

func (m *Manager) recover() error {
	files, err := os.ReadDir(m.dir)
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
		path := filepath.Join(m.dir, filename)
		segment, err := openSegment(path, m.maxSegSize)
		if err != nil {
			return err
		}
		m.segments = append(m.segments, segment)
	}

	if len(m.segments) > 0 {
		m.activeSegment = m.segments[len(m.segments)-1]
		m.segments = m.segments[:len(m.segments)-1]
	} else {
		return m.rotateSegment()
	}

	return nil
}

func (m *Manager) ReadAll() ([]*Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var allEntries []*Entry

	// Read from all inactive segments
	for _, segment := range m.segments {
		entries, err := segment.read()
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, entries...)
	}

	// Read from active segment
	if m.activeSegment != nil {
		entries, err := m.activeSegment.read()
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, segment := range m.segments {
		if err := segment.close(); err != nil {
			return err
		}
	}

	if m.activeSegment != nil {
		return m.activeSegment.close()
	}

	return nil
}

// RemoveOldSegments removes all segments except the active one
func (m *Manager) RemoveOldSegments() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close and remove old segments
	for _, seg := range m.segments {
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
	m.segments = nil
	return nil
}

func (m *Manager) ApplyRetentionPolicy() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.retentionPolicy == nil {
		return nil
	}

	// Remove segments based on count
	if m.retentionPolicy.MaxSegments > 0 {
		for len(m.segments) > m.retentionPolicy.MaxSegments {
			oldestSeg := m.segments[0]
			if err := m.removeSegment(oldestSeg); err != nil {
				return err
			}
			m.segments = m.segments[1:]
		}
	}

	// Remove segments based on age
	if m.retentionPolicy.MaxAge > 0 {
		cutoff := time.Now().Add(-m.retentionPolicy.MaxAge)
		for len(m.segments) > 0 {
			oldestSeg := m.segments[0]
			info, err := oldestSeg.file.Stat()
			if err != nil {
				return &WalError{Op: "stat_segment", Err: err}
			}

			if info.ModTime().Before(cutoff) {
				if err := m.removeSegment(oldestSeg); err != nil {
					return err
				}
				m.segments = m.segments[1:]
			} else {
				break
			}
		}
	}

	return nil
}

func (m *Manager) removeSegment(seg *segment) error {
	segPath := seg.file.Name()

	if err := seg.close(); err != nil {
		return &WalError{Op: "close_segment", Err: err}
	}

	if err := os.Remove(segPath); err != nil {
		return &WalError{Op: "remove_segment", Err: err}
	}

	return nil
}
