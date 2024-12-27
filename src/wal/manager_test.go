package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	// Create a temporary directory for tests
	tempDir := t.TempDir()

	tests := []struct {
		name            string
		Dir             string
		MaxSegSize      int64
		RetentionPolicy *RetentionPolicy
		wantErr         bool
		errorOp         string
	}{
		{
			name:       "successful creation",
			Dir:        filepath.Join(tempDir, "wal1"),
			MaxSegSize: 1024,
			RetentionPolicy: &RetentionPolicy{
				MaxSegments: 5,
				MaxAge:      24 * time.Hour,
			},
			wantErr: false,
		},
		{
			name:       "successful creation without retention policy",
			Dir:        filepath.Join(tempDir, "wal2"),
			MaxSegSize: 1024,
			wantErr:    false,
		},
		{
			name:       "invalid directory permissions",
			Dir:        "/root/invalid", // This should fail due to permissions
			MaxSegSize: 1024,
			wantErr:    true,
			errorOp:    "create_dir",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing directory
			_ = os.RemoveAll(tt.Dir)

			manager, err := NewManager(tt.Dir, tt.MaxSegSize, tt.RetentionPolicy)

			// Check error expectations
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewManager() error = nil, wantErr = true")
					return
				}
				if walErr, ok := err.(*WalError); ok && walErr.Op != tt.errorOp {
					t.Errorf("NewManager() error operation = %v, want %v", walErr.Op, tt.errorOp)
				}
				return
			}

			if err != nil {
				t.Errorf("NewManager() unexpected error = %v", err)
				return
			}

			// Verify manager properties
			if manager.Dir != tt.Dir {
				t.Errorf("manager.dir = %v, want %v", manager.Dir, tt.Dir)
			}
			if manager.MaxSegSize != tt.MaxSegSize {
				t.Errorf("manager.maxSegSize = %v, want %v", manager.MaxSegSize, tt.MaxSegSize)
			}
			if manager.RetentionPolicy != tt.RetentionPolicy {
				t.Errorf("manager.retentionPolicy = %v, want %v", manager.RetentionPolicy, tt.RetentionPolicy)
			}

			// Verify directory was created
			if _, err := os.Stat(tt.Dir); os.IsNotExist(err) {
				t.Errorf("Directory was not created: %v", tt.Dir)
			}

			// Verify initial segment was created
			if manager.activeSegment == nil {
				t.Error("Active segment was not created")
			}

			// Clean up
			if err := manager.Close(); err != nil {
				t.Errorf("Failed to close manager: %v", err)
			}
		})
	}
}

func TestManager_Append(t *testing.T) {
	tests := []struct {
		name    string
		entries []*Entry
		size    int64
	}{
		{
			name: "append single entry",
			entries: []*Entry{
				{
					Type:  EntryPut,
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
			size: 1024,
		},
		{
			name: "appendmultiple entries",
			entries: []*Entry{
				{
					Type:  EntryPut,
					Key:   []byte("k1"),
					Value: []byte("v1"),
				},
				{
					Type:  EntryPut,
					Key:   []byte("k2"),
					Value: []byte("v2"),
				},
				{
					Type:  EntryPut,
					Key:   []byte("k3"),
					Value: []byte("v3"),
				},
			},
			size: 1024,
		},
		{
			name: "append multiple entries and test for rotation",
			entries: []*Entry{
				{
					Type:  EntryPut,
					Key:   []byte("k1"),
					Value: []byte("v1"),
				},
				{
					Type:  EntryPut,
					Key:   []byte("k2"),
					Value: []byte("v2"),
				},
				{
					Type:  EntryPut,
					Key:   []byte("k3"),
					Value: []byte("v3"),
				},
			},
			size: 10,
		},
	}

	for _, tst := range tests {
		walDir, err := os.MkdirTemp("", "wal_test_*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(walDir)

		walManager, err := NewManager(walDir, tst.size, nil)
		if err != nil {
			t.Errorf("Failed to create WAL manager: %v", err)
		}
		for _, tst_entr := range tst.entries {
			err := walManager.Append(tst_entr)
			if err != nil {
				t.Errorf("Failed to append entry: %v", err)
			}
		}

		if tst.name == "append multiple entries and test for rotation" {
			if len(walManager.segments) == 0 {
				t.Errorf("Expected multiple segments, got %v", len(walManager.segments))
			}
		}

		entries_read, err := walManager.ReadAll()
		if err != nil {
			t.Errorf("Failed to read all entries: %v", err)
		}
		if len(entries_read) != len(tst.entries) {
			t.Errorf("Expected %v entries, got %v", len(tst.entries), len(entries_read))
		}

		for i, want := range tst.entries {
			if !bytes.Equal(want.Value, entries_read[i].Value) {
				t.Errorf("Entry %d: got %s, want %s", i, entries_read[i].Value, want.Value)
			}
		}
		walManager.Close()
	}
}

func TestManager_RemoveOldSegments(t *testing.T) {
	// Create temp directory
	walDir := t.TempDir()

	// Create manager with retention policy
	retentionPolicy := &RetentionPolicy{
		MaxSegments: 2,
		MaxAge:      time.Hour,
	}
	walManager, err := NewManager(walDir, 10, retentionPolicy)
	if err != nil {
		t.Fatal(err) // Use Fatal for setup errors
	}
	defer walManager.Close()

	// Add enough entries to create multiple segments
	entries := []*Entry{
		{Type: EntryPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: EntryPut, Key: []byte("k2"), Value: []byte("v2")},
		{Type: EntryPut, Key: []byte("k3"), Value: []byte("v3")},
	}

	for _, entry := range entries {
		if err := walManager.Append(entry); err != nil {
			t.Fatal(err)
		}
	}

	// Verify initial state
	initialSegmentCount := len(walManager.segments)
	if initialSegmentCount == 0 {
		t.Fatal("Expected multiple segments to be created")
	}

	// Remove old segments
	if err := walManager.RemoveOldSegments(); err != nil {
		t.Fatal(err)
	}

	// Verify final state
	if len(walManager.segments) >= initialSegmentCount {
		t.Errorf("Expected fewer segments after removal, got %d, had %d",
			len(walManager.segments), initialSegmentCount)
	}
	if walManager.activeSegment == nil {
		t.Error("Active segment should not be nil after removal")
	}
}

func TestManager_Close(t *testing.T) {
	// Create temporary directory for WAL files
	tmpDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("successful close", func(t *testing.T) {
		// Initialize manager
		manager, err := NewManager(tmpDir, 1024, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Add some entries to create segments
		entries := []*Entry{
			{Type: EntryPut, Key: []byte("k1"), Value: []byte("v1")},
			{Type: EntryPut, Key: []byte("k2"), Value: []byte("v2")},
		}
		for _, entry := range entries {
			if err := manager.Append(entry); err != nil {
				t.Fatal(err)
			}
		}

		// Close manager
		if err := manager.Close(); err != nil {
			t.Errorf("expected successful close, got error: %v", err)
		}

		// Verify segments are closed by attempting to write
		if err := manager.Append(&Entry{Type: EntryPut, Key: []byte("k3"), Value: []byte("v3")}); err == nil {
			t.Error("expected error writing to closed manager, got nil")
		}
	})

	t.Run("double close", func(t *testing.T) {
		// Initialize manager
		manager, err := NewManager(tmpDir, 1024, nil)
		if err != nil {
			t.Fatal(err)
		}

		// First close should succeed
		if err := manager.Close(); err != nil {
			t.Errorf("expected successful first close, got error: %v", err)
		}

		// Second close should also succeed (idempotent)
		if err := manager.Close(); err != nil {
			t.Errorf("expected successful second close, got error: %v", err)
		}
	})
}

func TestManager_MaxSegmentsApplyRetentionPolicy(t *testing.T) {

	tests := []struct {
		name        string
		maxSegments int
		maxAge      time.Duration
	}{
		{name: "apply max segments retention policy", maxSegments: 1, maxAge: 1 * time.Hour},
		{name: "apply max age retention policy", maxSegments: 100, maxAge: 1 * time.Hour},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			currentTestDir, _ := os.Getwd()
			walDir := filepath.Join(currentTestDir, ".wal")
			defer deleteDirectoryIfExists(walDir)

			walManager, err := NewManager(walDir, 100, &RetentionPolicy{
				MaxSegments: test.maxSegments,
				MaxAge:      test.maxAge,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer walManager.Close()

			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k1"), Value: []byte("v1")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k2"), Value: []byte("v2")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k3"), Value: []byte("v3")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k4"), Value: []byte("v4")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k5"), Value: []byte("v5")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k6"), Value: []byte("v6")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k7"), Value: []byte("v7")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k8"), Value: []byte("v8")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k9"), Value: []byte("v9")})
			walManager.Append(&Entry{Type: EntryPut, Key: []byte("k10"), Value: []byte("v10")})

			if test.name == "apply max age retention policy" {
				for _, seg := range walManager.segments {
					os.Chtimes(seg.file.Name(), time.Now().Add(-2*time.Hour), time.Now().Add(-2*time.Hour))
				}
			}

			walManager.ApplyRetentionPolicy()

			if test.name == "apply max segments retention policy" && len(walManager.segments) != 1 {
				t.Errorf("expected 1 segment, got %v", len(walManager.segments))
			}

			if test.name == "apply max age retention policy" && len(walManager.segments) != 0 {
				t.Errorf("expected 0 segments, got %v", len(walManager.segments))
			}
		})
	}
}

func deleteDirectoryIfExists(dirPath string) error {
	err := os.RemoveAll(dirPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error deleting directory: %w", err)
	}
	return nil
}
