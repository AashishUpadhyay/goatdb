package wal

import (
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
		dir             string
		maxSegSize      int64
		retentionPolicy *RetentionPolicy
		wantErr         bool
		errorOp         string
	}{
		{
			name:       "successful creation",
			dir:        filepath.Join(tempDir, "wal1"),
			maxSegSize: 1024,
			retentionPolicy: &RetentionPolicy{
				MaxSegments: 5,
				MaxAge:      24 * time.Hour,
			},
			wantErr: false,
		},
		{
			name:       "successful creation without retention policy",
			dir:        filepath.Join(tempDir, "wal2"),
			maxSegSize: 1024,
			wantErr:    false,
		},
		{
			name:       "invalid directory permissions",
			dir:        "/root/invalid", // This should fail due to permissions
			maxSegSize: 1024,
			wantErr:    true,
			errorOp:    "create_dir",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing directory
			_ = os.RemoveAll(tt.dir)

			manager, err := NewManager(tt.dir, tt.maxSegSize, tt.retentionPolicy)

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
			if manager.dir != tt.dir {
				t.Errorf("manager.dir = %v, want %v", manager.dir, tt.dir)
			}
			if manager.maxSegSize != tt.maxSegSize {
				t.Errorf("manager.maxSegSize = %v, want %v", manager.maxSegSize, tt.maxSegSize)
			}
			if manager.retentionPolicy != tt.retentionPolicy {
				t.Errorf("manager.retentionPolicy = %v, want %v", manager.retentionPolicy, tt.retentionPolicy)
			}

			// Verify directory was created
			if _, err := os.Stat(tt.dir); os.IsNotExist(err) {
				t.Errorf("Directory was not created: %v", tt.dir)
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
	walDir := t.TempDir()

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
		walManager.Close()
	}
}
