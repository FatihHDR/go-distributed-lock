package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// SnapshotMetadata contains metadata about a snapshot
type SnapshotMetadata struct {
	ID          string    `json:"id"`
	CreatedAt   time.Time `json:"created_at"`
	WALSequence uint64    `json:"wal_sequence"`
	LockCount   int       `json:"lock_count"`
	Version     int       `json:"version"`
}

// LockSnapshot represents a lock in a snapshot
type LockSnapshot struct {
	Key       string    `json:"key"`
	Owner     string    `json:"owner"`
	TTL       int64     `json:"ttl"` // nanoseconds
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
	Version   uint64    `json:"version"`
}

// Snapshot represents a complete state snapshot
type Snapshot struct {
	Metadata SnapshotMetadata `json:"metadata"`
	Locks    []*LockSnapshot  `json:"locks"`
}

// SnapshotConfig configures snapshot behavior
type SnapshotConfig struct {
	Dir              string
	MaxSnapshots     int           // Max snapshots to keep
	SnapshotInterval time.Duration // Auto-snapshot interval
}

// DefaultSnapshotConfig returns sensible defaults
func DefaultSnapshotConfig(dir string) *SnapshotConfig {
	return &SnapshotConfig{
		Dir:              dir,
		MaxSnapshots:     3,
		SnapshotInterval: 5 * time.Minute,
	}
}

// SnapshotStore manages snapshots
type SnapshotStore struct {
	config *SnapshotConfig
	mu     sync.RWMutex
}

// NewSnapshotStore creates a new snapshot store
func NewSnapshotStore(config *SnapshotConfig) (*SnapshotStore, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &SnapshotStore{
		config: config,
	}, nil
}

// Save saves a snapshot to disk
func (s *SnapshotStore) Save(snapshot *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate snapshot ID if not set
	if snapshot.Metadata.ID == "" {
		snapshot.Metadata.ID = fmt.Sprintf("snap-%d", time.Now().UnixNano())
	}
	snapshot.Metadata.CreatedAt = time.Now()
	snapshot.Metadata.LockCount = len(snapshot.Locks)
	snapshot.Metadata.Version = 1

	// Marshal snapshot
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Write to file
	filename := filepath.Join(s.config.Dir, snapshot.Metadata.ID+".json")
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	// Clean up old snapshots
	s.cleanupOldSnapshots()

	return nil
}

// Load loads the latest snapshot
func (s *SnapshotStore) Load() (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshots, err := s.listSnapshots()
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, nil // No snapshots exist
	}

	// Get the latest (last in sorted list)
	latest := snapshots[len(snapshots)-1]
	return s.loadSnapshot(latest)
}

// LoadByID loads a specific snapshot by ID
func (s *SnapshotStore) LoadByID(id string) (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filename := filepath.Join(s.config.Dir, id+".json")
	return s.loadSnapshotFile(filename)
}

// listSnapshots returns sorted list of snapshot IDs
func (s *SnapshotStore) listSnapshots() ([]string, error) {
	entries, err := os.ReadDir(s.config.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var snapshots []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "snap-") && strings.HasSuffix(entry.Name(), ".json") {
			id := strings.TrimSuffix(entry.Name(), ".json")
			snapshots = append(snapshots, id)
		}
	}

	// Sort by name (which includes timestamp)
	sort.Strings(snapshots)
	return snapshots, nil
}

// loadSnapshot loads a snapshot by ID
func (s *SnapshotStore) loadSnapshot(id string) (*Snapshot, error) {
	filename := filepath.Join(s.config.Dir, id+".json")
	return s.loadSnapshotFile(filename)
}

// loadSnapshotFile loads a snapshot from a file
func (s *SnapshotStore) loadSnapshotFile(filename string) (*Snapshot, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return &snapshot, nil
}

// cleanupOldSnapshots removes snapshots exceeding MaxSnapshots
func (s *SnapshotStore) cleanupOldSnapshots() {
	snapshots, err := s.listSnapshots()
	if err != nil {
		return
	}

	// Remove oldest snapshots if exceeding max
	for len(snapshots) > s.config.MaxSnapshots {
		oldest := snapshots[0]
		filename := filepath.Join(s.config.Dir, oldest+".json")
		os.Remove(filename)
		snapshots = snapshots[1:]
	}
}

// List returns metadata for all snapshots
func (s *SnapshotStore) List() ([]SnapshotMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids, err := s.listSnapshots()
	if err != nil {
		return nil, err
	}

	var result []SnapshotMetadata
	for _, id := range ids {
		snapshot, err := s.loadSnapshot(id)
		if err != nil {
			continue
		}
		result = append(result, snapshot.Metadata)
	}

	return result, nil
}

// Delete deletes a snapshot by ID
func (s *SnapshotStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filename := filepath.Join(s.config.Dir, id+".json")
	return os.Remove(filename)
}
