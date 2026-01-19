package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"distributed-lock/internal/storage"
)

// FilePersistence implements Persistence using the file system
type FilePersistence struct {
	mu sync.RWMutex

	dir           string
	stateFile     string
	snapshotStore *storage.SnapshotStore
	wal           *storage.WAL
}

// NewFilePersistence creates a new file-based persistence layer
func NewFilePersistence(dir string) (*FilePersistence, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create persistence directory: %w", err)
	}

	// Create WAL
	walConfig := storage.DefaultWALConfig(filepath.Join(dir, "wal"))
	wal, err := storage.NewWAL(walConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}
	if err := wal.Open(); err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	// Create snapshot store
	snapshotConfig := storage.DefaultSnapshotConfig(filepath.Join(dir, "snapshots"))
	snapshotStore, err := storage.NewSnapshotStore(snapshotConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	return &FilePersistence{
		dir:           dir,
		stateFile:     filepath.Join(dir, "raft_state.json"),
		snapshotStore: snapshotStore,
		wal:           wal,
	}, nil
}

// RaftState represents persistent Raft state
type RaftState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// SaveState saves the persistent Raft state
func (fp *FilePersistence) SaveState(term uint64, votedFor string) error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	state := RaftState{
		Term:     term,
		VotedFor: votedFor,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(fp.stateFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

// LoadState loads the persistent Raft state
func (fp *FilePersistence) LoadState() (term uint64, votedFor string, err error) {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	data, err := os.ReadFile(fp.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil
		}
		return 0, "", fmt.Errorf("failed to read state file: %w", err)
	}

	var state RaftState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, "", fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return state.Term, state.VotedFor, nil
}

// LogEntryWAL represents a log entry in WAL format
type LogEntryWAL struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command"`
}

// SaveLog saves log entries
func (fp *FilePersistence) SaveLog(entries []LogEntry) error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	for _, entry := range entries {
		walEntry := &storage.WALEntry{
			Type: "raft_log",
			Key:  fmt.Sprintf("log_%d", entry.Index),
		}

		// Encode the full entry in the Key field is a hack,
		// we should extend WALEntry for Raft
		data, err := json.Marshal(LogEntryWAL{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		})
		if err != nil {
			return err
		}
		walEntry.Owner = string(data)

		if err := fp.wal.Append(walEntry); err != nil {
			return fmt.Errorf("failed to append to WAL: %w", err)
		}
	}

	return nil
}

// LoadLog loads all log entries
func (fp *FilePersistence) LoadLog() ([]LogEntry, error) {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	walEntries, err := fp.wal.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}

	entries := make([]LogEntry, 0)
	for _, walEntry := range walEntries {
		if walEntry.Type != "raft_log" {
			continue
		}

		var logEntry LogEntryWAL
		if err := json.Unmarshal([]byte(walEntry.Owner), &logEntry); err != nil {
			continue
		}

		entries = append(entries, LogEntry{
			Index:   logEntry.Index,
			Term:    logEntry.Term,
			Command: logEntry.Command,
		})
	}

	return entries, nil
}

// RaftSnapshot represents a Raft snapshot with state machine data
type RaftSnapshot struct {
	LastIndex uint64    `json:"last_index"`
	LastTerm  uint64    `json:"last_term"`
	RaftState RaftState `json:"raft_state"`
	Data      []byte    `json:"data"`
}

// SaveSnapshot saves a snapshot
func (fp *FilePersistence) SaveSnapshot(snapshot *Snapshot) error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Convert to storage snapshot format
	raftSnapshot := &RaftSnapshot{
		LastIndex: snapshot.LastIndex,
		LastTerm:  snapshot.LastTerm,
		Data:      snapshot.Data,
	}

	data, err := json.Marshal(raftSnapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	storageSnapshot := &storage.Snapshot{
		Locks: nil, // Not used for Raft snapshots
	}
	storageSnapshot.Metadata.WALSequence = snapshot.LastIndex

	// Save to file
	snapshotFile := filepath.Join(fp.dir, "snapshots", fmt.Sprintf("raft_snap_%d.json", snapshot.LastIndex))
	if err := os.WriteFile(snapshotFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	// Truncate WAL
	fp.wal.Truncate()

	return nil
}

// LoadSnapshot loads the latest snapshot
func (fp *FilePersistence) LoadSnapshot() (*Snapshot, error) {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	// Find latest snapshot file
	snapshotDir := filepath.Join(fp.dir, "snapshots")
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var latestFile string
	var latestIndex uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var idx uint64
		if _, err := fmt.Sscanf(entry.Name(), "raft_snap_%d.json", &idx); err == nil {
			if idx > latestIndex {
				latestIndex = idx
				latestFile = filepath.Join(snapshotDir, entry.Name())
			}
		}
	}

	if latestFile == "" {
		return nil, nil
	}

	data, err := os.ReadFile(latestFile)
	if err != nil {
		return nil, err
	}

	var raftSnapshot RaftSnapshot
	if err := json.Unmarshal(data, &raftSnapshot); err != nil {
		return nil, err
	}

	return &Snapshot{
		LastIndex: raftSnapshot.LastIndex,
		LastTerm:  raftSnapshot.LastTerm,
		Data:      raftSnapshot.Data,
	}, nil
}

// Close closes the persistence layer
func (fp *FilePersistence) Close() error {
	return fp.wal.Close()
}
