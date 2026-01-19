package storage

import (
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"time"

	"distributed-lock/internal/lock"
)

// PersistentEngineConfig configures the persistent engine
type PersistentEngineConfig struct {
	DataDir          string
	SnapshotInterval time.Duration
	WALSyncOnWrite   bool
}

// DefaultPersistentEngineConfig returns sensible defaults
func DefaultPersistentEngineConfig(dataDir string) *PersistentEngineConfig {
	return &PersistentEngineConfig{
		DataDir:          dataDir,
		SnapshotInterval: 5 * time.Minute,
		WALSyncOnWrite:   true,
	}
}

// PersistentEngine wraps a lock engine with persistence
type PersistentEngine struct {
	config        *PersistentEngineConfig
	engine        *lock.Engine
	wal           *WAL
	snapshotStore *SnapshotStore

	mu      sync.RWMutex
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
}

// NewPersistentEngine creates a new persistent engine
func NewPersistentEngine(config *PersistentEngineConfig) (*PersistentEngine, error) {
	// Create lock engine
	engine := lock.NewEngine()

	// Create WAL
	walConfig := DefaultWALConfig(filepath.Join(config.DataDir, "wal"))
	walConfig.SyncOnWrite = config.WALSyncOnWrite
	wal, err := NewWAL(walConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Create snapshot store
	snapshotConfig := DefaultSnapshotConfig(filepath.Join(config.DataDir, "snapshots"))
	snapshotConfig.SnapshotInterval = config.SnapshotInterval
	snapshotStore, err := NewSnapshotStore(snapshotConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	return &PersistentEngine{
		config:        config,
		engine:        engine,
		wal:           wal,
		snapshotStore: snapshotStore,
		stopCh:        make(chan struct{}),
	}, nil
}

// Start starts the persistent engine and recovers state
func (pe *PersistentEngine) Start() error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if pe.running {
		return nil
	}

	// Open WAL
	if err := pe.wal.Open(); err != nil {
		return fmt.Errorf("failed to open WAL: %w", err)
	}

	// Recover state
	if err := pe.recover(); err != nil {
		return fmt.Errorf("failed to recover state: %w", err)
	}

	// Start the underlying engine
	pe.engine.Start()

	// Set up expiration callback to log to WAL
	pe.engine.SetOnExpiredCallback(pe.onLockExpired)

	// Start periodic snapshotting
	if pe.config.SnapshotInterval > 0 {
		pe.wg.Add(1)
		go pe.snapshotLoop()
	}

	pe.running = true
	log.Printf("Persistent engine started, data dir: %s", pe.config.DataDir)
	return nil
}

// Stop stops the persistent engine
func (pe *PersistentEngine) Stop() error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if !pe.running {
		return nil
	}
	pe.running = false
	close(pe.stopCh)

	pe.wg.Wait()

	// Take final snapshot before stopping
	if err := pe.takeSnapshotInternal(); err != nil {
		log.Printf("Warning: failed to take final snapshot: %v", err)
	}

	pe.engine.Stop()
	pe.wal.Close()

	log.Println("Persistent engine stopped")
	return nil
}

// recover recovers state from snapshot and WAL
func (pe *PersistentEngine) recover() error {
	log.Println("Recovering state...")

	// Load latest snapshot
	snapshot, err := pe.snapshotStore.Load()
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	restoredCount := 0
	if snapshot != nil {
		log.Printf("Found snapshot: %s with %d locks", snapshot.Metadata.ID, snapshot.Metadata.LockCount)

		// Restore locks from snapshot
		now := time.Now()
		for _, ls := range snapshot.Locks {
			// Skip expired locks
			if ls.ExpiresAt.Before(now) {
				continue
			}

			// Restore lock
			resp := pe.engine.AcquireLock(&lock.AcquireRequest{
				Key:   ls.Key,
				Owner: ls.Owner,
				TTL:   time.Duration(ls.TTL),
			})
			if resp.Success {
				restoredCount++
			}
		}
		log.Printf("Restored %d locks from snapshot", restoredCount)
	}

	// Replay WAL entries after snapshot
	walEntries, err := pe.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	if len(walEntries) > 0 {
		log.Printf("Replaying %d WAL entries...", len(walEntries))

		startSeq := uint64(0)
		if snapshot != nil {
			startSeq = snapshot.Metadata.WALSequence
		}

		replayed := 0
		for _, entry := range walEntries {
			// Skip entries already in snapshot
			if entry.Sequence <= startSeq {
				continue
			}

			pe.replayEntry(entry)
			replayed++
		}
		log.Printf("Replayed %d WAL entries", replayed)
	}

	log.Printf("Recovery complete: %d locks active", len(pe.engine.ListLocks()))
	return nil
}

// replayEntry replays a single WAL entry
func (pe *PersistentEngine) replayEntry(entry *WALEntry) {
	now := time.Now()

	switch entry.Type {
	case WALAcquire:
		// Only restore if not expired
		if entry.ExpiresAt.After(now) {
			pe.engine.AcquireLock(&lock.AcquireRequest{
				Key:   entry.Key,
				Owner: entry.Owner,
				TTL:   time.Duration(entry.TTL),
			})
		}

	case WALRenew:
		if entry.ExpiresAt.After(now) {
			pe.engine.RenewLock(&lock.RenewRequest{
				Key:   entry.Key,
				Owner: entry.Owner,
				TTL:   time.Duration(entry.TTL),
			})
		}

	case WALRelease, WALExpire:
		pe.engine.ReleaseLock(&lock.ReleaseRequest{
			Key:   entry.Key,
			Owner: entry.Owner,
		})
	}
}

// snapshotLoop periodically takes snapshots
func (pe *PersistentEngine) snapshotLoop() {
	defer pe.wg.Done()

	ticker := time.NewTicker(pe.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := pe.TakeSnapshot(); err != nil {
				log.Printf("Failed to take snapshot: %v", err)
			}
		case <-pe.stopCh:
			return
		}
	}
}

// onLockExpired is called when a lock expires
func (pe *PersistentEngine) onLockExpired(l *lock.Lock) {
	pe.wal.LogExpire(l.Key, l.Owner)
}

// TakeSnapshot takes a snapshot of current state
func (pe *PersistentEngine) TakeSnapshot() error {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	return pe.takeSnapshotInternal()
}

// takeSnapshotInternal takes a snapshot (must hold lock)
func (pe *PersistentEngine) takeSnapshotInternal() error {
	locks := pe.engine.ListLocks()

	snapshot := &Snapshot{
		Metadata: SnapshotMetadata{
			WALSequence: pe.wal.GetSequence(),
		},
		Locks: make([]*LockSnapshot, 0, len(locks)),
	}

	for _, l := range locks {
		snapshot.Locks = append(snapshot.Locks, &LockSnapshot{
			Key:       l.Key,
			Owner:     l.Owner,
			TTL:       int64(l.TTL),
			ExpiresAt: l.ExpiresAt,
			CreatedAt: l.CreatedAt,
			Version:   l.Version,
		})
	}

	if err := pe.snapshotStore.Save(snapshot); err != nil {
		return err
	}

	// Truncate WAL after successful snapshot
	if err := pe.wal.Truncate(); err != nil {
		log.Printf("Warning: failed to truncate WAL: %v", err)
	}

	log.Printf("Snapshot taken: %d locks", len(locks))
	return nil
}

// AcquireLock acquires a lock with persistence
func (pe *PersistentEngine) AcquireLock(req *lock.AcquireRequest) *lock.AcquireResponse {
	resp := pe.engine.AcquireLock(req)
	if resp.Success {
		pe.wal.LogAcquire(req.Key, req.Owner, req.TTL, resp.Lock.ExpiresAt)
	}
	return resp
}

// RenewLock renews a lock with persistence
func (pe *PersistentEngine) RenewLock(req *lock.RenewRequest) *lock.RenewResponse {
	resp := pe.engine.RenewLock(req)
	if resp.Success {
		pe.wal.LogRenew(req.Key, req.Owner, req.TTL, resp.Lock.ExpiresAt)
	}
	return resp
}

// ReleaseLock releases a lock with persistence
func (pe *PersistentEngine) ReleaseLock(req *lock.ReleaseRequest) *lock.ReleaseResponse {
	resp := pe.engine.ReleaseLock(req)
	if resp.Success {
		pe.wal.LogRelease(req.Key, req.Owner)
	}
	return resp
}

// GetLock gets lock information
func (pe *PersistentEngine) GetLock(key string) (*lock.Lock, error) {
	return pe.engine.GetLock(key)
}

// ListLocks lists all active locks
func (pe *PersistentEngine) ListLocks() []*lock.Lock {
	return pe.engine.ListLocks()
}

// GetEngine returns the underlying lock engine
func (pe *PersistentEngine) GetEngine() *lock.Engine {
	return pe.engine
}

// ReadWAL reads all WAL entries (for debugging/inspection)
func (pe *PersistentEngine) ReadWAL() ([]*WALEntry, error) {
	return pe.wal.ReadAll()
}

// ListSnapshots lists all snapshots
func (pe *PersistentEngine) ListSnapshots() ([]SnapshotMetadata, error) {
	return pe.snapshotStore.List()
}

// RecoverFromFile recovers from a specific WAL file
func RecoverFromFile(filename string) ([]*WALEntry, error) {
	reader, err := NewWALReader(filename)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, nil
	}
	defer reader.Close()

	var entries []*WALEntry
	for {
		entry, err := reader.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // Skip corrupt entries
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
