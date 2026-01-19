package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"distributed-lock/internal/lock"
)

func TestWAL_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	config := DefaultWALConfig(dir)
	config.SyncOnWrite = false // Faster for tests

	wal, err := NewWAL(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Append entries
	expiresAt := time.Now().Add(1 * time.Hour)

	if err := wal.LogAcquire("key1", "owner1", 30*time.Second, expiresAt); err != nil {
		t.Fatalf("Failed to log acquire: %v", err)
	}
	if err := wal.LogRenew("key1", "owner1", 60*time.Second, expiresAt); err != nil {
		t.Fatalf("Failed to log renew: %v", err)
	}
	if err := wal.LogRelease("key1", "owner1"); err != nil {
		t.Fatalf("Failed to log release: %v", err)
	}

	// Read all entries
	entries, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read WAL: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	if entries[0].Type != WALAcquire {
		t.Errorf("Expected acquire, got %s", entries[0].Type)
	}
	if entries[1].Type != WALRenew {
		t.Errorf("Expected renew, got %s", entries[1].Type)
	}
	if entries[2].Type != WALRelease {
		t.Errorf("Expected release, got %s", entries[2].Type)
	}
}

func TestSnapshot_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSnapshotConfig(dir)

	store, err := NewSnapshotStore(config)
	if err != nil {
		t.Fatalf("Failed to create snapshot store: %v", err)
	}

	// Create snapshot
	snapshot := &Snapshot{
		Locks: []*LockSnapshot{
			{
				Key:       "key1",
				Owner:     "owner1",
				TTL:       int64(30 * time.Second),
				ExpiresAt: time.Now().Add(30 * time.Second),
				CreatedAt: time.Now(),
				Version:   1,
			},
			{
				Key:       "key2",
				Owner:     "owner2",
				TTL:       int64(60 * time.Second),
				ExpiresAt: time.Now().Add(60 * time.Second),
				CreatedAt: time.Now(),
				Version:   2,
			},
		},
	}

	// Save
	if err := store.Save(snapshot); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Load
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if loaded == nil {
		t.Fatal("Loaded snapshot is nil")
	}

	if len(loaded.Locks) != 2 {
		t.Errorf("Expected 2 locks, got %d", len(loaded.Locks))
	}

	if loaded.Locks[0].Key != "key1" {
		t.Errorf("Expected key1, got %s", loaded.Locks[0].Key)
	}
}

func TestPersistentEngine_Recovery(t *testing.T) {
	dir := t.TempDir()
	config := DefaultPersistentEngineConfig(dir)
	config.SnapshotInterval = 0 // Disable auto-snapshot

	// Create and start engine
	pe, err := NewPersistentEngine(config)
	if err != nil {
		t.Fatalf("Failed to create persistent engine: %v", err)
	}

	if err := pe.Start(); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}

	// Acquire some locks
	resp := pe.AcquireLock(&lock.AcquireRequest{
		Key:   "persistent-key-1",
		Owner: "owner-1",
		TTL:   1 * time.Hour,
	})
	if !resp.Success {
		t.Fatalf("Failed to acquire lock: %s", resp.Error)
	}

	resp = pe.AcquireLock(&lock.AcquireRequest{
		Key:   "persistent-key-2",
		Owner: "owner-2",
		TTL:   1 * time.Hour,
	})
	if !resp.Success {
		t.Fatalf("Failed to acquire lock: %s", resp.Error)
	}

	// Verify locks exist
	locks := pe.ListLocks()
	if len(locks) != 2 {
		t.Errorf("Expected 2 locks, got %d", len(locks))
	}

	// Stop engine
	pe.Stop()

	// Create new engine with same directory (simulating restart)
	pe2, err := NewPersistentEngine(config)
	if err != nil {
		t.Fatalf("Failed to create second engine: %v", err)
	}

	if err := pe2.Start(); err != nil {
		t.Fatalf("Failed to start second engine: %v", err)
	}
	defer pe2.Stop()

	// Verify locks were recovered
	locks = pe2.ListLocks()
	if len(locks) != 2 {
		t.Errorf("Expected 2 recovered locks, got %d", len(locks))
	}

	// Verify lock details
	lock1, err := pe2.GetLock("persistent-key-1")
	if err != nil {
		t.Errorf("Failed to get lock 1: %v", err)
	} else if lock1.Owner != "owner-1" {
		t.Errorf("Expected owner-1, got %s", lock1.Owner)
	}
}

func TestPersistentEngine_SnapshotAndWAL(t *testing.T) {
	dir := t.TempDir()
	config := DefaultPersistentEngineConfig(dir)
	config.SnapshotInterval = 0 // Manual snapshots

	pe, err := NewPersistentEngine(config)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	if err := pe.Start(); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}

	// Acquire lock 1
	pe.AcquireLock(&lock.AcquireRequest{
		Key:   "key1",
		Owner: "owner1",
		TTL:   1 * time.Hour,
	})

	// Take snapshot
	if err := pe.TakeSnapshot(); err != nil {
		t.Fatalf("Failed to take snapshot: %v", err)
	}

	// Acquire lock 2 (after snapshot)
	pe.AcquireLock(&lock.AcquireRequest{
		Key:   "key2",
		Owner: "owner2",
		TTL:   1 * time.Hour,
	})

	pe.Stop()

	// Restart - should recover both locks (1 from snapshot, 2 from WAL)
	pe2, _ := NewPersistentEngine(config)
	pe2.Start()
	defer pe2.Stop()

	locks := pe2.ListLocks()
	if len(locks) != 2 {
		t.Errorf("Expected 2 locks after recovery, got %d", len(locks))
	}
}

func TestSnapshot_MaxSnapshots(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSnapshotConfig(dir)
	config.MaxSnapshots = 2

	store, _ := NewSnapshotStore(config)

	// Create 4 snapshots
	for i := 0; i < 4; i++ {
		snapshot := &Snapshot{
			Locks: []*LockSnapshot{},
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
		store.Save(snapshot)
	}

	// Should only have 2 (the most recent ones)
	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots (max), got %d", len(snapshots))
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	config := DefaultWALConfig(dir)
	config.SyncOnWrite = false

	wal, _ := NewWAL(config)
	wal.Open()
	defer wal.Close()

	// Add some entries
	wal.LogAcquire("key1", "owner1", 30*time.Second, time.Now().Add(1*time.Hour))
	wal.LogAcquire("key2", "owner2", 30*time.Second, time.Now().Add(1*time.Hour))

	entries, _ := wal.ReadAll()
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries before truncate, got %d", len(entries))
	}

	// Truncate
	if err := wal.Truncate(); err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	// Should be empty now
	entries, _ = wal.ReadAll()
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries after truncate, got %d", len(entries))
	}

	// Verify we can still write
	wal.LogAcquire("key3", "owner3", 30*time.Second, time.Now().Add(1*time.Hour))
	entries, _ = wal.ReadAll()
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry after new write, got %d", len(entries))
	}
}

func TestWAL_RecoverFromCrash(t *testing.T) {
	dir := t.TempDir()

	// Simulate writing directly to WAL file
	walFile := filepath.Join(dir, "wal.log")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("Failed to create dir: %v", err)
	}

	// Write some entries
	entries := []string{
		`{"seq":1,"ts":"2024-01-01T00:00:00Z","type":"acquire","key":"key1","owner":"owner1","ttl":30000000000}`,
		`{"seq":2,"ts":"2024-01-01T00:00:01Z","type":"renew","key":"key1","owner":"owner1","ttl":60000000000}`,
		`corrupted line that should be skipped`,
		`{"seq":3,"ts":"2024-01-01T00:00:02Z","type":"release","key":"key1","owner":"owner1"}`,
	}

	data := ""
	for _, e := range entries {
		data += e + "\n"
	}

	if err := os.WriteFile(walFile, []byte(data), 0644); err != nil {
		t.Fatalf("Failed to write WAL: %v", err)
	}

	// Recover using reader
	reader, err := NewWALReader(walFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	validEntries := 0
	for {
		entry, err := reader.ReadNext()
		if err != nil {
			break
		}
		if entry != nil {
			validEntries++
		}
	}

	// Should have 3 valid entries (1 was corrupt)
	if validEntries != 3 {
		t.Errorf("Expected 3 valid entries, got %d", validEntries)
	}
}
