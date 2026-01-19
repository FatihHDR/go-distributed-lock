package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"distributed-lock/internal/lock"
)

// LockStateMachine implements StateMachine for lock operations
type LockStateMachine struct {
	mu sync.RWMutex

	engine  *lock.Engine
	tracker *CommandTracker

	// For deduplication
	lastApplied map[string]*CommandResult
}

// NewLockStateMachine creates a new lock state machine
func NewLockStateMachine(engine *lock.Engine) *LockStateMachine {
	return &LockStateMachine{
		engine:      engine,
		tracker:     NewCommandTracker(5 * time.Minute),
		lastApplied: make(map[string]*CommandResult),
	}
}

// Apply applies a log entry to the state machine
func (lsm *LockStateMachine) Apply(entry LogEntry) interface{} {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		log.Printf("[StateMachine] Failed to decode command: %v", err)
		return &CommandResult{Success: false, Error: err.Error()}
	}

	// Check for duplicate
	if cmd.RequestID != "" {
		if result, exists := lsm.tracker.Get(cmd.RequestID); exists {
			return result
		}
	}

	var result *CommandResult

	switch cmd.Type {
	case CmdNoop:
		result = &CommandResult{Success: true}

	case CmdAcquire:
		resp := lsm.engine.AcquireLock(&lock.AcquireRequest{
			Key:   cmd.Key,
			Owner: cmd.Owner,
			TTL:   cmd.TTL,
		})
		result = &CommandResult{
			Success: resp.Success,
			Data:    resp.Lock,
			Error:   resp.Error,
		}

	case CmdRenew:
		resp := lsm.engine.RenewLock(&lock.RenewRequest{
			Key:   cmd.Key,
			Owner: cmd.Owner,
			TTL:   cmd.TTL,
		})
		result = &CommandResult{
			Success: resp.Success,
			Data:    resp.Lock,
			Error:   resp.Error,
		}

	case CmdRelease:
		resp := lsm.engine.ReleaseLock(&lock.ReleaseRequest{
			Key:   cmd.Key,
			Owner: cmd.Owner,
		})
		result = &CommandResult{
			Success: resp.Success,
			Error:   resp.Error,
		}

	default:
		result = &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("unknown command type: %d", cmd.Type),
		}
	}

	// Track for deduplication
	if cmd.RequestID != "" {
		lsm.tracker.Track(cmd.RequestID, result)
	}

	log.Printf("[StateMachine] Applied %s for key=%s owner=%s success=%v",
		cmd.Type, cmd.Key, cmd.Owner, result.Success)

	return result
}

// Snapshot creates a snapshot of the current state
func (lsm *LockStateMachine) Snapshot() ([]byte, error) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	locks := lsm.engine.ListLocks()

	snapshot := &LockStateSnapshot{
		Locks: make([]LockSnapshotEntry, 0, len(locks)),
	}

	for _, l := range locks {
		snapshot.Locks = append(snapshot.Locks, LockSnapshotEntry{
			Key:       l.Key,
			Owner:     l.Owner,
			TTL:       l.TTL,
			ExpiresAt: l.ExpiresAt,
			CreatedAt: l.CreatedAt,
			Version:   l.Version,
		})
	}

	return json.Marshal(snapshot)
}

// Restore restores state from a snapshot
func (lsm *LockStateMachine) Restore(data []byte) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	var snapshot LockStateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	// Clear existing state and restore
	// Note: This requires access to the internal store
	// For now, we'll acquire locks from the snapshot
	for _, entry := range snapshot.Locks {
		if entry.ExpiresAt.After(time.Now()) {
			lsm.engine.AcquireLock(&lock.AcquireRequest{
				Key:   entry.Key,
				Owner: entry.Owner,
				TTL:   entry.TTL,
			})
		}
	}

	log.Printf("[StateMachine] Restored %d locks from snapshot", len(snapshot.Locks))
	return nil
}

// GetLock retrieves a lock (read-only, doesn't go through Raft)
func (lsm *LockStateMachine) GetLock(key string) (*lock.Lock, error) {
	return lsm.engine.GetLock(key)
}

// ListLocks lists all locks (read-only, doesn't go through Raft)
func (lsm *LockStateMachine) ListLocks() []*lock.Lock {
	return lsm.engine.ListLocks()
}

// GetEngine returns the underlying lock engine
func (lsm *LockStateMachine) GetEngine() *lock.Engine {
	return lsm.engine
}

// LockStateSnapshot represents a snapshot of lock state
type LockStateSnapshot struct {
	Locks []LockSnapshotEntry `json:"locks"`
}

// LockSnapshotEntry represents a lock in a snapshot
type LockSnapshotEntry struct {
	Key       string        `json:"key"`
	Owner     string        `json:"owner"`
	TTL       time.Duration `json:"ttl"`
	ExpiresAt time.Time     `json:"expires_at"`
	CreatedAt time.Time     `json:"created_at"`
	Version   uint64        `json:"version"`
}
