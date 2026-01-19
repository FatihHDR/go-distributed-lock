package lock

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrLockNotFound     = errors.New("lock not found")
	ErrLockHeld         = errors.New("lock already held by another owner")
	ErrNotLockOwner     = errors.New("not the lock owner")
	ErrInvalidTTL       = errors.New("invalid TTL value")
	ErrLockExpired      = errors.New("lock has expired")
	ErrOperationTimeout = errors.New("operation timed out")
)

// Engine manages distributed lock operations
type Engine struct {
	store          *LockStore
	ttlMgr         *TTLManager
	mu             sync.Mutex
	versionCounter uint64
}

// NewEngine creates a new lock engine
func NewEngine() *Engine {
	e := &Engine{
		store:          NewLockStore(),
		versionCounter: 0,
	}
	e.ttlMgr = NewTTLManager(e.store, e.onLockExpired)
	return e
}

// Start begins the TTL manager background process
func (e *Engine) Start() {
	e.ttlMgr.Start()
}

// Stop halts the TTL manager
func (e *Engine) Stop() {
	e.ttlMgr.Stop()
}

// onLockExpired is called when a lock expires
func (e *Engine) onLockExpired(lock *Lock) {
	// Hook for future use (notifications, metrics, etc.)
}

// nextVersion generates the next version number
func (e *Engine) nextVersion() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.versionCounter++
	return e.versionCounter
}

// AcquireLock attempts to acquire a lock for the given key
func (e *Engine) AcquireLock(req *AcquireRequest) *AcquireResponse {
	if req.TTL <= 0 {
		return &AcquireResponse{
			Success: false,
			Error:   ErrInvalidTTL.Error(),
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Check if lock already exists
	existing, exists := e.store.Get(req.Key)
	if exists {
		// If lock exists but expired, we can take it
		if !existing.IsExpired() {
			// Lock is still valid
			if existing.Owner == req.Owner {
				// Same owner, renew instead
				return e.renewLockInternal(existing, req.TTL)
			}
			return &AcquireResponse{
				Success: false,
				Error:   ErrLockHeld.Error(),
			}
		}
		// Lock expired, clean it up
		e.store.Delete(req.Key)
	}

	// Create new lock
	now := time.Now()
	lock := &Lock{
		Key:       req.Key,
		Owner:     req.Owner,
		TTL:       req.TTL,
		ExpiresAt: now.Add(req.TTL),
		CreatedAt: now,
		Version:   e.nextVersion(),
	}

	e.store.Set(lock)

	return &AcquireResponse{
		Success: true,
		Lock:    lock,
	}
}

// RenewLock extends the TTL of an existing lock
func (e *Engine) RenewLock(req *RenewRequest) *RenewResponse {
	e.mu.Lock()
	defer e.mu.Unlock()

	existing, exists := e.store.Get(req.Key)
	if !exists {
		return &RenewResponse{
			Success: false,
			Error:   ErrLockNotFound.Error(),
		}
	}

	if existing.IsExpired() {
		e.store.Delete(req.Key)
		return &RenewResponse{
			Success: false,
			Error:   ErrLockExpired.Error(),
		}
	}

	if existing.Owner != req.Owner {
		return &RenewResponse{
			Success: false,
			Error:   ErrNotLockOwner.Error(),
		}
	}

	// Use provided TTL or original TTL
	ttl := req.TTL
	if ttl <= 0 {
		ttl = existing.TTL
	}

	resp := e.renewLockInternal(existing, ttl)
	return &RenewResponse{
		Success: resp.Success,
		Lock:    resp.Lock,
		Error:   resp.Error,
	}
}

// renewLockInternal renews a lock (must be called with mutex held)
func (e *Engine) renewLockInternal(lock *Lock, ttl time.Duration) *AcquireResponse {
	lock.TTL = ttl
	lock.ExpiresAt = time.Now().Add(ttl)
	lock.Version = e.nextVersion()
	e.store.Set(lock)

	return &AcquireResponse{
		Success: true,
		Lock:    lock,
	}
}

// ReleaseLock releases a lock held by the owner
func (e *Engine) ReleaseLock(req *ReleaseRequest) *ReleaseResponse {
	e.mu.Lock()
	defer e.mu.Unlock()

	existing, exists := e.store.Get(req.Key)
	if !exists {
		return &ReleaseResponse{
			Success: false,
			Error:   ErrLockNotFound.Error(),
		}
	}

	if existing.Owner != req.Owner {
		return &ReleaseResponse{
			Success: false,
			Error:   ErrNotLockOwner.Error(),
		}
	}

	e.store.Delete(req.Key)

	return &ReleaseResponse{
		Success: true,
	}
}

// GetLock retrieves lock information (read-only)
func (e *Engine) GetLock(key string) (*Lock, error) {
	lock, exists := e.store.Get(key)
	if !exists {
		return nil, ErrLockNotFound
	}
	if lock.IsExpired() {
		return nil, ErrLockExpired
	}
	return lock, nil
}

// ListLocks returns all active (non-expired) locks
func (e *Engine) ListLocks() []*Lock {
	all := e.store.All()
	active := make([]*Lock, 0)
	for _, lock := range all {
		if !lock.IsExpired() {
			active = append(active, lock)
		}
	}
	return active
}
