package lock

import (
	"sync"
	"time"
)

// Lock represents a distributed lock entry
type Lock struct {
	Key       string    `json:"key"`
	Owner     string    `json:"owner"`      // Node ID that owns the lock
	TTL       time.Duration `json:"ttl"`    // Time-to-live
	ExpiresAt time.Time `json:"expires_at"` // Absolute expiration time
	CreatedAt time.Time `json:"created_at"`
	Version   uint64    `json:"version"`    // For optimistic locking
}

// IsExpired checks if the lock has expired
func (l *Lock) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

// Remaining returns the remaining time before expiration
func (l *Lock) Remaining() time.Duration {
	return time.Until(l.ExpiresAt)
}

// LockStore is an in-memory store for locks
type LockStore struct {
	mu    sync.RWMutex
	locks map[string]*Lock
}

// NewLockStore creates a new in-memory lock store
func NewLockStore() *LockStore {
	return &LockStore{
		locks: make(map[string]*Lock),
	}
}

// Get retrieves a lock by key
func (s *LockStore) Get(key string) (*Lock, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lock, exists := s.locks[key]
	return lock, exists
}

// Set stores a lock
func (s *LockStore) Set(lock *Lock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.locks[lock.Key] = lock
}

// Delete removes a lock by key
func (s *LockStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.locks, key)
}

// All returns all locks (for iteration/cleanup)
func (s *LockStore) All() []*Lock {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Lock, 0, len(s.locks))
	for _, lock := range s.locks {
		result = append(result, lock)
	}
	return result
}

// AcquireRequest represents a lock acquisition request
type AcquireRequest struct {
	Key   string        `json:"key"`
	Owner string        `json:"owner"`
	TTL   time.Duration `json:"ttl"`
}

// AcquireResponse represents a lock acquisition response
type AcquireResponse struct {
	Success bool   `json:"success"`
	Lock    *Lock  `json:"lock,omitempty"`
	Error   string `json:"error,omitempty"`
}

// RenewRequest represents a lock renewal request
type RenewRequest struct {
	Key   string        `json:"key"`
	Owner string        `json:"owner"`
	TTL   time.Duration `json:"ttl"` // New TTL (optional, uses original if zero)
}

// RenewResponse represents a lock renewal response
type RenewResponse struct {
	Success bool   `json:"success"`
	Lock    *Lock  `json:"lock,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ReleaseRequest represents a lock release request
type ReleaseRequest struct {
	Key   string `json:"key"`
	Owner string `json:"owner"`
}

// ReleaseResponse represents a lock release response
type ReleaseResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}
