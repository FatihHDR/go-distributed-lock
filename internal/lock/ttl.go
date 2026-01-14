package lock

import (
	"sync"
	"time"
)

const (
	// DefaultCleanupInterval is the default interval for TTL cleanup
	DefaultCleanupInterval = 1 * time.Second
)

// ExpiredCallback is called when a lock expires
type ExpiredCallback func(lock *Lock)

// TTLManager handles automatic expiration of locks
type TTLManager struct {
	store     *LockStore
	interval  time.Duration
	onExpired ExpiredCallback
	stopCh    chan struct{}
	wg        sync.WaitGroup
	running   bool
	mu        sync.Mutex
}

// NewTTLManager creates a new TTL manager
func NewTTLManager(store *LockStore, onExpired ExpiredCallback) *TTLManager {
	return &TTLManager{
		store:     store,
		interval:  DefaultCleanupInterval,
		onExpired: onExpired,
		// stopCh is created in Start(), not here
	}
}

// SetInterval sets the cleanup interval
func (m *TTLManager) SetInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.interval = interval
}

// Start begins the TTL cleanup goroutine
func (m *TTLManager) Start() {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.stopCh = make(chan struct{})
	interval := m.interval
	m.mu.Unlock()

	m.wg.Add(1)
	go m.cleanupLoop(interval)
}

// Stop halts the TTL cleanup goroutine
func (m *TTLManager) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.running = false
	close(m.stopCh)
	m.mu.Unlock()

	m.wg.Wait()
}

// IsRunning returns whether the manager is running
func (m *TTLManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// cleanupLoop periodically removes expired locks
func (m *TTLManager) cleanupLoop(interval time.Duration) {
	defer m.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanup()
		case <-m.stopCh:
			return
		}
	}
}

// cleanup removes all expired locks
func (m *TTLManager) cleanup() {
	locks := m.store.All()
	for _, lock := range locks {
		if lock.IsExpired() {
			m.store.Delete(lock.Key)
			if m.onExpired != nil {
				m.onExpired(lock)
			}
		}
	}
}

// CleanupNow forces an immediate cleanup (useful for testing)
func (m *TTLManager) CleanupNow() {
	m.cleanup()
}
