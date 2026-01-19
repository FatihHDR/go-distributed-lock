package lock

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

var (
	ErrLeaseExpired   = errors.New("lease has expired")
	ErrLeaseCancelled = errors.New("lease was cancelled")
	ErrContextTimeout = errors.New("operation timed out")
)

// LeaseConfig holds configuration for lease management
type LeaseConfig struct {
	// TTL is the lock's time-to-live
	TTL time.Duration
	// RenewInterval is how often to renew (should be < TTL)
	RenewInterval time.Duration
	// RenewTimeout is the max time to wait for a renew RPC
	RenewTimeout time.Duration
	// MaxRenewFailures is how many consecutive failures before giving up
	MaxRenewFailures int
}

// DefaultLeaseConfig returns sensible defaults
func DefaultLeaseConfig(ttl time.Duration) *LeaseConfig {
	return &LeaseConfig{
		TTL:              ttl,
		RenewInterval:    ttl / 3, // Renew at 1/3 of TTL
		RenewTimeout:     ttl / 4, // Timeout at 1/4 of TTL
		MaxRenewFailures: 3,
	}
}

// Lease represents an active lock lease with auto-renewal
type Lease struct {
	Key    string
	Owner  string
	Config *LeaseConfig
	lock   *Lock
	engine *Engine

	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.RWMutex
	valid         bool
	renewFailures int
	lastRenewed   time.Time

	// Callbacks
	onExpired func()
	onRenewed func(*Lock)
	onLost    func(error)
}

// NewLease creates a new lease for a lock
func NewLease(engine *Engine, key, owner string, config *LeaseConfig) *Lease {
	ctx, cancel := context.WithCancel(context.Background())
	return &Lease{
		Key:         key,
		Owner:       owner,
		Config:      config,
		engine:      engine,
		ctx:         ctx,
		cancel:      cancel,
		valid:       false,
		lastRenewed: time.Now(),
	}
}

// SetOnExpired sets callback for when lease expires
func (l *Lease) SetOnExpired(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onExpired = fn
}

// SetOnRenewed sets callback for successful renewal
func (l *Lease) SetOnRenewed(fn func(*Lock)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onRenewed = fn
}

// SetOnLost sets callback for when lease is lost unexpectedly
func (l *Lease) SetOnLost(fn func(error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onLost = fn
}

// Acquire attempts to acquire the lock with context timeout
func (l *Lease) Acquire(ctx context.Context) error {
	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, l.Config.RenewTimeout)
	defer cancel()

	// Channel for result
	resultCh := make(chan *AcquireResponse, 1)

	go func() {
		resp := l.engine.AcquireLock(&AcquireRequest{
			Key:   l.Key,
			Owner: l.Owner,
			TTL:   l.Config.TTL,
		})
		resultCh <- resp
	}()

	select {
	case <-timeoutCtx.Done():
		return ErrContextTimeout
	case resp := <-resultCh:
		if !resp.Success {
			return errors.New(resp.Error)
		}
		l.mu.Lock()
		l.lock = resp.Lock
		l.valid = true
		l.lastRenewed = time.Now()
		l.mu.Unlock()
		return nil
	}
}

// StartAutoRenewal begins automatic lease renewal in background
func (l *Lease) StartAutoRenewal() {
	go l.renewLoop()
}

// renewLoop handles automatic lease renewal
func (l *Lease) renewLoop() {
	ticker := time.NewTicker(l.Config.RenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if err := l.renew(); err != nil {
				l.mu.Lock()
				l.renewFailures++
				failures := l.renewFailures
				maxFailures := l.Config.MaxRenewFailures
				onLost := l.onLost
				l.mu.Unlock()

				log.Printf("Lease renewal failed for %s: %v (failure %d/%d)",
					l.Key, err, failures, maxFailures)

				if failures >= maxFailures {
					l.mu.Lock()
					l.valid = false
					l.mu.Unlock()

					if onLost != nil {
						onLost(err)
					}
					return
				}
			}
		}
	}
}

// renew attempts to renew the lease
func (l *Lease) renew() error {
	l.mu.RLock()
	if !l.valid {
		l.mu.RUnlock()
		return ErrLeaseExpired
	}
	l.mu.RUnlock()

	// Create timeout context for the renewal
	ctx, cancel := context.WithTimeout(l.ctx, l.Config.RenewTimeout)
	defer cancel()

	resultCh := make(chan *RenewResponse, 1)

	go func() {
		resp := l.engine.RenewLock(&RenewRequest{
			Key:   l.Key,
			Owner: l.Owner,
			TTL:   l.Config.TTL,
		})
		resultCh <- resp
	}()

	select {
	case <-ctx.Done():
		// Timeout - slow client, might be considered dead
		return ErrContextTimeout
	case resp := <-resultCh:
		if !resp.Success {
			// Late renew (lock expired) or not owner anymore
			if resp.Error == ErrLockExpired.Error() {
				l.mu.Lock()
				l.valid = false
				onExpired := l.onExpired
				l.mu.Unlock()

				if onExpired != nil {
					onExpired()
				}
				return ErrLeaseExpired
			}
			return errors.New(resp.Error)
		}

		// Success - reset failure count
		l.mu.Lock()
		l.lock = resp.Lock
		l.renewFailures = 0
		l.lastRenewed = time.Now()
		onRenewed := l.onRenewed
		l.mu.Unlock()

		if onRenewed != nil {
			onRenewed(resp.Lock)
		}
		return nil
	}
}

// Release releases the lease
func (l *Lease) Release() error {
	l.cancel() // Stop renewal loop

	l.mu.Lock()
	l.valid = false
	l.mu.Unlock()

	resp := l.engine.ReleaseLock(&ReleaseRequest{
		Key:   l.Key,
		Owner: l.Owner,
	})

	if !resp.Success {
		return errors.New(resp.Error)
	}
	return nil
}

// IsValid returns whether the lease is still valid
func (l *Lease) IsValid() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.valid
}

// GetLock returns the current lock info
func (l *Lease) GetLock() *Lock {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lock
}

// Remaining returns time until lease expires
func (l *Lease) Remaining() time.Duration {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.lock == nil {
		return 0
	}
	return time.Until(l.lock.ExpiresAt)
}

// Context returns the lease's context (cancelled when lease is released/lost)
func (l *Lease) Context() context.Context {
	return l.ctx
}

// LeaseManager manages multiple leases
type LeaseManager struct {
	engine *Engine
	leases map[string]*Lease
	mu     sync.RWMutex
}

// NewLeaseManager creates a new lease manager
func NewLeaseManager(engine *Engine) *LeaseManager {
	return &LeaseManager{
		engine: engine,
		leases: make(map[string]*Lease),
	}
}

// AcquireLease acquires a new lease with automatic renewal
func (m *LeaseManager) AcquireLease(ctx context.Context, key, owner string, ttl time.Duration) (*Lease, error) {
	config := DefaultLeaseConfig(ttl)
	lease := NewLease(m.engine, key, owner, config)

	if err := lease.Acquire(ctx); err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.leases[key] = lease
	m.mu.Unlock()

	// Start auto-renewal
	lease.StartAutoRenewal()

	// Clean up when lease is lost
	lease.SetOnLost(func(err error) {
		m.mu.Lock()
		delete(m.leases, key)
		m.mu.Unlock()
	})

	return lease, nil
}

// ReleaseLease releases a lease
func (m *LeaseManager) ReleaseLease(key string) error {
	m.mu.Lock()
	lease, exists := m.leases[key]
	if exists {
		delete(m.leases, key)
	}
	m.mu.Unlock()

	if !exists {
		return ErrLockNotFound
	}

	return lease.Release()
}

// GetLease returns a lease by key
func (m *LeaseManager) GetLease(key string) (*Lease, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lease, exists := m.leases[key]
	return lease, exists
}

// ReleaseAll releases all leases
func (m *LeaseManager) ReleaseAll() {
	m.mu.Lock()
	leases := make([]*Lease, 0, len(m.leases))
	for _, lease := range m.leases {
		leases = append(leases, lease)
	}
	m.leases = make(map[string]*Lease)
	m.mu.Unlock()

	for _, lease := range leases {
		lease.Release()
	}
}
