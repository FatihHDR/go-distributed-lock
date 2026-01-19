package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

var (
	ErrPoolClosed     = errors.New("connection pool is closed")
	ErrNoConnection   = errors.New("no connection available")
	ErrConnectionDead = errors.New("connection is dead")
)

// PoolConfig configures the connection pool
type PoolConfig struct {
	Address             string
	MaxConnections      int
	MinConnections      int
	ConnectTimeout      time.Duration
	IdleTimeout         time.Duration
	HealthCheckInterval time.Duration
	MaxRetries          int
}

// DefaultPoolConfig returns sensible defaults
func DefaultPoolConfig(address string) *PoolConfig {
	return &PoolConfig{
		Address:             address,
		MaxConnections:      10,
		MinConnections:      2,
		ConnectTimeout:      5 * time.Second,
		IdleTimeout:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
		MaxRetries:          3,
	}
}

// PooledConnection wraps a connection with metadata
type PooledConnection struct {
	conn       net.Conn
	encoder    *json.Encoder
	decoder    *json.Decoder
	createdAt  time.Time
	lastUsedAt time.Time
	inUse      bool
	healthy    bool
	mu         sync.Mutex
}

// ConnectionPool manages a pool of long-lived connections
type ConnectionPool struct {
	config      *PoolConfig
	connections []*PooledConnection
	mu          sync.RWMutex
	closed      bool
	stopCh      chan struct{}
	wg          sync.WaitGroup
	reqID       uint64
	reqMu       sync.Mutex
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(config *PoolConfig) *ConnectionPool {
	pool := &ConnectionPool{
		config:      config,
		connections: make([]*PooledConnection, 0, config.MaxConnections),
		stopCh:      make(chan struct{}),
	}
	return pool
}

// Start initializes the pool and starts health checking
func (p *ConnectionPool) Start() error {
	// Create minimum connections
	for i := 0; i < p.config.MinConnections; i++ {
		if err := p.addConnection(); err != nil {
			log.Printf("Warning: could not create initial connection: %v", err)
		}
	}

	// Start health check goroutine
	p.wg.Add(1)
	go p.healthCheckLoop()

	return nil
}

// Stop closes all connections and stops health checking
func (p *ConnectionPool) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.stopCh)

	for _, pc := range p.connections {
		if pc.conn != nil {
			pc.conn.Close()
		}
	}
	p.connections = nil
	p.mu.Unlock()

	p.wg.Wait()
}

// addConnection creates a new connection and adds it to the pool
func (p *ConnectionPool) addConnection() error {
	conn, err := net.DialTimeout("tcp", p.config.Address, p.config.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	pc := &PooledConnection{
		conn:       conn,
		encoder:    json.NewEncoder(conn),
		decoder:    json.NewDecoder(conn),
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		healthy:    true,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		conn.Close()
		return ErrPoolClosed
	}

	if len(p.connections) >= p.config.MaxConnections {
		conn.Close()
		return nil // Pool is full, that's ok
	}

	p.connections = append(p.connections, pc)
	return nil
}

// getConnection gets an available connection from the pool
func (p *ConnectionPool) getConnection() (*PooledConnection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	// Find an available healthy connection
	for _, pc := range p.connections {
		pc.mu.Lock()
		if !pc.inUse && pc.healthy {
			pc.inUse = true
			pc.lastUsedAt = time.Now()
			pc.mu.Unlock()
			return pc, nil
		}
		pc.mu.Unlock()
	}

	// No connection available, try to create one
	if len(p.connections) < p.config.MaxConnections {
		p.mu.Unlock()
		err := p.addConnection()
		p.mu.Lock()
		if err != nil {
			return nil, err
		}
		// Try again to find an available connection
		for _, pc := range p.connections {
			pc.mu.Lock()
			if !pc.inUse && pc.healthy {
				pc.inUse = true
				pc.lastUsedAt = time.Now()
				pc.mu.Unlock()
				return pc, nil
			}
			pc.mu.Unlock()
		}
	}

	return nil, ErrNoConnection
}

// releaseConnection returns a connection to the pool
func (p *ConnectionPool) releaseConnection(pc *PooledConnection) {
	pc.mu.Lock()
	pc.inUse = false
	pc.lastUsedAt = time.Now()
	pc.mu.Unlock()
}

// markUnhealthy marks a connection as unhealthy
func (p *ConnectionPool) markUnhealthy(pc *PooledConnection) {
	pc.mu.Lock()
	pc.healthy = false
	pc.inUse = false
	pc.mu.Unlock()
}

// healthCheckLoop periodically checks connection health
func (p *ConnectionPool) healthCheckLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.performHealthCheck()
		case <-p.stopCh:
			return
		}
	}
}

// performHealthCheck checks all connections and removes unhealthy ones
func (p *ConnectionPool) performHealthCheck() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	healthy := make([]*PooledConnection, 0)

	for _, pc := range p.connections {
		pc.mu.Lock()
		isHealthy := pc.healthy
		inUse := pc.inUse
		idleTime := time.Since(pc.lastUsedAt)
		pc.mu.Unlock()

		// Remove connections that are:
		// - Unhealthy
		// - Idle for too long (and above min connections)
		if !isHealthy || (!inUse && idleTime > p.config.IdleTimeout && len(healthy) >= p.config.MinConnections) {
			if pc.conn != nil {
				pc.conn.Close()
			}
			continue
		}

		healthy = append(healthy, pc)
	}

	p.connections = healthy

	// Ensure minimum connections
	for len(p.connections) < p.config.MinConnections {
		p.mu.Unlock()
		if err := p.addConnection(); err != nil {
			p.mu.Lock()
			break
		}
		p.mu.Lock()
	}
}

// nextRequestID generates a unique request ID
func (p *ConnectionPool) nextRequestID() string {
	p.reqMu.Lock()
	defer p.reqMu.Unlock()
	p.reqID++
	return fmt.Sprintf("req-%d-%d", time.Now().UnixNano(), p.reqID)
}

// Call sends a request using a pooled connection
func (p *ConnectionPool) Call(msgType MessageType, payload interface{}) (*Response, error) {
	var lastErr error

	for attempt := 0; attempt < p.config.MaxRetries; attempt++ {
		pc, err := p.getConnection()
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := p.callWithConnection(pc, msgType, payload)
		if err != nil {
			p.markUnhealthy(pc)
			lastErr = err
			continue
		}

		p.releaseConnection(pc)
		return resp, nil
	}

	return nil, fmt.Errorf("all retries failed: %w", lastErr)
}

// callWithConnection performs the RPC call using a specific connection
func (p *ConnectionPool) callWithConnection(pc *PooledConnection, msgType MessageType, payload interface{}) (*Response, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	req := &Request{
		Type:      msgType,
		RequestID: p.nextRequestID(),
		Payload:   payloadBytes,
	}

	// Set deadline for this call
	pc.conn.SetDeadline(time.Now().Add(p.config.ConnectTimeout))
	defer pc.conn.SetDeadline(time.Time{}) // Clear deadline

	if err := pc.encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	var resp Response
	if err := pc.decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to receive response: %w", err)
	}

	return &resp, nil
}

// Stats returns pool statistics
func (p *ConnectionPool) Stats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	total := len(p.connections)
	inUse := 0
	healthy := 0

	for _, pc := range p.connections {
		pc.mu.Lock()
		if pc.inUse {
			inUse++
		}
		if pc.healthy {
			healthy++
		}
		pc.mu.Unlock()
	}

	return map[string]interface{}{
		"total":   total,
		"in_use":  inUse,
		"healthy": healthy,
		"idle":    total - inUse,
		"closed":  p.closed,
	}
}

// PersistentClient wraps ConnectionPool with convenience methods
type PersistentClient struct {
	pool   *ConnectionPool
	nodeID string
}

// NewPersistentClient creates a client with persistent connections
func NewPersistentClient(address, nodeID string) *PersistentClient {
	config := DefaultPoolConfig(address)
	return &PersistentClient{
		pool:   NewConnectionPool(config),
		nodeID: nodeID,
	}
}

// Start starts the persistent client
func (c *PersistentClient) Start() error {
	return c.pool.Start()
}

// Stop stops the persistent client
func (c *PersistentClient) Stop() {
	c.pool.Stop()
}

// Ping sends a ping to verify connection
func (c *PersistentClient) Ping() (bool, error) {
	resp, err := c.pool.Call(MsgPing, nil)
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

// GetConnectionStats returns connection pool statistics
func (c *PersistentClient) GetConnectionStats() map[string]interface{} {
	return c.pool.Stats()
}
