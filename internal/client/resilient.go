package client

import (
	"context"
	"errors"
	"time"

	"distributed-lock/internal/lock"
	"distributed-lock/internal/logging"
	"distributed-lock/internal/metrics"
	"distributed-lock/internal/retry"
	"distributed-lock/internal/rpc"
)

var (
	ErrClientNotConnected = errors.New("client not connected")
	ErrLockAcquireFailed  = errors.New("failed to acquire lock after retries")
)

// ResilientClientConfig configures the resilient client
type ResilientClientConfig struct {
	// Server address
	Address string

	// Retry configuration
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	// Timeouts
	ConnectTimeout time.Duration
	RequestTimeout time.Duration

	// Client identity
	ClientID string
}

// DefaultResilientClientConfig returns sensible defaults
func DefaultResilientClientConfig(address, clientID string) *ResilientClientConfig {
	return &ResilientClientConfig{
		Address:        address,
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		ConnectTimeout: 5 * time.Second,
		RequestTimeout: 10 * time.Second,
		ClientID:       clientID,
	}
}

// ResilientClient wraps RPC client with retry, metrics, and logging
type ResilientClient struct {
	config         *ResilientClientConfig
	rpcClient      *rpc.Client
	retryer        *retry.Retryer
	circuitBreaker *retry.CircuitBreaker
	metrics        *metrics.LockMetrics
	logger         *logging.Logger
	connected      bool
}

// NewResilientClient creates a new resilient client
func NewResilientClient(config *ResilientClientConfig) *ResilientClient {
	retryConfig := &retry.RetryConfig{
		MaxAttempts: config.MaxRetries,
		Backoff:     retry.NewExponentialBackoff(config.InitialBackoff, config.MaxBackoff),
		RetryableErrors: func(err error) bool {
			// Retry on connection errors, not on lock held errors
			if err == nil {
				return false
			}
			errStr := err.Error()
			// Don't retry on lock ownership errors
			if errStr == lock.ErrLockHeld.Error() ||
				errStr == lock.ErrNotLockOwner.Error() {
				return false
			}
			return true
		},
	}

	return &ResilientClient{
		config:         config,
		rpcClient:      rpc.NewClient(config.Address),
		retryer:        retry.NewRetryer(retryConfig),
		circuitBreaker: retry.NewCircuitBreaker(5, 30*time.Second),
		metrics:        metrics.NewLockMetrics(),
		logger:         logging.NewLogger(config.ClientID).WithComponent("client"),
	}
}

// Connect establishes connection to the server
func (c *ResilientClient) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.ConnectTimeout)
	defer cancel()

	result := c.retryer.Do(ctx, func() error {
		return c.rpcClient.Connect()
	})

	if !result.Success {
		c.logger.Error("Failed to connect", logging.Fields{
			"address":  c.config.Address,
			"attempts": result.Attempts,
			"error":    result.LastErr.Error(),
		})
		return result.LastErr
	}

	c.connected = true
	c.logger.Info("Connected to server", logging.Fields{
		"address":  c.config.Address,
		"attempts": result.Attempts,
	})
	return nil
}

// Close closes the connection
func (c *ResilientClient) Close() error {
	c.connected = false
	return c.rpcClient.Close()
}

// IsConnected returns connection status
func (c *ResilientClient) IsConnected() bool {
	return c.connected
}

// AcquireLock acquires a lock with retry and metrics
func (c *ResilientClient) AcquireLock(ctx context.Context, key string, ttl time.Duration) (*lock.AcquireResponse, error) {
	start := time.Now()

	if !c.connected {
		c.metrics.RecordAcquire(false, time.Since(start))
		return nil, ErrClientNotConnected
	}

	var resp *lock.AcquireResponse
	result := c.retryer.Do(ctx, func() error {
		return c.circuitBreaker.Execute(func() error {
			var err error
			resp, err = c.rpcClient.AcquireLock(key, c.config.ClientID, ttl)
			if err != nil {
				return err
			}
			if !resp.Success && resp.Error != lock.ErrLockHeld.Error() {
				return errors.New(resp.Error)
			}
			return nil
		})
	})

	duration := time.Since(start)
	success := result.Success && resp != nil && resp.Success

	c.metrics.RecordAcquire(success, duration)
	c.logger.LogLockAcquire(key, c.config.ClientID, success, result.LastErr, duration)

	if result.Attempts > 1 {
		c.logger.LogRetry("acquire", result.Attempts, c.config.MaxRetries, result.LastErr, duration)
	}

	if !result.Success {
		return resp, result.LastErr
	}

	return resp, nil
}

// RenewLock renews a lock with retry and metrics
func (c *ResilientClient) RenewLock(ctx context.Context, key string, ttl time.Duration) (*lock.RenewResponse, error) {
	start := time.Now()

	if !c.connected {
		c.metrics.RecordRenew(false, time.Since(start))
		return nil, ErrClientNotConnected
	}

	var resp *lock.RenewResponse
	result := c.retryer.Do(ctx, func() error {
		return c.circuitBreaker.Execute(func() error {
			var err error
			resp, err = c.rpcClient.RenewLock(key, c.config.ClientID, ttl)
			if err != nil {
				return err
			}
			if !resp.Success {
				return errors.New(resp.Error)
			}
			return nil
		})
	})

	duration := time.Since(start)
	success := result.Success && resp != nil && resp.Success

	c.metrics.RecordRenew(success, duration)
	c.logger.LogLockRenew(key, c.config.ClientID, success, result.LastErr, duration)

	if !result.Success {
		return resp, result.LastErr
	}

	return resp, nil
}

// ReleaseLock releases a lock with retry and metrics
func (c *ResilientClient) ReleaseLock(ctx context.Context, key string) (*lock.ReleaseResponse, error) {
	start := time.Now()

	if !c.connected {
		c.metrics.RecordRelease(false, time.Since(start))
		return nil, ErrClientNotConnected
	}

	var resp *lock.ReleaseResponse
	result := c.retryer.Do(ctx, func() error {
		return c.circuitBreaker.Execute(func() error {
			var err error
			resp, err = c.rpcClient.ReleaseLock(key, c.config.ClientID)
			if err != nil {
				return err
			}
			if !resp.Success {
				return errors.New(resp.Error)
			}
			return nil
		})
	})

	duration := time.Since(start)
	success := result.Success && resp != nil && resp.Success

	c.metrics.RecordRelease(success, duration)
	c.logger.LogLockRelease(key, c.config.ClientID, success, result.LastErr)

	if !result.Success {
		return resp, result.LastErr
	}

	return resp, nil
}

// GetLeader gets the current leader
func (c *ResilientClient) GetLeader(ctx context.Context) (*rpc.GetLeaderResponse, error) {
	if !c.connected {
		return nil, ErrClientNotConnected
	}

	var resp *rpc.GetLeaderResponse
	result := c.retryer.Do(ctx, func() error {
		var err error
		resp, err = c.rpcClient.GetLeader()
		return err
	})

	if !result.Success {
		return nil, result.LastErr
	}

	return resp, nil
}

// GetMetrics returns the client's metrics
func (c *ResilientClient) GetMetrics() map[string]interface{} {
	return c.metrics.Snapshot()
}

// GetCircuitBreakerState returns the circuit breaker state
func (c *ResilientClient) GetCircuitBreakerState() string {
	return c.circuitBreaker.GetState()
}

// IdempotentKey generates an idempotent key for a request
type IdempotentKey struct {
	ClientID  string
	RequestID string
	Key       string
	Operation string
	Timestamp time.Time
}

// NewIdempotentKey creates a new idempotent key
func NewIdempotentKey(clientID, key, operation string) *IdempotentKey {
	return &IdempotentKey{
		ClientID:  clientID,
		RequestID: generateRequestID(),
		Key:       key,
		Operation: operation,
		Timestamp: time.Now(),
	}
}

func generateRequestID() string {
	return time.Now().Format("20060102150405.000000")
}
