package lock

import (
	"context"
	"time"
)

// AcquireLockWithContext attempts to acquire a lock with context timeout
// If the context is cancelled or times out, returns ErrOperationTimeout
func (e *Engine) AcquireLockWithContext(ctx context.Context, req *AcquireRequest) *AcquireResponse {
	resultCh := make(chan *AcquireResponse, 1)

	go func() {
		resultCh <- e.AcquireLock(req)
	}()

	select {
	case <-ctx.Done():
		return &AcquireResponse{
			Success: false,
			Error:   ErrOperationTimeout.Error(),
		}
	case resp := <-resultCh:
		return resp
	}
}

// RenewLockWithContext extends TTL with context timeout
// Late renew (after expiry) is rejected - client is considered dead
func (e *Engine) RenewLockWithContext(ctx context.Context, req *RenewRequest) *RenewResponse {
	resultCh := make(chan *RenewResponse, 1)

	go func() {
		resultCh <- e.RenewLock(req)
	}()

	select {
	case <-ctx.Done():
		// Slow client - treat as potentially dead
		return &RenewResponse{
			Success: false,
			Error:   ErrOperationTimeout.Error(),
		}
	case resp := <-resultCh:
		return resp
	}
}

// ReleaseLockWithContext releases a lock with context timeout
func (e *Engine) ReleaseLockWithContext(ctx context.Context, req *ReleaseRequest) *ReleaseResponse {
	resultCh := make(chan *ReleaseResponse, 1)

	go func() {
		resultCh <- e.ReleaseLock(req)
	}()

	select {
	case <-ctx.Done():
		return &ReleaseResponse{
			Success: false,
			Error:   ErrOperationTimeout.Error(),
		}
	case resp := <-resultCh:
		return resp
	}
}

// TryAcquireLock attempts to acquire without blocking, returns immediately
func (e *Engine) TryAcquireLock(req *AcquireRequest) *AcquireResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	return e.AcquireLockWithContext(ctx, req)
}
