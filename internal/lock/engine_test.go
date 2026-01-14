package lock

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAcquireLock_Success(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	req := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	}

	resp := engine.AcquireLock(req)

	if !resp.Success {
		t.Fatalf("Expected success, got error: %s", resp.Error)
	}
	if resp.Lock.Key != "test-key" {
		t.Errorf("Expected key 'test-key', got '%s'", resp.Lock.Key)
	}
	if resp.Lock.Owner != "owner-1" {
		t.Errorf("Expected owner 'owner-1', got '%s'", resp.Lock.Owner)
	}
}

func TestAcquireLock_AlreadyHeld(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	// First acquire
	req1 := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	}
	engine.AcquireLock(req1)

	// Second acquire by different owner - should fail
	req2 := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-2",
		TTL:   5 * time.Second,
	}
	resp := engine.AcquireLock(req2)

	if resp.Success {
		t.Fatal("Expected failure, but got success")
	}
	if resp.Error != ErrLockHeld.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrLockHeld.Error(), resp.Error)
	}
}

func TestAcquireLock_StealExpired(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	// First acquire with very short TTL
	req1 := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   1 * time.Millisecond,
	}
	engine.AcquireLock(req1)

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Second owner should be able to steal
	req2 := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-2",
		TTL:   5 * time.Second,
	}
	resp := engine.AcquireLock(req2)

	if !resp.Success {
		t.Fatalf("Expected success (steal expired), got error: %s", resp.Error)
	}
	if resp.Lock.Owner != "owner-2" {
		t.Errorf("Expected owner 'owner-2', got '%s'", resp.Lock.Owner)
	}
}

func TestAcquireLock_SameOwnerRenews(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	req := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	}

	resp1 := engine.AcquireLock(req)
	if !resp1.Success {
		t.Fatalf("First acquire failed: %s", resp1.Error)
	}

	// Same owner acquires again - should renew
	resp2 := engine.AcquireLock(req)
	if !resp2.Success {
		t.Fatalf("Second acquire (renew) failed: %s", resp2.Error)
	}
	if resp2.Lock.Version <= resp1.Lock.Version {
		t.Error("Expected version to increase on renew")
	}
}

func TestAcquireLock_InvalidTTL(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	req := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   0, // Invalid
	}

	resp := engine.AcquireLock(req)

	if resp.Success {
		t.Fatal("Expected failure for invalid TTL")
	}
	if resp.Error != ErrInvalidTTL.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrInvalidTTL.Error(), resp.Error)
	}
}

func TestReleaseLock_Success(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	// Acquire
	acqReq := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	}
	engine.AcquireLock(acqReq)

	// Release
	relReq := &ReleaseRequest{
		Key:   "test-key",
		Owner: "owner-1",
	}
	resp := engine.ReleaseLock(relReq)

	if !resp.Success {
		t.Fatalf("Expected success, got error: %s", resp.Error)
	}

	// Verify lock is gone
	_, err := engine.GetLock("test-key")
	if err != ErrLockNotFound {
		t.Errorf("Expected lock to be deleted, got: %v", err)
	}
}

func TestReleaseLock_NotOwner(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	// Acquire by owner-1
	acqReq := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	}
	engine.AcquireLock(acqReq)

	// Try to release by owner-2 - should fail
	relReq := &ReleaseRequest{
		Key:   "test-key",
		Owner: "owner-2",
	}
	resp := engine.ReleaseLock(relReq)

	if resp.Success {
		t.Fatal("Expected failure, but got success")
	}
	if resp.Error != ErrNotLockOwner.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrNotLockOwner.Error(), resp.Error)
	}
}

func TestReleaseLock_NotFound(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	relReq := &ReleaseRequest{
		Key:   "non-existent",
		Owner: "owner-1",
	}
	resp := engine.ReleaseLock(relReq)

	if resp.Success {
		t.Fatal("Expected failure for non-existent lock")
	}
	if resp.Error != ErrLockNotFound.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrLockNotFound.Error(), resp.Error)
	}
}

func TestRenewLock_Success(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	// Acquire
	acqReq := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   1 * time.Second,
	}
	acqResp := engine.AcquireLock(acqReq)
	originalExpiry := acqResp.Lock.ExpiresAt

	time.Sleep(10 * time.Millisecond)

	// Renew with new TTL
	renewReq := &RenewRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   10 * time.Second,
	}
	resp := engine.RenewLock(renewReq)

	if !resp.Success {
		t.Fatalf("Expected success, got error: %s", resp.Error)
	}
	if !resp.Lock.ExpiresAt.After(originalExpiry) {
		t.Error("Expected expiry to be extended")
	}
}

func TestRenewLock_NotOwner(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	// Acquire by owner-1
	acqReq := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	}
	engine.AcquireLock(acqReq)

	// Try to renew by owner-2 - should fail
	renewReq := &RenewRequest{
		Key:   "test-key",
		Owner: "owner-2",
		TTL:   5 * time.Second,
	}
	resp := engine.RenewLock(renewReq)

	if resp.Success {
		t.Fatal("Expected failure, but got success")
	}
	if resp.Error != ErrNotLockOwner.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrNotLockOwner.Error(), resp.Error)
	}
}

func TestTTLManager_AutoCleanup(t *testing.T) {
	engine := NewEngine()
	engine.ttlMgr.SetInterval(10 * time.Millisecond)
	engine.Start()
	defer engine.Stop()

	// Acquire with short TTL
	req := &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   20 * time.Millisecond,
	}
	engine.AcquireLock(req)

	// Verify lock exists
	_, err := engine.GetLock("test-key")
	if err != nil {
		t.Fatalf("Lock should exist: %v", err)
	}

	// Wait for expiration and cleanup
	time.Sleep(100 * time.Millisecond)

	// Lock should be cleaned up - either expired or not found
	_, err = engine.GetLock("test-key")
	if err == nil {
		t.Error("Expected lock to be expired or cleaned up")
	}
}

func TestConcurrentAcquire(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	const numGoroutines = 50
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := &AcquireRequest{
				Key:   "contested-key",
				Owner: fmt.Sprintf("owner-%d", id),
				TTL:   5 * time.Second,
			}
			resp := engine.AcquireLock(req)
			if resp.Success {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Only one should succeed
	if successCount != 1 {
		t.Errorf("Expected exactly 1 success, got %d", successCount)
	}
}

func TestNoDeadlock_ClientDies(t *testing.T) {
	engine := NewEngine()
	engine.ttlMgr.SetInterval(10 * time.Millisecond)
	engine.Start()
	defer engine.Stop()

	// Simulate client acquiring lock then "dying" (not releasing)
	req := &AcquireRequest{
		Key:   "resource-1",
		Owner: "dead-client",
		TTL:   30 * time.Millisecond,
	}
	engine.AcquireLock(req)

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// New client should be able to acquire
	req2 := &AcquireRequest{
		Key:   "resource-1",
		Owner: "new-client",
		TTL:   5 * time.Second,
	}
	resp := engine.AcquireLock(req2)

	if !resp.Success {
		t.Fatalf("New client should acquire after dead client's lock expires: %s", resp.Error)
	}
	if resp.Lock.Owner != "new-client" {
		t.Errorf("Expected new-client as owner, got %s", resp.Lock.Owner)
	}
}

func TestListLocks(t *testing.T) {
	engine := NewEngine()
	defer engine.Stop()

	// Acquire multiple locks
	for i := 0; i < 5; i++ {
		req := &AcquireRequest{
			Key:   fmt.Sprintf("key-%d", i),
			Owner: "owner-1",
			TTL:   5 * time.Second,
		}
		engine.AcquireLock(req)
	}

	locks := engine.ListLocks()
	if len(locks) != 5 {
		t.Errorf("Expected 5 locks, got %d", len(locks))
	}
}
