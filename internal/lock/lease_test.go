package lock

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestLease_AcquireAndAutoRenew(t *testing.T) {
	engine := NewEngine()
	engine.Start()
	defer engine.Stop()

	config := &LeaseConfig{
		TTL:              100 * time.Millisecond,
		RenewInterval:    30 * time.Millisecond,
		RenewTimeout:     20 * time.Millisecond,
		MaxRenewFailures: 3,
	}

	lease := NewLease(engine, "test-key", "owner-1", config)

	err := lease.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Failed to acquire lease: %v", err)
	}

	if !lease.IsValid() {
		t.Fatal("Lease should be valid after acquisition")
	}

	// Start auto-renewal
	lease.StartAutoRenewal()

	// Wait for a few renewal cycles
	time.Sleep(150 * time.Millisecond)

	// Lease should still be valid due to auto-renewal
	if !lease.IsValid() {
		t.Fatal("Lease should still be valid after auto-renewal")
	}

	// Release
	err = lease.Release()
	if err != nil {
		t.Fatalf("Failed to release lease: %v", err)
	}
}

func TestLease_RenewOnlyByOwner(t *testing.T) {
	engine := NewEngine()
	engine.Start()
	defer engine.Stop()

	// Owner 1 acquires
	resp := engine.AcquireLock(&AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	})
	if !resp.Success {
		t.Fatalf("Failed to acquire lock: %s", resp.Error)
	}

	// Owner 2 tries to renew - should fail
	renewResp := engine.RenewLock(&RenewRequest{
		Key:   "test-key",
		Owner: "owner-2",
		TTL:   5 * time.Second,
	})

	if renewResp.Success {
		t.Fatal("Renew by non-owner should fail")
	}
	if renewResp.Error != ErrNotLockOwner.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrNotLockOwner.Error(), renewResp.Error)
	}
}

func TestLease_LateRenewRejected(t *testing.T) {
	engine := NewEngine()
	// Don't start TTL manager to control expiration manually

	// Acquire with short TTL
	resp := engine.AcquireLock(&AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   10 * time.Millisecond,
	})
	if !resp.Success {
		t.Fatalf("Failed to acquire lock: %s", resp.Error)
	}

	// Wait for lock to expire
	time.Sleep(20 * time.Millisecond)

	// Late renew should be rejected
	renewResp := engine.RenewLock(&RenewRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	})

	if renewResp.Success {
		t.Fatal("Late renew should be rejected")
	}
	if renewResp.Error != ErrLockExpired.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrLockExpired.Error(), renewResp.Error)
	}
}

func TestLease_ContextTimeout(t *testing.T) {
	engine := NewEngine()
	engine.Start()
	defer engine.Stop()

	// Create a very short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Small delay to ensure context expires
	time.Sleep(1 * time.Millisecond)

	resp := engine.AcquireLockWithContext(ctx, &AcquireRequest{
		Key:   "test-key",
		Owner: "owner-1",
		TTL:   5 * time.Second,
	})

	if resp.Success {
		t.Fatal("Should fail with context timeout")
	}
	if resp.Error != ErrOperationTimeout.Error() {
		t.Errorf("Expected error '%s', got '%s'", ErrOperationTimeout.Error(), resp.Error)
	}
}

func TestLease_CrashedClientDoesNotLeak(t *testing.T) {
	engine := NewEngine()
	engine.ttlMgr.SetInterval(10 * time.Millisecond)
	engine.Start()
	defer engine.Stop()

	config := &LeaseConfig{
		TTL:              50 * time.Millisecond,
		RenewInterval:    15 * time.Millisecond,
		RenewTimeout:     10 * time.Millisecond,
		MaxRenewFailures: 2,
	}

	// Simulate a client that acquires but then "crashes" (stops renewing)
	lease := NewLease(engine, "crashed-resource", "crashed-client", config)

	err := lease.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Failed to acquire lease: %v", err)
	}

	// Don't start auto-renewal - simulating crash
	// lease.StartAutoRenewal() <- intentionally not called

	// Wait for lock to expire
	time.Sleep(100 * time.Millisecond)

	// New client should be able to acquire
	newConfig := DefaultLeaseConfig(5 * time.Second)
	newLease := NewLease(engine, "crashed-resource", "new-client", newConfig)

	err = newLease.Acquire(context.Background())
	if err != nil {
		t.Fatalf("New client should acquire after crash: %v", err)
	}

	if !newLease.IsValid() {
		t.Fatal("New lease should be valid")
	}

	newLease.Release()
}

func TestLease_OnLostCallback(t *testing.T) {
	engine := NewEngine()
	engine.Start()
	defer engine.Stop()

	config := &LeaseConfig{
		TTL:              30 * time.Millisecond,
		RenewInterval:    10 * time.Millisecond,
		RenewTimeout:     5 * time.Millisecond,
		MaxRenewFailures: 2,
	}

	lease := NewLease(engine, "callback-test", "owner-1", config)

	var lostCalled int32
	lease.SetOnLost(func(err error) {
		atomic.StoreInt32(&lostCalled, 1)
	})

	err := lease.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Failed to acquire lease: %v", err)
	}

	// Start renewal but don't actually keep lock alive
	// (TTL will expire because renewal can't keep up)
	lease.StartAutoRenewal()

	// Wait for lease to be lost
	time.Sleep(150 * time.Millisecond)

	if atomic.LoadInt32(&lostCalled) != 1 {
		t.Error("OnLost callback should have been called")
	}
}

func TestLeaseManager(t *testing.T) {
	engine := NewEngine()
	engine.Start()
	defer engine.Stop()

	manager := NewLeaseManager(engine)

	// Acquire lease through manager
	lease, err := manager.AcquireLease(context.Background(), "managed-key", "owner-1", 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to acquire lease: %v", err)
	}

	if !lease.IsValid() {
		t.Fatal("Lease should be valid")
	}

	// Get lease from manager
	retrieved, exists := manager.GetLease("managed-key")
	if !exists {
		t.Fatal("Lease should exist in manager")
	}
	if retrieved != lease {
		t.Error("Retrieved lease should be the same as original")
	}

	// Release through manager
	err = manager.ReleaseLease("managed-key")
	if err != nil {
		t.Fatalf("Failed to release lease: %v", err)
	}

	// Should no longer exist
	_, exists = manager.GetLease("managed-key")
	if exists {
		t.Error("Lease should not exist after release")
	}
}
