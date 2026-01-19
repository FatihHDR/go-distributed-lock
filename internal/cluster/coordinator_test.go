package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"distributed-lock/internal/lock"
)

func TestCoordinator_SingleNode(t *testing.T) {
	config := DefaultCoordinatorConfig("node-1", "127.0.0.1", 9000)
	coord := NewCoordinator(config)

	err := coord.Start()
	if err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}
	defer coord.Stop()

	// Bootstrap as leader
	coord.Bootstrap()

	if !coord.IsLeader() {
		t.Fatal("Node should be leader after bootstrap")
	}

	// Test lock operations
	ctx := context.Background()
	resp := coord.AcquireLock(ctx, &lock.AcquireRequest{
		Key:   "test-key",
		Owner: "client-1",
		TTL:   5 * time.Second,
	})

	if !resp.Success {
		t.Fatalf("Failed to acquire lock: %s", resp.Error)
	}

	// Release lock
	releaseResp := coord.ReleaseLock(ctx, &lock.ReleaseRequest{
		Key:   "test-key",
		Owner: "client-1",
	})

	if !releaseResp.Success {
		t.Fatalf("Failed to release lock: %s", releaseResp.Error)
	}
}

func TestCoordinator_LeaderElection(t *testing.T) {
	// Create two nodes
	config1 := DefaultCoordinatorConfig("node-1", "127.0.0.1", 9001)
	config1.ElectionTimeout = 500 * time.Millisecond
	coord1 := NewCoordinator(config1)

	config2 := DefaultCoordinatorConfig("node-2", "127.0.0.1", 9002)
	config2.ElectionTimeout = 500 * time.Millisecond
	coord2 := NewCoordinator(config2)

	// Add peers
	coord1.AddPeer("node-2", "127.0.0.1", 9002)
	coord2.AddPeer("node-1", "127.0.0.1", 9001)

	// Start both
	coord1.Start()
	coord2.Start()
	defer coord1.Stop()
	defer coord2.Stop()

	// Bootstrap node-1 as leader
	coord1.Bootstrap()

	if !coord1.IsLeader() {
		t.Fatal("Node-1 should be leader after bootstrap")
	}

	if coord2.IsLeader() {
		t.Fatal("Node-2 should not be leader")
	}
}

func TestSimulation_ThreeNodeCluster(t *testing.T) {
	sim := NewSimulation()

	// Add 3 nodes
	sim.AddNode("node-1", 9010)
	sim.AddNode("node-2", 9011)
	sim.AddNode("node-3", 9012)

	// Connect nodes
	sim.ConnectNodes()

	// Start cluster
	if err := sim.Start(); err != nil {
		t.Fatalf("Failed to start simulation: %v", err)
	}
	defer sim.Stop()

	// Bootstrap leader
	leaderID := sim.BootstrapLeader()
	if leaderID == "" {
		t.Fatal("Failed to bootstrap leader")
	}

	// Wait for leader
	leader, err := sim.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("No leader elected: %v", err)
	}

	t.Logf("Leader elected: %s", leader.config.NodeID)

	// Verify followers
	followers := sim.GetFollowers()
	if len(followers) != 2 {
		t.Errorf("Expected 2 followers, got %d", len(followers))
	}
}

func TestSimulation_LeaderFailover(t *testing.T) {
	sim := NewSimulation()

	// Add 3 nodes with short election timeout
	for i := 1; i <= 3; i++ {
		sim.AddNode(
			fmt.Sprintf("node-%d", i),
			9020+i,
		)
	}

	sim.ConnectNodes()

	if err := sim.Start(); err != nil {
		t.Fatalf("Failed to start simulation: %v", err)
	}
	defer sim.Stop()

	// Bootstrap leader
	sim.BootstrapLeader()

	// Get initial leader
	leader, err := sim.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("No initial leader: %v", err)
	}
	oldLeaderID := leader.config.NodeID
	t.Logf("Initial leader: %s", oldLeaderID)

	// Kill the leader
	killedID, err := sim.KillLeader()
	if err != nil {
		t.Fatalf("Failed to kill leader: %v", err)
	}
	t.Logf("Killed leader: %s", killedID)

	// System should still work - remaining nodes available
	nodes := sim.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 remaining nodes, got %d", len(nodes))
	}

	// Verify remaining nodes are still operational
	for _, node := range nodes {
		info := node.GetClusterInfo()
		if info.Status != "ready" {
			t.Errorf("Node %s should be ready, got %s", info.NodeID, info.Status)
		}
	}
}

func TestSimulation_LockAcrossNodes(t *testing.T) {
	sim := NewSimulation()

	// Add 2 nodes
	sim.AddNode("node-1", 9030)
	sim.AddNode("node-2", 9031)

	sim.ConnectNodes()
	sim.Start()
	defer sim.Stop()

	// Bootstrap node-1 as leader
	sim.BootstrapLeader()

	// Wait for leader
	_, err := sim.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("No leader: %v", err)
	}

	// Acquire lock via leader
	leader, _ := sim.GetLeader()
	ctx := context.Background()

	resp := leader.AcquireLock(ctx, &lock.AcquireRequest{
		Key:   "shared-resource",
		Owner: "client-1",
		TTL:   5 * time.Second,
	})

	if !resp.Success {
		t.Fatalf("Failed to acquire lock via leader: %s", resp.Error)
	}

	// Verify lock exists on leader's engine
	lockInfo, err := leader.GetLockEngine().GetLock("shared-resource")
	if err != nil {
		t.Fatalf("Lock should exist on leader: %v", err)
	}

	if lockInfo.Owner != "client-1" {
		t.Errorf("Expected owner 'client-1', got '%s'", lockInfo.Owner)
	}
}
