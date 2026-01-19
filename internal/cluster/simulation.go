package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"distributed-lock/internal/lock"
)

// Simulation provides a multi-node cluster simulation for testing
type Simulation struct {
	nodes   map[string]*Coordinator
	mu      sync.RWMutex
	running bool
}

// NewSimulation creates a new cluster simulation
func NewSimulation() *Simulation {
	return &Simulation{
		nodes: make(map[string]*Coordinator),
	}
}

// AddNode adds a node to the simulation
func (s *Simulation) AddNode(nodeID string, port int) *Coordinator {
	s.mu.Lock()
	defer s.mu.Unlock()

	config := DefaultCoordinatorConfig(nodeID, "127.0.0.1", port)
	coord := NewCoordinator(config)
	s.nodes[nodeID] = coord

	return coord
}

// GetNode returns a node by ID
func (s *Simulation) GetNode(nodeID string) (*Coordinator, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, exists := s.nodes[nodeID]
	return node, exists
}

// GetNodes returns all nodes
func (s *Simulation) GetNodes() []*Coordinator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Coordinator, 0, len(s.nodes))
	for _, node := range s.nodes {
		result = append(result, node)
	}
	return result
}

// ConnectNodes connects all nodes to each other as peers
func (s *Simulation) ConnectNodes() {
	s.mu.RLock()
	nodes := make([]*Coordinator, 0)
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	s.mu.RUnlock()

	// Each node should know about all other nodes
	for _, node := range nodes {
		for _, peer := range nodes {
			if node.config.NodeID != peer.config.NodeID {
				node.AddPeer(
					peer.config.NodeID,
					peer.config.Address,
					peer.config.Port,
				)
			}
		}
	}
}

// Start starts all nodes
func (s *Simulation) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, node := range s.nodes {
		if err := node.Start(); err != nil {
			return fmt.Errorf("failed to start node %s: %w", node.config.NodeID, err)
		}
	}
	s.running = true
	return nil
}

// Stop stops all nodes
func (s *Simulation) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, node := range s.nodes {
		node.Stop()
	}
	s.running = false
}

// BootstrapLeader bootstraps the first node as leader
func (s *Simulation) BootstrapLeader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for nodeID, node := range s.nodes {
		node.Bootstrap()
		return nodeID
	}
	return ""
}

// GetLeader returns the current leader node
func (s *Simulation) GetLeader() (*Coordinator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, node := range s.nodes {
		if node.IsLeader() {
			return node, nil
		}
	}
	return nil, ErrNoLeader
}

// GetFollowers returns all follower nodes
func (s *Simulation) GetFollowers() []*Coordinator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	followers := make([]*Coordinator, 0)
	for _, node := range s.nodes {
		if !node.IsLeader() {
			followers = append(followers, node)
		}
	}
	return followers
}

// KillNode stops a specific node (simulates node failure)
func (s *Simulation) KillNode(nodeID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes[nodeID]
	if !exists {
		return false
	}

	log.Printf("[Simulation] Killing node: %s", nodeID)
	node.Stop()
	delete(s.nodes, nodeID)

	// Remove from other nodes' peer lists
	for _, n := range s.nodes {
		n.RemovePeer(nodeID)
	}

	return true
}

// KillLeader kills the current leader node
func (s *Simulation) KillLeader() (string, error) {
	leader, err := s.GetLeader()
	if err != nil {
		return "", err
	}

	leaderID := leader.config.NodeID
	s.KillNode(leaderID)
	return leaderID, nil
}

// WaitForLeader waits for a leader to be elected
func (s *Simulation) WaitForLeader(timeout time.Duration) (*Coordinator, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if leader, err := s.GetLeader(); err == nil {
			return leader, nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("no leader elected within %v", timeout)
}

// WaitForNewLeader waits for a new leader different from the old one
func (s *Simulation) WaitForNewLeader(oldLeaderID string, timeout time.Duration) (*Coordinator, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if leader, err := s.GetLeader(); err == nil {
			if leader.config.NodeID != oldLeaderID {
				return leader, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("no new leader elected within %v", timeout)
}

// AcquireLockViaAnyNode tries to acquire a lock through any available node
func (s *Simulation) AcquireLockViaAnyNode(key, owner string, ttl time.Duration) (*lock.AcquireResponse, error) {
	s.mu.RLock()
	nodes := make([]*Coordinator, 0)
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	s.mu.RUnlock()

	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	// Try the first available node (it will forward to leader if needed)
	ctx := context.Background()
	resp := nodes[0].AcquireLock(ctx, &lock.AcquireRequest{
		Key:   key,
		Owner: owner,
		TTL:   ttl,
	})

	return resp, nil
}

// ReleaseLockViaAnyNode tries to release a lock through any available node
func (s *Simulation) ReleaseLockViaAnyNode(key, owner string) (*lock.ReleaseResponse, error) {
	s.mu.RLock()
	nodes := make([]*Coordinator, 0)
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	s.mu.RUnlock()

	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	ctx := context.Background()
	resp := nodes[0].ReleaseLock(ctx, &lock.ReleaseRequest{
		Key:   key,
		Owner: owner,
	})

	return resp, nil
}

// PrintClusterStatus prints the status of all nodes
func (s *Simulation) PrintClusterStatus() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fmt.Println("=== Cluster Status ===")
	for nodeID, node := range s.nodes {
		info := node.GetClusterInfo()
		role := "follower"
		if info.IsLeader {
			role = "LEADER"
		}
		fmt.Printf("  %s [%s] - Status: %s, Peers: %d alive\n",
			nodeID, role, info.Status, info.AlivePeers)
	}
	fmt.Println("======================")
}
