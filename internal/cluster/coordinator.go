package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"distributed-lock/internal/leader"
	"distributed-lock/internal/lock"
	"distributed-lock/internal/node"
	"distributed-lock/internal/rpc"
)

var (
	ErrNoLeader        = errors.New("no leader available")
	ErrNotLeader       = errors.New("this node is not the leader")
	ErrForwardFailed   = errors.New("failed to forward request to leader")
	ErrLeaderUnhealthy = errors.New("leader is unhealthy")
)

// CoordinatorConfig holds cluster coordinator configuration
type CoordinatorConfig struct {
	NodeID            string
	Address           string
	Port              int
	LeaderTTL         time.Duration // TTL for leadership lease
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	ForwardTimeout    time.Duration // Timeout for forwarding requests
}

// DefaultCoordinatorConfig returns sensible defaults
func DefaultCoordinatorConfig(nodeID, address string, port int) *CoordinatorConfig {
	return &CoordinatorConfig{
		NodeID:            nodeID,
		Address:           address,
		Port:              port,
		LeaderTTL:         10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		ElectionTimeout:   5 * time.Second,
		ForwardTimeout:    5 * time.Second,
	}
}

// Coordinator manages the cluster and coordinates lock operations
type Coordinator struct {
	config     *CoordinatorConfig
	nodeState  *node.State
	election   *leader.Election
	lockEngine *lock.Engine

	// Peer connections for forwarding
	peerClients map[string]*rpc.Client

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Callbacks
	onBecomeLeader   func()
	onLoseLeadership func()
}

// NewCoordinator creates a new cluster coordinator
func NewCoordinator(config *CoordinatorConfig) *Coordinator {
	nodeState := node.NewState(config.NodeID, config.Address, config.Port)

	electionConfig := &leader.ElectionConfig{
		NodeID:            config.NodeID,
		ElectionTimeout:   config.ElectionTimeout,
		ElectionJitter:    config.ElectionTimeout / 2,
		HeartbeatInterval: config.HeartbeatInterval,
	}
	election := leader.NewElection(electionConfig)

	lockEngine := lock.NewEngine()

	c := &Coordinator{
		config:      config,
		nodeState:   nodeState,
		election:    election,
		lockEngine:  lockEngine,
		peerClients: make(map[string]*rpc.Client),
	}

	// Set up election callbacks
	election.SetOnStateChange(func(old, new leader.State) {
		log.Printf("[%s] State changed: %s -> %s", config.NodeID, old, new)

		if new == leader.StateLeader {
			nodeState.SetLeader(true, config.NodeID)
			if c.onBecomeLeader != nil {
				c.onBecomeLeader()
			}
		} else if old == leader.StateLeader {
			nodeState.SetLeader(false, "")
			if c.onLoseLeadership != nil {
				c.onLoseLeadership()
			}
		}
	})

	election.SetOnLeaderChange(func(leaderID string) {
		log.Printf("[%s] Leader changed to: %s", config.NodeID, leaderID)
		nodeState.SetLeader(leaderID == config.NodeID, leaderID)
	})

	return c
}

// SetOnBecomeLeader sets callback for when this node becomes leader
func (c *Coordinator) SetOnBecomeLeader(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onBecomeLeader = fn
}

// SetOnLoseLeadership sets callback for when this node loses leadership
func (c *Coordinator) SetOnLoseLeadership(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onLoseLeadership = fn
}

// AddPeer adds a peer node
func (c *Coordinator) AddPeer(id, address string, port int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodeState.AddPeer(id, address, port)

	// Create RPC client for peer
	addr := fmt.Sprintf("%s:%d", address, port)
	client := rpc.NewClient(addr)
	c.peerClients[id] = client

	// Update election peers
	c.election.SetPeers(c.nodeState.GetPeerIDs())

	return nil
}

// RemovePeer removes a peer node
func (c *Coordinator) RemovePeer(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if client, exists := c.peerClients[id]; exists {
		client.Close()
		delete(c.peerClients, id)
	}
	c.nodeState.RemovePeer(id)
	c.election.SetPeers(c.nodeState.GetPeerIDs())
}

// Start begins the coordinator
func (c *Coordinator) Start() error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = true
	c.stopCh = make(chan struct{})
	c.mu.Unlock()

	c.lockEngine.Start()
	c.election.Start()
	c.nodeState.SetStatus(node.StatusReady)

	// Start heartbeat sender for leader
	c.wg.Add(1)
	go c.heartbeatLoop()

	log.Printf("[%s] Coordinator started", c.config.NodeID)
	return nil
}

// Stop halts the coordinator
func (c *Coordinator) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	close(c.stopCh)
	c.mu.Unlock()

	c.wg.Wait()
	c.election.Stop()
	c.lockEngine.Stop()
	c.nodeState.SetStatus(node.StatusDown)

	// Close peer connections
	c.mu.Lock()
	for _, client := range c.peerClients {
		client.Close()
	}
	c.peerClients = make(map[string]*rpc.Client)
	c.mu.Unlock()

	log.Printf("[%s] Coordinator stopped", c.config.NodeID)
}

// heartbeatLoop sends heartbeats when leader
func (c *Coordinator) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.election.IsLeader() {
				c.sendHeartbeats()
			}
		case <-c.stopCh:
			return
		}
	}
}

// sendHeartbeats sends heartbeat to all peers
func (c *Coordinator) sendHeartbeats() {
	c.mu.RLock()
	clients := make(map[string]*rpc.Client)
	for id, client := range c.peerClients {
		clients[id] = client
	}
	c.mu.RUnlock()

	msg := &leader.HeartbeatMessage{
		LeaderID:  c.config.NodeID,
		Term:      c.election.GetTerm(),
		Timestamp: time.Now(),
	}

	for peerID, client := range clients {
		go func(id string, cl *rpc.Client) {
			// Try to connect if not connected
			if err := cl.Connect(); err != nil {
				c.nodeState.SetPeerAlive(id, false)
				return
			}

			_, err := cl.Call(rpc.MsgHeartbeat, msg)
			if err != nil {
				c.nodeState.SetPeerAlive(id, false)
			} else {
				c.nodeState.UpdatePeerSeen(id)
			}
		}(peerID, client)
	}
}

// ReceiveHeartbeat processes incoming heartbeat
func (c *Coordinator) ReceiveHeartbeat(msg *leader.HeartbeatMessage) {
	c.election.ReceiveHeartbeat(msg.LeaderID, msg.Term)
	c.nodeState.UpdatePeerSeen(msg.LeaderID)
}

// Bootstrap forces this node to become leader (for initial cluster setup)
func (c *Coordinator) Bootstrap() {
	log.Printf("[%s] Bootstrapping as leader", c.config.NodeID)
	c.election.ForceLeader()
}

// IsLeader returns whether this node is the leader
func (c *Coordinator) IsLeader() bool {
	return c.election.IsLeader()
}

// GetLeader returns the current leader ID
func (c *Coordinator) GetLeader() (string, error) {
	return c.election.GetLeader()
}

// GetNodeState returns the node state
func (c *Coordinator) GetNodeState() *node.State {
	return c.nodeState
}

// GetElection returns the election manager
func (c *Coordinator) GetElection() *leader.Election {
	return c.election
}

// GetLockEngine returns the lock engine
func (c *Coordinator) GetLockEngine() *lock.Engine {
	return c.lockEngine
}

// AcquireLock handles lock acquisition (forwards to leader if not leader)
func (c *Coordinator) AcquireLock(ctx context.Context, req *lock.AcquireRequest) *lock.AcquireResponse {
	// If we're the leader, handle locally
	if c.IsLeader() {
		return c.lockEngine.AcquireLockWithContext(ctx, req)
	}

	// Forward to leader
	return c.forwardAcquire(ctx, req)
}

// forwardAcquire forwards acquire request to leader
func (c *Coordinator) forwardAcquire(ctx context.Context, req *lock.AcquireRequest) *lock.AcquireResponse {
	leaderID, err := c.GetLeader()
	if err != nil {
		return &lock.AcquireResponse{
			Success: false,
			Error:   ErrNoLeader.Error(),
		}
	}

	c.mu.RLock()
	client, exists := c.peerClients[leaderID]
	c.mu.RUnlock()

	if !exists {
		return &lock.AcquireResponse{
			Success: false,
			Error:   ErrNoLeader.Error(),
		}
	}

	// Connect to leader
	if err := client.Connect(); err != nil {
		return &lock.AcquireResponse{
			Success: false,
			Error:   ErrForwardFailed.Error(),
		}
	}

	// Forward with timeout
	resp, err := client.AcquireLock(req.Key, req.Owner, req.TTL)
	if err != nil {
		return &lock.AcquireResponse{
			Success: false,
			Error:   ErrForwardFailed.Error(),
		}
	}

	return resp
}

// RenewLock handles lock renewal (forwards to leader if not leader)
func (c *Coordinator) RenewLock(ctx context.Context, req *lock.RenewRequest) *lock.RenewResponse {
	if c.IsLeader() {
		return c.lockEngine.RenewLockWithContext(ctx, req)
	}

	return c.forwardRenew(ctx, req)
}

// forwardRenew forwards renew request to leader
func (c *Coordinator) forwardRenew(ctx context.Context, req *lock.RenewRequest) *lock.RenewResponse {
	leaderID, err := c.GetLeader()
	if err != nil {
		return &lock.RenewResponse{
			Success: false,
			Error:   ErrNoLeader.Error(),
		}
	}

	c.mu.RLock()
	client, exists := c.peerClients[leaderID]
	c.mu.RUnlock()

	if !exists {
		return &lock.RenewResponse{
			Success: false,
			Error:   ErrNoLeader.Error(),
		}
	}

	if err := client.Connect(); err != nil {
		return &lock.RenewResponse{
			Success: false,
			Error:   ErrForwardFailed.Error(),
		}
	}

	resp, err := client.RenewLock(req.Key, req.Owner, req.TTL)
	if err != nil {
		return &lock.RenewResponse{
			Success: false,
			Error:   ErrForwardFailed.Error(),
		}
	}

	return resp
}

// ReleaseLock handles lock release (forwards to leader if not leader)
func (c *Coordinator) ReleaseLock(ctx context.Context, req *lock.ReleaseRequest) *lock.ReleaseResponse {
	if c.IsLeader() {
		return c.lockEngine.ReleaseLockWithContext(ctx, req)
	}

	return c.forwardRelease(ctx, req)
}

// forwardRelease forwards release request to leader
func (c *Coordinator) forwardRelease(ctx context.Context, req *lock.ReleaseRequest) *lock.ReleaseResponse {
	leaderID, err := c.GetLeader()
	if err != nil {
		return &lock.ReleaseResponse{
			Success: false,
			Error:   ErrNoLeader.Error(),
		}
	}

	c.mu.RLock()
	client, exists := c.peerClients[leaderID]
	c.mu.RUnlock()

	if !exists {
		return &lock.ReleaseResponse{
			Success: false,
			Error:   ErrNoLeader.Error(),
		}
	}

	if err := client.Connect(); err != nil {
		return &lock.ReleaseResponse{
			Success: false,
			Error:   ErrForwardFailed.Error(),
		}
	}

	resp, err := client.ReleaseLock(req.Key, req.Owner)
	if err != nil {
		return &lock.ReleaseResponse{
			Success: false,
			Error:   ErrForwardFailed.Error(),
		}
	}

	return resp
}

// GetClusterInfo returns cluster information
func (c *Coordinator) GetClusterInfo() node.ClusterInfo {
	return c.nodeState.GetClusterInfo()
}
