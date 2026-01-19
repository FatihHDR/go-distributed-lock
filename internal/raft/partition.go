package raft

import (
	"log"
	"sync"
	"time"
)

// PartitionDetector monitors network connectivity and detects partitions
type PartitionDetector struct {
	mu sync.RWMutex

	nodeID     string
	peers      []string
	quorumSize int
	timeout    time.Duration

	lastContact  map[string]time.Time
	reachable    map[string]bool
	quorumLostAt time.Time
	hasQuorum    bool

	onQuorumLost   func()
	onQuorumGained func()
}

// NewPartitionDetector creates a new partition detector
func NewPartitionDetector(nodeID string, timeout time.Duration) *PartitionDetector {
	return &PartitionDetector{
		nodeID:      nodeID,
		timeout:     timeout,
		lastContact: make(map[string]time.Time),
		reachable:   make(map[string]bool),
		hasQuorum:   true,
	}
}

// SetPeers configures the peer list
func (pd *PartitionDetector) SetPeers(peers []string) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	pd.peers = peers
	pd.quorumSize = (len(peers)+1)/2 + 1

	for _, p := range peers {
		if _, exists := pd.lastContact[p]; !exists {
			pd.lastContact[p] = time.Now()
			pd.reachable[p] = true
		}
	}
}

// SetOnQuorumLost sets callback for when quorum is lost
func (pd *PartitionDetector) SetOnQuorumLost(fn func()) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.onQuorumLost = fn
}

// SetOnQuorumGained sets callback for when quorum is regained
func (pd *PartitionDetector) SetOnQuorumGained(fn func()) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.onQuorumGained = fn
}

// RecordContact records successful contact with a peer
func (pd *PartitionDetector) RecordContact(peerID string) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	pd.lastContact[peerID] = time.Now()
	wasReachable := pd.reachable[peerID]
	pd.reachable[peerID] = true

	if !wasReachable {
		pd.checkQuorum()
	}
}

// RecordFailure records failed contact with a peer
func (pd *PartitionDetector) RecordFailure(peerID string) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	wasReachable := pd.reachable[peerID]
	pd.reachable[peerID] = false

	if wasReachable {
		pd.checkQuorum()
	}
}

// checkQuorum checks if we still have quorum (must hold lock)
func (pd *PartitionDetector) checkQuorum() {
	now := time.Now()
	reachableCount := 1 // Count self

	for _, peerID := range pd.peers {
		lastSeen := pd.lastContact[peerID]
		if pd.reachable[peerID] && now.Sub(lastSeen) < pd.timeout {
			reachableCount++
		}
	}

	hadQuorum := pd.hasQuorum
	pd.hasQuorum = reachableCount >= pd.quorumSize

	if hadQuorum && !pd.hasQuorum {
		pd.quorumLostAt = now
		log.Printf("[Partition %s] Quorum lost! Reachable: %d, Need: %d",
			pd.nodeID, reachableCount, pd.quorumSize)
		if pd.onQuorumLost != nil {
			go pd.onQuorumLost()
		}
	} else if !hadQuorum && pd.hasQuorum {
		log.Printf("[Partition %s] Quorum regained! Reachable: %d",
			pd.nodeID, reachableCount)
		if pd.onQuorumGained != nil {
			go pd.onQuorumGained()
		}
	}
}

// HasQuorum returns true if we can reach a majority
func (pd *PartitionDetector) HasQuorum() bool {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return pd.hasQuorum
}

// GetReachableCount returns the number of reachable peers
func (pd *PartitionDetector) GetReachableCount() int {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	count := 1 // Self
	now := time.Now()

	for _, peerID := range pd.peers {
		if pd.reachable[peerID] && now.Sub(pd.lastContact[peerID]) < pd.timeout {
			count++
		}
	}
	return count
}

// GetUnreachablePeers returns list of currently unreachable peers
func (pd *PartitionDetector) GetUnreachablePeers() []string {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	var unreachable []string
	now := time.Now()

	for _, peerID := range pd.peers {
		if !pd.reachable[peerID] || now.Sub(pd.lastContact[peerID]) >= pd.timeout {
			unreachable = append(unreachable, peerID)
		}
	}
	return unreachable
}

// QuorumLostDuration returns how long we've been without quorum
func (pd *PartitionDetector) QuorumLostDuration() time.Duration {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	if pd.hasQuorum {
		return 0
	}
	return time.Since(pd.quorumLostAt)
}

// PeriodicCheck performs periodic connectivity check
func (pd *PartitionDetector) PeriodicCheck() {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	now := time.Now()

	// Mark peers as unreachable if we haven't heard from them
	for peerID := range pd.reachable {
		if pd.reachable[peerID] && now.Sub(pd.lastContact[peerID]) >= pd.timeout {
			pd.reachable[peerID] = false
		}
	}

	pd.checkQuorum()
}

// LeaderPartitionHandler handles partition scenarios for leaders
type LeaderPartitionHandler struct {
	mu sync.RWMutex

	node                 *RaftNode
	detector             *PartitionDetector
	stepDownOnQuorumLoss bool
	graceperiod          time.Duration

	stopCh  chan struct{}
	running bool
}

// NewLeaderPartitionHandler creates a partition handler for leaders
func NewLeaderPartitionHandler(node *RaftNode, detector *PartitionDetector) *LeaderPartitionHandler {
	return &LeaderPartitionHandler{
		node:                 node,
		detector:             detector,
		stepDownOnQuorumLoss: true,
		graceperiod:          node.config.ElectionTimeout * 3,
	}
}

// Start begins partition monitoring
func (lph *LeaderPartitionHandler) Start() {
	lph.mu.Lock()
	if lph.running {
		lph.mu.Unlock()
		return
	}
	lph.running = true
	lph.stopCh = make(chan struct{})
	lph.mu.Unlock()

	go lph.monitorLoop()
}

// Stop halts partition monitoring
func (lph *LeaderPartitionHandler) Stop() {
	lph.mu.Lock()
	if !lph.running {
		lph.mu.Unlock()
		return
	}
	lph.running = false
	close(lph.stopCh)
	lph.mu.Unlock()
}

// monitorLoop periodically checks for partition conditions
func (lph *LeaderPartitionHandler) monitorLoop() {
	ticker := time.NewTicker(lph.node.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lph.check()
		case <-lph.stopCh:
			return
		}
	}
}

// check performs partition check
func (lph *LeaderPartitionHandler) check() {
	// Only relevant if we're the leader
	if !lph.node.IsLeader() {
		return
	}

	lph.detector.PeriodicCheck()

	if !lph.detector.HasQuorum() {
		// Check if we've been without quorum long enough
		if lph.stepDownOnQuorumLoss && lph.detector.QuorumLostDuration() > lph.graceperiod {
			log.Printf("[Partition] Leader stepping down due to quorum loss")
			lph.node.StepDown()
		}
	}
}

// StepDown forces the node to step down from leadership
func (rn *RaftNode) StepDown() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state == StateLeader {
		log.Printf("[Raft %s] Stepping down from leadership", rn.config.NodeID)
		rn.becomeFollower(rn.currentTerm)
	}
}

// CanServeRequest checks if this node can serve client requests
func (rn *RaftNode) CanServeRequest() error {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.state != StateLeader {
		return ErrNotLeader
	}

	// Check if we have a working quorum
	reachable := 1 // Self
	for _, matched := range rn.matchIndex {
		if matched > 0 {
			reachable++
		}
	}

	if reachable < rn.quorumSizeLocked() {
		return ErrNoQuorum
	}

	return nil
}
