package leader

import (
	"sync"
	"time"
)

// HeartbeatMessage represents a leader heartbeat
type HeartbeatMessage struct {
	LeaderID  string    `json:"leader_id"`
	Term      uint64    `json:"term"`
	Timestamp time.Time `json:"timestamp"`
}

// HeartbeatSender sends periodic heartbeats to peers
type HeartbeatSender interface {
	SendHeartbeat(peerID string, msg *HeartbeatMessage) error
}

// Heartbeat manages heartbeat sending and receiving
type Heartbeat struct {
	nodeID   string
	interval time.Duration
	election *Election
	sender   HeartbeatSender

	peers   []string
	mu      sync.RWMutex
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
}

// NewHeartbeat creates a new heartbeat manager
func NewHeartbeat(nodeID string, election *Election, interval time.Duration) *Heartbeat {
	return &Heartbeat{
		nodeID:   nodeID,
		interval: interval,
		election: election,
		peers:    make([]string, 0),
	}
}

// SetSender sets the heartbeat sender implementation
func (h *Heartbeat) SetSender(sender HeartbeatSender) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sender = sender
}

// SetPeers sets the list of peer node IDs
func (h *Heartbeat) SetPeers(peers []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.peers = peers
}

// Start begins the heartbeat loop (only sends if leader)
func (h *Heartbeat) Start() {
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return
	}
	h.running = true
	h.stopCh = make(chan struct{})
	h.mu.Unlock()

	h.wg.Add(1)
	go h.heartbeatLoop()
}

// Stop halts the heartbeat loop
func (h *Heartbeat) Stop() {
	h.mu.Lock()
	if !h.running {
		h.mu.Unlock()
		return
	}
	h.running = false
	close(h.stopCh)
	h.mu.Unlock()

	h.wg.Wait()
}

// heartbeatLoop sends heartbeats if we're the leader
func (h *Heartbeat) heartbeatLoop() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if h.election.IsLeader() {
				h.sendHeartbeats()
			}
		case <-h.stopCh:
			return
		}
	}
}

// sendHeartbeats sends heartbeat to all peers
func (h *Heartbeat) sendHeartbeats() {
	h.mu.RLock()
	peers := h.peers
	sender := h.sender
	h.mu.RUnlock()

	if sender == nil {
		return
	}

	msg := &HeartbeatMessage{
		LeaderID:  h.nodeID,
		Term:      h.election.GetTerm(),
		Timestamp: time.Now(),
	}

	for _, peerID := range peers {
		// Send asynchronously to avoid blocking
		go func(peer string) {
			_ = sender.SendHeartbeat(peer, msg)
		}(peerID)
	}
}

// HandleHeartbeat processes an incoming heartbeat
func (h *Heartbeat) HandleHeartbeat(msg *HeartbeatMessage) {
	h.election.ReceiveHeartbeat(msg.LeaderID, msg.Term)
}

// PeerHealth tracks peer health status
type PeerHealth struct {
	PeerID       string
	LastSeen     time.Time
	IsHealthy    bool
	FailureCount int
}

// HealthTracker monitors peer health via heartbeats
type HealthTracker struct {
	peers   map[string]*PeerHealth
	timeout time.Duration
	mu      sync.RWMutex
}

// NewHealthTracker creates a new health tracker
func NewHealthTracker(timeout time.Duration) *HealthTracker {
	return &HealthTracker{
		peers:   make(map[string]*PeerHealth),
		timeout: timeout,
	}
}

// RegisterPeer adds a peer to track
func (t *HealthTracker) RegisterPeer(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peerID] = &PeerHealth{
		PeerID:    peerID,
		LastSeen:  time.Now(),
		IsHealthy: true,
	}
}

// RecordHeartbeat records a successful heartbeat from a peer
func (t *HealthTracker) RecordHeartbeat(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if peer, exists := t.peers[peerID]; exists {
		peer.LastSeen = time.Now()
		peer.IsHealthy = true
		peer.FailureCount = 0
	}
}

// RecordFailure records a failed communication with a peer
func (t *HealthTracker) RecordFailure(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if peer, exists := t.peers[peerID]; exists {
		peer.FailureCount++
		if peer.FailureCount >= 3 {
			peer.IsHealthy = false
		}
	}
}

// GetHealthyPeers returns list of healthy peer IDs
func (t *HealthTracker) GetHealthyPeers() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := time.Now()
	healthy := make([]string, 0)

	for peerID, peer := range t.peers {
		if peer.IsHealthy && now.Sub(peer.LastSeen) < t.timeout {
			healthy = append(healthy, peerID)
		}
	}

	return healthy
}

// GetPeerHealth returns health info for a specific peer
func (t *HealthTracker) GetPeerHealth(peerID string) (*PeerHealth, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	peer, exists := t.peers[peerID]
	return peer, exists
}
