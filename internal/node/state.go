package node

import (
	"sync"
	"time"
)

// Status represents the overall node status
type Status int

const (
	StatusInitializing Status = iota
	StatusReady
	StatusJoining
	StatusLeaving
	StatusDown
)

func (s Status) String() string {
	switch s {
	case StatusInitializing:
		return "initializing"
	case StatusReady:
		return "ready"
	case StatusJoining:
		return "joining"
	case StatusLeaving:
		return "leaving"
	case StatusDown:
		return "down"
	default:
		return "unknown"
	}
}

// Info contains node information
type Info struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Port      int       `json:"port"`
	StartedAt time.Time `json:"started_at"`
}

// State manages the state of a node in the cluster
type State struct {
	info     Info
	status   Status
	isLeader bool
	leaderID string
	peers    map[string]*PeerInfo

	mu sync.RWMutex

	// Callbacks
	onStatusChange func(old, new Status)
}

// PeerInfo contains information about a peer node
type PeerInfo struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	Port     int       `json:"port"`
	LastSeen time.Time `json:"last_seen"`
	IsAlive  bool      `json:"is_alive"`
}

// NewState creates a new node state
func NewState(id, address string, port int) *State {
	return &State{
		info: Info{
			ID:        id,
			Address:   address,
			Port:      port,
			StartedAt: time.Now(),
		},
		status: StatusInitializing,
		peers:  make(map[string]*PeerInfo),
	}
}

// GetInfo returns the node's info
func (s *State) GetInfo() Info {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.info
}

// GetID returns the node's ID
func (s *State) GetID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.info.ID
}

// GetStatus returns the current status
func (s *State) GetStatus() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

// SetStatus updates the node status
func (s *State) SetStatus(status Status) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status == status {
		return
	}

	old := s.status
	s.status = status

	if s.onStatusChange != nil {
		s.onStatusChange(old, status)
	}
}

// SetOnStatusChange sets the status change callback
func (s *State) SetOnStatusChange(fn func(old, new Status)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onStatusChange = fn
}

// IsLeader returns whether this node is the leader
func (s *State) IsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isLeader
}

// SetLeader updates the leader state
func (s *State) SetLeader(isLeader bool, leaderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isLeader = isLeader
	s.leaderID = leaderID
}

// GetLeaderID returns the current leader's ID
func (s *State) GetLeaderID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderID
}

// AddPeer adds or updates a peer
func (s *State) AddPeer(id, address string, port int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.peers[id] = &PeerInfo{
		ID:       id,
		Address:  address,
		Port:     port,
		LastSeen: time.Now(),
		IsAlive:  true,
	}
}

// RemovePeer removes a peer
func (s *State) RemovePeer(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peers, id)
}

// UpdatePeerSeen updates the last seen time for a peer
func (s *State) UpdatePeerSeen(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if peer, exists := s.peers[id]; exists {
		peer.LastSeen = time.Now()
		peer.IsAlive = true
	}
}

// SetPeerAlive sets a peer's alive status
func (s *State) SetPeerAlive(id string, alive bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if peer, exists := s.peers[id]; exists {
		peer.IsAlive = alive
	}
}

// GetPeer returns a peer by ID
func (s *State) GetPeer(id string) (*PeerInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	peer, exists := s.peers[id]
	return peer, exists
}

// GetPeers returns all peers
func (s *State) GetPeers() []*PeerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(s.peers))
	for _, peer := range s.peers {
		result = append(result, peer)
	}
	return result
}

// GetAlivePeers returns only alive peers
func (s *State) GetAlivePeers() []*PeerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*PeerInfo, 0)
	for _, peer := range s.peers {
		if peer.IsAlive {
			result = append(result, peer)
		}
	}
	return result
}

// GetPeerIDs returns all peer IDs
func (s *State) GetPeerIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.peers))
	for id := range s.peers {
		ids = append(ids, id)
	}
	return ids
}

// ClusterInfo represents the cluster state from this node's perspective
type ClusterInfo struct {
	NodeID     string `json:"node_id"`
	LeaderID   string `json:"leader_id"`
	IsLeader   bool   `json:"is_leader"`
	Status     string `json:"status"`
	PeerCount  int    `json:"peer_count"`
	AlivePeers int    `json:"alive_peers"`
	Uptime     string `json:"uptime"`
}

// GetClusterInfo returns cluster information
func (s *State) GetClusterInfo() ClusterInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	alivePeers := 0
	for _, peer := range s.peers {
		if peer.IsAlive {
			alivePeers++
		}
	}

	return ClusterInfo{
		NodeID:     s.info.ID,
		LeaderID:   s.leaderID,
		IsLeader:   s.isLeader,
		Status:     s.status.String(),
		PeerCount:  len(s.peers),
		AlivePeers: alivePeers,
		Uptime:     time.Since(s.info.StartedAt).String(),
	}
}
