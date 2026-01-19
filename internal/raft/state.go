package raft

import (
	"errors"
	"log"
	"sync"
	"time"
)

var (
	ErrNotLeader    = errors.New("not the leader")
	ErrNoQuorum     = errors.New("cannot reach quorum")
	ErrTermExpired  = errors.New("term expired")
	ErrLogInconsist = errors.New("log inconsistency detected")
	ErrShuttingDown = errors.New("raft node is shutting down")
	ErrApplyTimeout = errors.New("apply timeout")
)

// NodeState represents the current state of a Raft node
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// RaftConfig holds Raft configuration parameters
type RaftConfig struct {
	NodeID            string
	ElectionTimeout   time.Duration // Base election timeout
	HeartbeatInterval time.Duration // Leader heartbeat interval
	ApplyTimeout      time.Duration // Timeout for applying commands
	MaxLogEntries     int           // Max entries per AppendEntries
}

// DefaultRaftConfig returns sensible defaults
func DefaultRaftConfig(nodeID string) *RaftConfig {
	return &RaftConfig{
		NodeID:            nodeID,
		ElectionTimeout:   150 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		ApplyTimeout:      5 * time.Second,
		MaxLogEntries:     100,
	}
}

// PeerInfo holds information about a peer node
type PeerInfo struct {
	ID      string
	Address string
}

// ApplyResult represents the result of applying a command
type ApplyResult struct {
	Index uint64
	Term  uint64
	Data  interface{}
	Error error
}

// ApplyFuture represents a pending apply operation
type ApplyFuture struct {
	index    uint64
	resultCh chan ApplyResult
}

// Wait waits for the apply to complete and returns the result
func (f *ApplyFuture) Wait() ApplyResult {
	return <-f.resultCh
}

// WaitTimeout waits for the apply with a timeout
func (f *ApplyFuture) WaitTimeout(timeout time.Duration) (ApplyResult, bool) {
	select {
	case result := <-f.resultCh:
		return result, true
	case <-time.After(timeout):
		return ApplyResult{Error: ErrApplyTimeout}, false
	}
}

// RaftNode represents a single Raft consensus node
type RaftNode struct {
	mu sync.RWMutex

	// Configuration
	config *RaftConfig

	// Persistent state (must be saved to stable storage)
	currentTerm uint64
	votedFor    string
	raftLog     *RaftLog

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Current node state
	state    NodeState
	leaderID string

	// Cluster configuration
	peers []PeerInfo

	// Channels
	stopCh     chan struct{}
	applyCh    chan LogEntry
	commitCh   chan struct{}
	applyQueue map[uint64]*ApplyFuture

	// Components
	transport    Transport
	stateMachine StateMachine
	persistence  Persistence

	// Callbacks
	onStateChange  func(old, new NodeState)
	onLeaderChange func(leaderID string)

	// Timing
	lastHeartbeat time.Time
	electionTimer *time.Timer

	// Running state
	wg      sync.WaitGroup
	running bool
}

// Transport interface for RPC communication
type Transport interface {
	SendRequestVote(peer PeerInfo, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peer PeerInfo, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(peer PeerInfo, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// StateMachine interface for applying committed entries
type StateMachine interface {
	Apply(entry LogEntry) interface{}
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Persistence interface for durable storage
type Persistence interface {
	SaveState(term uint64, votedFor string) error
	LoadState() (term uint64, votedFor string, err error)
	SaveLog(entries []LogEntry) error
	LoadLog() ([]LogEntry, error)
	SaveSnapshot(snapshot *Snapshot) error
	LoadSnapshot() (*Snapshot, error)
}

// Snapshot represents a state machine snapshot
type Snapshot struct {
	LastIndex uint64
	LastTerm  uint64
	Data      []byte
}

// NewRaftNode creates a new Raft node
func NewRaftNode(config *RaftConfig) *RaftNode {
	return &RaftNode{
		config:        config,
		state:         StateFollower,
		raftLog:       NewRaftLog(),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		peers:         make([]PeerInfo, 0),
		stopCh:        make(chan struct{}),
		applyCh:       make(chan LogEntry, 100),
		commitCh:      make(chan struct{}, 1),
		applyQueue:    make(map[uint64]*ApplyFuture),
		lastHeartbeat: time.Now(),
	}
}

// SetTransport sets the transport layer
func (rn *RaftNode) SetTransport(t Transport) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.transport = t
}

// SetStateMachine sets the state machine
func (rn *RaftNode) SetStateMachine(sm StateMachine) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.stateMachine = sm
}

// SetPersistence sets the persistence layer
func (rn *RaftNode) SetPersistence(p Persistence) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.persistence = p
}

// SetOnStateChange sets the state change callback
func (rn *RaftNode) SetOnStateChange(fn func(old, new NodeState)) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.onStateChange = fn
}

// SetOnLeaderChange sets the leader change callback
func (rn *RaftNode) SetOnLeaderChange(fn func(leaderID string)) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.onLeaderChange = fn
}

// AddPeer adds a peer node
func (rn *RaftNode) AddPeer(id, address string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for _, p := range rn.peers {
		if p.ID == id {
			return // Already exists
		}
	}
	rn.peers = append(rn.peers, PeerInfo{ID: id, Address: address})
}

// RemovePeer removes a peer node
func (rn *RaftNode) RemovePeer(id string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for i, p := range rn.peers {
		if p.ID == id {
			rn.peers = append(rn.peers[:i], rn.peers[i+1:]...)
			return
		}
	}
}

// Start begins the Raft node operation
func (rn *RaftNode) Start() error {
	rn.mu.Lock()
	if rn.running {
		rn.mu.Unlock()
		return nil
	}
	rn.running = true
	rn.stopCh = make(chan struct{})
	rn.mu.Unlock()

	// Restore persisted state
	if rn.persistence != nil {
		term, votedFor, err := rn.persistence.LoadState()
		if err == nil {
			rn.currentTerm = term
			rn.votedFor = votedFor
		}
		entries, err := rn.persistence.LoadLog()
		if err == nil && len(entries) > 0 {
			rn.raftLog.entries = entries
		}
	}

	// Start background goroutines
	rn.wg.Add(3)
	go rn.electionLoop()
	go rn.applyLoop()
	go rn.commitLoop()

	log.Printf("[Raft %s] Started as %s", rn.config.NodeID, rn.state)
	return nil
}

// Stop halts the Raft node
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	if !rn.running {
		rn.mu.Unlock()
		return
	}
	rn.running = false
	close(rn.stopCh)
	rn.mu.Unlock()

	rn.wg.Wait()
	log.Printf("[Raft %s] Stopped", rn.config.NodeID)
}

// GetState returns the current state
func (rn *RaftNode) GetState() NodeState {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

// GetTerm returns the current term
func (rn *RaftNode) GetTerm() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm
}

// GetLeader returns the current leader ID
func (rn *RaftNode) GetLeader() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderID
}

// IsLeader returns true if this node is the leader
func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state == StateLeader
}

// GetCommitIndex returns the current commit index
func (rn *RaftNode) GetCommitIndex() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.commitIndex
}

// QuorumSize returns the quorum size
func (rn *RaftNode) QuorumSize() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return (len(rn.peers)+1)/2 + 1
}

// HasQuorum checks if we can reach quorum
func (rn *RaftNode) HasQuorum() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.state != StateLeader {
		return false
	}

	// Count nodes we've heard from recently
	reachable := 1 // Count self
	timeout := rn.config.ElectionTimeout * 2
	now := time.Now()

	for peerID := range rn.matchIndex {
		// If we have a recent matchIndex update, peer is reachable
		if rn.matchIndex[peerID] > 0 || now.Sub(rn.lastHeartbeat) < timeout {
			reachable++
		}
	}

	return reachable >= rn.QuorumSize()
}

// becomeFollower transitions to follower state
func (rn *RaftNode) becomeFollower(term uint64) {
	oldState := rn.state
	rn.state = StateFollower
	rn.currentTerm = term
	rn.votedFor = ""

	if rn.persistence != nil {
		rn.persistence.SaveState(rn.currentTerm, rn.votedFor)
	}

	if oldState != StateFollower && rn.onStateChange != nil {
		go rn.onStateChange(oldState, StateFollower)
	}

	log.Printf("[Raft %s] Became follower for term %d", rn.config.NodeID, term)
}

// becomeCandidate transitions to candidate state
func (rn *RaftNode) becomeCandidate() {
	oldState := rn.state
	rn.state = StateCandidate
	rn.currentTerm++
	rn.votedFor = rn.config.NodeID
	rn.leaderID = ""

	if rn.persistence != nil {
		rn.persistence.SaveState(rn.currentTerm, rn.votedFor)
	}

	if rn.onStateChange != nil {
		go rn.onStateChange(oldState, StateCandidate)
	}

	log.Printf("[Raft %s] Became candidate for term %d", rn.config.NodeID, rn.currentTerm)
}

// becomeLeader transitions to leader state
func (rn *RaftNode) becomeLeader() {
	oldState := rn.state
	rn.state = StateLeader
	rn.leaderID = rn.config.NodeID

	// Initialize leader state
	lastIndex := rn.raftLog.LastIndex()
	for _, peer := range rn.peers {
		rn.nextIndex[peer.ID] = lastIndex + 1
		rn.matchIndex[peer.ID] = 0
	}

	if rn.onStateChange != nil {
		go rn.onStateChange(oldState, StateLeader)
	}
	if rn.onLeaderChange != nil {
		go rn.onLeaderChange(rn.config.NodeID)
	}

	log.Printf("[Raft %s] Became leader for term %d", rn.config.NodeID, rn.currentTerm)

	// Send initial empty AppendEntries (heartbeat)
	go rn.broadcastHeartbeat()
}

// persistState saves persistent state
func (rn *RaftNode) persistState() {
	if rn.persistence != nil {
		rn.persistence.SaveState(rn.currentTerm, rn.votedFor)
	}
}
