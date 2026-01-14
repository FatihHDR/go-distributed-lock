package leader

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrNoLeader           = errors.New("no leader elected")
	ErrNotLeader          = errors.New("this node is not the leader")
	ErrElectionInProgress = errors.New("election in progress")
)

// State represents the node's role in the cluster
type State int

const (
	StateFollower State = iota
	StateCandidate
	StateLeader
)

func (s State) String() string {
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

// ElectionConfig holds configuration for leader election
type ElectionConfig struct {
	NodeID            string
	ElectionTimeout   time.Duration // Min time before starting election
	ElectionJitter    time.Duration // Random jitter to add to timeout
	HeartbeatInterval time.Duration // Leader heartbeat interval
}

// DefaultElectionConfig returns sensible defaults
func DefaultElectionConfig(nodeID string) *ElectionConfig {
	return &ElectionConfig{
		NodeID:            nodeID,
		ElectionTimeout:   5 * time.Second,
		ElectionJitter:    2 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	}
}

// Election manages simple leader election (bully algorithm variant)
type Election struct {
	config        *ElectionConfig
	state         State
	currentTerm   uint64
	leaderID      string
	lastHeartbeat time.Time

	peers []string        // Other node IDs
	votes map[string]bool // Votes received in current term

	mu      sync.RWMutex
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool

	// Callbacks
	onStateChange  func(old, new State)
	onLeaderChange func(leaderID string)
}

// NewElection creates a new election manager
func NewElection(config *ElectionConfig) *Election {
	return &Election{
		config:        config,
		state:         StateFollower,
		currentTerm:   0,
		votes:         make(map[string]bool),
		peers:         make([]string, 0),
		lastHeartbeat: time.Now(),
	}
}

// SetPeers sets the list of peer node IDs
func (e *Election) SetPeers(peers []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.peers = peers
}

// SetOnStateChange sets the state change callback
func (e *Election) SetOnStateChange(fn func(old, new State)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onStateChange = fn
}

// SetOnLeaderChange sets the leader change callback
func (e *Election) SetOnLeaderChange(fn func(leaderID string)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onLeaderChange = fn
}

// Start begins the election monitoring loop
func (e *Election) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.stopCh = make(chan struct{})
	e.mu.Unlock()

	e.wg.Add(1)
	go e.electionLoop()
}

// Stop halts the election monitoring
func (e *Election) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.running = false
	close(e.stopCh)
	e.mu.Unlock()

	e.wg.Wait()
}

// electionLoop monitors heartbeats and triggers elections
func (e *Election) electionLoop() {
	defer e.wg.Done()

	for {
		timeout := e.randomTimeout()
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:
			e.mu.RLock()
			state := e.state
			lastHB := e.lastHeartbeat
			e.mu.RUnlock()

			// If we're a follower and haven't heard from leader, start election
			if state == StateFollower && time.Since(lastHB) > e.config.ElectionTimeout {
				e.startElection()
			}

		case <-e.stopCh:
			timer.Stop()
			return
		}
	}
}

// randomTimeout returns election timeout with jitter
func (e *Election) randomTimeout() time.Duration {
	jitter := time.Duration(rand.Int63n(int64(e.config.ElectionJitter)))
	return e.config.ElectionTimeout + jitter
}

// startElection initiates a new election
func (e *Election) startElection() {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Transition to candidate
	oldState := e.state
	e.state = StateCandidate
	e.currentTerm++
	e.votes = make(map[string]bool)
	e.votes[e.config.NodeID] = true // Vote for self

	if e.onStateChange != nil {
		e.onStateChange(oldState, e.state)
	}

	// In a real implementation, we would request votes from peers here
	// For now, with simple simulation, if we have majority of "simulated" votes, become leader
	e.checkVotes()
}

// ReceiveVote records a vote from a peer
func (e *Election) ReceiveVote(fromNodeID string, term uint64, granted bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Ignore votes from old terms
	if term != e.currentTerm {
		return
	}

	if granted {
		e.votes[fromNodeID] = true
	}

	e.checkVotes()
}

// checkVotes determines if we have won the election (must hold lock)
func (e *Election) checkVotes() {
	if e.state != StateCandidate {
		return
	}

	// Calculate majority: (total nodes + 1) / 2
	totalNodes := len(e.peers) + 1 // Include self
	majority := (totalNodes / 2) + 1

	if len(e.votes) >= majority {
		e.becomeLeader()
	}
}

// becomeLeader transitions to leader state (must hold lock)
func (e *Election) becomeLeader() {
	oldState := e.state
	e.state = StateLeader
	e.leaderID = e.config.NodeID

	if e.onStateChange != nil {
		e.onStateChange(oldState, e.state)
	}
	if e.onLeaderChange != nil {
		e.onLeaderChange(e.leaderID)
	}
}

// ReceiveHeartbeat processes a heartbeat from the leader
func (e *Election) ReceiveHeartbeat(leaderID string, term uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// If term is higher than ours, update and step down
	if term > e.currentTerm {
		e.currentTerm = term
		if e.state != StateFollower {
			oldState := e.state
			e.state = StateFollower
			if e.onStateChange != nil {
				e.onStateChange(oldState, e.state)
			}
		}
	}

	// Update leader and heartbeat time
	if e.leaderID != leaderID {
		e.leaderID = leaderID
		if e.onLeaderChange != nil {
			e.onLeaderChange(leaderID)
		}
	}
	e.lastHeartbeat = time.Now()
}

// GetState returns the current state
func (e *Election) GetState() State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}

// GetLeader returns the current leader ID
func (e *Election) GetLeader() (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.leaderID == "" {
		return "", ErrNoLeader
	}
	return e.leaderID, nil
}

// IsLeader returns true if this node is the leader
func (e *Election) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state == StateLeader
}

// GetTerm returns the current term
func (e *Election) GetTerm() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentTerm
}

// ForceLeader forces this node to become leader (for testing/bootstrap)
func (e *Election) ForceLeader() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.currentTerm++
	e.becomeLeader()
}
