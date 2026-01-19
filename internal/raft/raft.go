package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Propose proposes a command to be replicated
func (rn *RaftNode) Propose(command []byte) (*ApplyFuture, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != StateLeader {
		return nil, ErrNotLeader
	}

	// Append to local log
	entry := rn.raftLog.AppendNew(rn.currentTerm, command)

	// Persist the entry
	if rn.persistence != nil {
		rn.persistence.SaveLog([]LogEntry{entry})
	}

	// Create future for this entry
	future := &ApplyFuture{
		index:    entry.Index,
		resultCh: make(chan ApplyResult, 1),
	}
	rn.applyQueue[entry.Index] = future

	// Update match index for self
	rn.matchIndex[rn.config.NodeID] = entry.Index

	log.Printf("[Raft %s] Proposed entry at index %d", rn.config.NodeID, entry.Index)

	// Immediately replicate to peers
	go rn.broadcastHeartbeat()

	return future, nil
}

// ProposeCommand proposes a lock command
func (rn *RaftNode) ProposeCommand(cmd *Command) (*ApplyFuture, error) {
	data, err := cmd.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode command: %w", err)
	}
	return rn.Propose(data)
}

// GetNodeInfo returns information about this node
func (rn *RaftNode) GetNodeInfo() NodeInfo {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	return NodeInfo{
		ID:          rn.config.NodeID,
		State:       rn.state.String(),
		Term:        rn.currentTerm,
		LeaderID:    rn.leaderID,
		CommitIndex: rn.commitIndex,
		LastApplied: rn.lastApplied,
		LogLength:   uint64(rn.raftLog.Size()),
	}
}

// NodeInfo contains information about a Raft node
type NodeInfo struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Term        uint64 `json:"term"`
	LeaderID    string `json:"leader_id"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
	LogLength   uint64 `json:"log_length"`
}

// ClusterInfo contains information about the cluster
type ClusterInfo struct {
	Nodes      []NodeInfo `json:"nodes"`
	LeaderID   string     `json:"leader_id"`
	Term       uint64     `json:"term"`
	QuorumSize int        `json:"quorum_size"`
	HasQuorum  bool       `json:"has_quorum"`
}

// GetClusterInfo returns information about the cluster
func (rn *RaftNode) GetClusterInfo() ClusterInfo {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(rn.peers)+1)

	// Add self
	nodes = append(nodes, NodeInfo{
		ID:          rn.config.NodeID,
		State:       rn.state.String(),
		Term:        rn.currentTerm,
		LeaderID:    rn.leaderID,
		CommitIndex: rn.commitIndex,
		LastApplied: rn.lastApplied,
		LogLength:   uint64(rn.raftLog.Size()),
	})

	// Add peers (with limited info)
	for _, peer := range rn.peers {
		nodes = append(nodes, NodeInfo{
			ID: peer.ID,
		})
	}

	return ClusterInfo{
		Nodes:      nodes,
		LeaderID:   rn.leaderID,
		Term:       rn.currentTerm,
		QuorumSize: rn.quorumSizeLocked(),
		HasQuorum:  rn.state == StateLeader,
	}
}

// Bootstrap forces this node to become leader (for initial cluster setup)
func (rn *RaftNode) Bootstrap() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if len(rn.peers) > 0 {
		return fmt.Errorf("cannot bootstrap with existing peers")
	}

	rn.currentTerm = 1
	rn.votedFor = rn.config.NodeID
	rn.state = StateLeader
	rn.leaderID = rn.config.NodeID

	rn.persistState()

	log.Printf("[Raft %s] Bootstrapped as leader for term 1", rn.config.NodeID)
	return nil
}

// WaitForLeader waits until a leader is elected or timeout
func (rn *RaftNode) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		rn.mu.RLock()
		leader := rn.leaderID
		rn.mu.RUnlock()

		if leader != "" {
			return leader, nil
		}

		time.Sleep(50 * time.Millisecond)
	}

	return "", fmt.Errorf("timeout waiting for leader")
}

// ForwardToLeader returns information needed to forward a request to the leader
func (rn *RaftNode) ForwardToLeader() (leaderID string, leaderAddr string, err error) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.leaderID == "" {
		return "", "", fmt.Errorf("no leader elected")
	}

	if rn.leaderID == rn.config.NodeID {
		return "", "", fmt.Errorf("already the leader")
	}

	// Find leader address
	for _, peer := range rn.peers {
		if peer.ID == rn.leaderID {
			return rn.leaderID, peer.Address, nil
		}
	}

	return rn.leaderID, "", fmt.Errorf("leader address unknown")
}

// String returns a string representation of node state
func (rn *RaftNode) String() string {
	info := rn.GetNodeInfo()
	data, _ := json.Marshal(info)
	return string(data)
}
