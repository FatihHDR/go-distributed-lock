package raft

import (
	"sync"
	"testing"
	"time"
)

func TestRaftLog_AppendAndGet(t *testing.T) {
	log := NewRaftLog()

	// Test empty log
	if log.LastIndex() != 0 {
		t.Errorf("Expected last index 0, got %d", log.LastIndex())
	}
	if log.LastTerm() != 0 {
		t.Errorf("Expected last term 0, got %d", log.LastTerm())
	}

	// Append entries
	entry1 := log.AppendNew(1, []byte("cmd1"))
	if entry1.Index != 1 {
		t.Errorf("Expected index 1, got %d", entry1.Index)
	}

	entry2 := log.AppendNew(1, []byte("cmd2"))
	if entry2.Index != 2 {
		t.Errorf("Expected index 2, got %d", entry2.Index)
	}

	entry3 := log.AppendNew(2, []byte("cmd3"))
	if entry3.Index != 3 {
		t.Errorf("Expected index 3, got %d", entry3.Index)
	}

	// Get entries
	got, ok := log.Get(1)
	if !ok || string(got.Command) != "cmd1" {
		t.Errorf("Expected cmd1, got %v", got)
	}

	got, ok = log.Get(2)
	if !ok || string(got.Command) != "cmd2" {
		t.Errorf("Expected cmd2, got %v", got)
	}

	// Get non-existent
	_, ok = log.Get(0)
	if ok {
		t.Error("Expected no entry at index 0")
	}

	_, ok = log.Get(10)
	if ok {
		t.Error("Expected no entry at index 10")
	}

	// Check last index/term
	if log.LastIndex() != 3 {
		t.Errorf("Expected last index 3, got %d", log.LastIndex())
	}
	if log.LastTerm() != 2 {
		t.Errorf("Expected last term 2, got %d", log.LastTerm())
	}
}

func TestRaftLog_TruncateAfter(t *testing.T) {
	log := NewRaftLog()

	log.AppendNew(1, []byte("cmd1"))
	log.AppendNew(1, []byte("cmd2"))
	log.AppendNew(2, []byte("cmd3"))
	log.AppendNew(2, []byte("cmd4"))

	// Truncate after index 2
	log.TruncateAfter(2)

	if log.LastIndex() != 2 {
		t.Errorf("Expected last index 2 after truncate, got %d", log.LastIndex())
	}

	_, ok := log.Get(3)
	if ok {
		t.Error("Entry at index 3 should not exist after truncate")
	}

	_, ok = log.Get(2)
	if !ok {
		t.Error("Entry at index 2 should still exist")
	}
}

func TestRaftLog_IsUpToDate(t *testing.T) {
	log := NewRaftLog()

	log.AppendNew(1, []byte("cmd1"))
	log.AppendNew(2, []byte("cmd2"))

	// Should be up to date: same term, same or higher index
	if !log.IsUpToDate(2, 2) {
		t.Error("Expected (2, 2) to be up to date")
	}
	if !log.IsUpToDate(3, 2) {
		t.Error("Expected (3, 2) to be up to date")
	}

	// Should be up to date: higher term
	if !log.IsUpToDate(1, 3) {
		t.Error("Expected (1, 3) to be up to date")
	}

	// Should NOT be up to date: lower term
	if log.IsUpToDate(10, 1) {
		t.Error("Expected (10, 1) to NOT be up to date")
	}

	// Should NOT be up to date: same term, lower index
	if log.IsUpToDate(1, 2) {
		t.Error("Expected (1, 2) to NOT be up to date")
	}
}

func TestRaftLog_MatchTerm(t *testing.T) {
	log := NewRaftLog()

	log.AppendNew(1, []byte("cmd1"))
	log.AppendNew(2, []byte("cmd2"))

	if !log.MatchTerm(1, 1) {
		t.Error("Term at index 1 should match 1")
	}
	if !log.MatchTerm(2, 2) {
		t.Error("Term at index 2 should match 2")
	}
	if log.MatchTerm(1, 2) {
		t.Error("Term at index 1 should not match 2")
	}
	if log.MatchTerm(0, 0) == false {
		t.Error("Term at index 0 should match 0")
	}
}

func TestRaftNode_Quorum(t *testing.T) {
	config := DefaultRaftConfig("node1")
	node := NewRaftNode(config)

	// Single node cluster
	if node.QuorumSize() != 1 {
		t.Errorf("Expected quorum size 1, got %d", node.QuorumSize())
	}

	// Add peers
	node.AddPeer("node2", "localhost:8002")
	node.AddPeer("node3", "localhost:8003")

	// 3 node cluster
	if node.QuorumSize() != 2 {
		t.Errorf("Expected quorum size 2 for 3-node cluster, got %d", node.QuorumSize())
	}

	// Add more peers
	node.AddPeer("node4", "localhost:8004")
	node.AddPeer("node5", "localhost:8005")

	// 5 node cluster
	if node.QuorumSize() != 3 {
		t.Errorf("Expected quorum size 3 for 5-node cluster, got %d", node.QuorumSize())
	}
}

func TestRaftNode_StateTransitions(t *testing.T) {
	config := DefaultRaftConfig("node1")
	node := NewRaftNode(config)

	// Initial state should be follower
	if node.GetState() != StateFollower {
		t.Errorf("Expected initial state Follower, got %s", node.GetState())
	}

	// Term should be 0
	if node.GetTerm() != 0 {
		t.Errorf("Expected initial term 0, got %d", node.GetTerm())
	}
}

func TestRequestVote_Handler(t *testing.T) {
	config := DefaultRaftConfig("node1")
	node := NewRaftNode(config)

	// Request vote from node2 with higher term
	args := &RequestVoteArgs{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	reply := node.HandleRequestVote(args)

	// Should grant vote (term is higher, log is up to date)
	if !reply.VoteGranted {
		t.Error("Expected vote to be granted")
	}
	if reply.Term != 1 {
		t.Errorf("Expected term 1, got %d", reply.Term)
	}
	if node.GetTerm() != 1 {
		t.Errorf("Node term should be updated to 1, got %d", node.GetTerm())
	}

	// Request vote from node3 with same term (should deny - already voted)
	args2 := &RequestVoteArgs{
		Term:         1,
		CandidateID:  "node3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	reply2 := node.HandleRequestVote(args2)

	if reply2.VoteGranted {
		t.Error("Expected vote to be denied (already voted)")
	}

	// Request vote with lower term should be denied
	args3 := &RequestVoteArgs{
		Term:         0,
		CandidateID:  "node4",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	reply3 := node.HandleRequestVote(args3)

	if reply3.VoteGranted {
		t.Error("Expected vote to be denied (lower term)")
	}
}

func TestAppendEntries_Handler(t *testing.T) {
	config := DefaultRaftConfig("node1")
	node := NewRaftNode(config)

	// Heartbeat from leader
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	reply := node.HandleAppendEntries(args)

	if !reply.Success {
		t.Error("Expected heartbeat to succeed")
	}
	if node.GetLeader() != "leader1" {
		t.Errorf("Expected leader to be 'leader1', got '%s'", node.GetLeader())
	}

	// Append entries
	entries := []LogEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
	}

	args2 := &AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 1,
	}

	reply2 := node.HandleAppendEntries(args2)

	if !reply2.Success {
		t.Error("Expected append entries to succeed")
	}
	if node.GetCommitIndex() != 1 {
		t.Errorf("Expected commit index 1, got %d", node.GetCommitIndex())
	}

	// Lower term should fail
	args3 := &AppendEntriesArgs{
		Term:         0,
		LeaderID:     "old_leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	reply3 := node.HandleAppendEntries(args3)

	if reply3.Success {
		t.Error("Expected append entries with lower term to fail")
	}
}

func TestPartitionDetector(t *testing.T) {
	pd := NewPartitionDetector("node1", 500*time.Millisecond)
	pd.SetPeers([]string{"node2", "node3", "node4"})

	// Initially should have quorum
	if !pd.HasQuorum() {
		t.Error("Expected to have quorum initially")
	}

	// Record failures for majority
	pd.RecordFailure("node2")
	pd.RecordFailure("node3")
	pd.RecordFailure("node4")

	// Should lose quorum
	if pd.HasQuorum() {
		t.Error("Expected to lose quorum after majority failures")
	}

	// Record contact from one peer
	pd.RecordContact("node2")
	pd.RecordContact("node3")

	// Should regain quorum (self + node2 + node3 = 3/5 = majority)
	if !pd.HasQuorum() {
		t.Error("Expected to regain quorum")
	}
}

func TestCommand_EncodeDecode(t *testing.T) {
	cmd := NewAcquireCommand("req1", "lock1", "owner1", 30*time.Second)

	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.Type != CmdAcquire {
		t.Errorf("Expected type Acquire, got %v", decoded.Type)
	}
	if decoded.Key != "lock1" {
		t.Errorf("Expected key 'lock1', got '%s'", decoded.Key)
	}
	if decoded.Owner != "owner1" {
		t.Errorf("Expected owner 'owner1', got '%s'", decoded.Owner)
	}
	if decoded.TTL != 30*time.Second {
		t.Errorf("Expected TTL 30s, got %v", decoded.TTL)
	}
}

// MockTransport for testing
type MockTransport struct {
	mu            sync.Mutex
	votes         map[string]*RequestVoteReply
	appendReplies map[string]*AppendEntriesReply
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		votes:         make(map[string]*RequestVoteReply),
		appendReplies: make(map[string]*AppendEntriesReply),
	}
}

func (m *MockTransport) SendRequestVote(peer PeerInfo, args *RequestVoteArgs) (*RequestVoteReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reply, ok := m.votes[peer.ID]; ok {
		return reply, nil
	}
	return &RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (m *MockTransport) SendAppendEntries(peer PeerInfo, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reply, ok := m.appendReplies[peer.ID]; ok {
		return reply, nil
	}
	return &AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func (m *MockTransport) SendInstallSnapshot(peer PeerInfo, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return &InstallSnapshotReply{Term: args.Term}, nil
}

func TestRaftNode_Bootstrap(t *testing.T) {
	config := DefaultRaftConfig("node1")
	node := NewRaftNode(config)

	err := node.Bootstrap()
	if err != nil {
		t.Fatalf("Bootstrap failed: %v", err)
	}

	if !node.IsLeader() {
		t.Error("Expected node to be leader after bootstrap")
	}
	if node.GetTerm() != 1 {
		t.Errorf("Expected term 1 after bootstrap, got %d", node.GetTerm())
	}
	if node.GetLeader() != "node1" {
		t.Errorf("Expected leader 'node1', got '%s'", node.GetLeader())
	}
}

func TestRaftNode_BootstrapWithPeers(t *testing.T) {
	config := DefaultRaftConfig("node1")
	node := NewRaftNode(config)
	node.AddPeer("node2", "localhost:8002")

	err := node.Bootstrap()
	if err == nil {
		t.Error("Expected bootstrap to fail with peers")
	}
}
