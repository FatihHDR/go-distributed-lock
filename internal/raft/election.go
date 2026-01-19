package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// electionLoop handles election timeout and triggers elections
func (rn *RaftNode) electionLoop() {
	defer rn.wg.Done()

	for {
		timeout := rn.randomElectionTimeout()
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:
			rn.mu.Lock()
			// Only start election if we're a follower and haven't heard from leader
			if rn.state == StateFollower && time.Since(rn.lastHeartbeat) >= rn.config.ElectionTimeout {
				rn.startElection()
			} else if rn.state == StateLeader {
				// Leader sends heartbeats
				rn.broadcastHeartbeat()
			}
			rn.mu.Unlock()

		case <-rn.stopCh:
			timer.Stop()
			return
		}
	}
}

// randomElectionTimeout returns election timeout with randomization
func (rn *RaftNode) randomElectionTimeout() time.Duration {
	// Randomize between ElectionTimeout and 2*ElectionTimeout
	jitter := time.Duration(rand.Int63n(int64(rn.config.ElectionTimeout)))
	return rn.config.ElectionTimeout + jitter
}

// startElection initiates a new leader election
func (rn *RaftNode) startElection() {
	rn.becomeCandidate()

	// Prepare RequestVote arguments
	args := &RequestVoteArgs{
		Term:         rn.currentTerm,
		CandidateID:  rn.config.NodeID,
		LastLogIndex: rn.raftLog.LastIndex(),
		LastLogTerm:  rn.raftLog.LastTerm(),
	}

	// Count votes (start with self vote)
	votesReceived := 1
	votesNeeded := rn.quorumSizeLocked()
	term := rn.currentTerm

	// Check if we already have majority (single node cluster)
	if votesReceived >= votesNeeded {
		rn.becomeLeader()
		return
	}

	// Request votes from all peers
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range rn.peers {
		wg.Add(1)
		go func(p PeerInfo) {
			defer wg.Done()
			rn.requestVote(p, args, &voteMu, &votesReceived, term, votesNeeded)
		}(peer)
	}

	// Wait for all vote requests to complete (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Release lock while waiting for votes
	rn.mu.Unlock()

	select {
	case <-done:
	case <-time.After(rn.config.ElectionTimeout):
	case <-rn.stopCh:
	}

	rn.mu.Lock()
}

// requestVote sends a RequestVote RPC to a peer
func (rn *RaftNode) requestVote(peer PeerInfo, args *RequestVoteArgs, voteMu *sync.Mutex, votes *int, term uint64, needed int) {
	if rn.transport == nil {
		return
	}

	reply, err := rn.transport.SendRequestVote(peer, args)
	if err != nil {
		log.Printf("[Raft %s] Failed to request vote from %s: %v", rn.config.NodeID, peer.ID, err)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Ignore if we're no longer a candidate or term changed
	if rn.state != StateCandidate || rn.currentTerm != term {
		return
	}

	// If reply contains higher term, step down
	if reply.Term > rn.currentTerm {
		rn.becomeFollower(reply.Term)
		return
	}

	// Count the vote
	if reply.VoteGranted {
		voteMu.Lock()
		*votes++
		currentVotes := *votes
		voteMu.Unlock()

		log.Printf("[Raft %s] Received vote from %s (total: %d, needed: %d)",
			rn.config.NodeID, peer.ID, currentVotes, needed)

		// Check if we have majority
		if currentVotes >= needed && rn.state == StateCandidate {
			rn.becomeLeader()
		}
	}
}

// broadcastHeartbeat sends AppendEntries to all peers
func (rn *RaftNode) broadcastHeartbeat() {
	if rn.state != StateLeader {
		return
	}

	for _, peer := range rn.peers {
		go rn.sendAppendEntries(peer)
	}
}

// sendAppendEntries sends an AppendEntries RPC to a peer
func (rn *RaftNode) sendAppendEntries(peer PeerInfo) {
	rn.mu.RLock()
	if rn.state != StateLeader || rn.transport == nil {
		rn.mu.RUnlock()
		return
	}

	nextIdx := rn.nextIndex[peer.ID]
	prevLogIndex := nextIdx - 1
	prevLogTerm, _ := rn.raftLog.Term(prevLogIndex)

	// Get entries to send
	var entries []LogEntry
	if nextIdx <= rn.raftLog.LastIndex() {
		entries = rn.raftLog.GetFrom(nextIdx)
		// Limit entries per RPC
		if len(entries) > rn.config.MaxLogEntries {
			entries = entries[:rn.config.MaxLogEntries]
		}
	}

	args := &AppendEntriesArgs{
		Term:         rn.currentTerm,
		LeaderID:     rn.config.NodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
	}
	term := rn.currentTerm
	rn.mu.RUnlock()

	reply, err := rn.transport.SendAppendEntries(peer, args)
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Ignore stale responses
	if rn.state != StateLeader || rn.currentTerm != term {
		return
	}

	if reply.Term > rn.currentTerm {
		rn.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		// Update nextIndex and matchIndex
		if len(entries) > 0 {
			newMatchIndex := entries[len(entries)-1].Index
			if newMatchIndex > rn.matchIndex[peer.ID] {
				rn.matchIndex[peer.ID] = newMatchIndex
				rn.nextIndex[peer.ID] = newMatchIndex + 1
			}
		}
		// Try to advance commit index
		rn.advanceCommitIndex()
	} else {
		// Decrement nextIndex and retry
		if reply.ConflictTerm > 0 {
			// Fast backup: find last entry with ConflictTerm
			found := false
			for i := rn.raftLog.LastIndex(); i > rn.raftLog.StartIndex(); i-- {
				t, ok := rn.raftLog.Term(i)
				if ok && t == reply.ConflictTerm {
					rn.nextIndex[peer.ID] = i + 1
					found = true
					break
				}
			}
			if !found {
				rn.nextIndex[peer.ID] = reply.ConflictIndex
			}
		} else {
			rn.nextIndex[peer.ID] = reply.ConflictIndex
		}
		if rn.nextIndex[peer.ID] < 1 {
			rn.nextIndex[peer.ID] = 1
		}
	}
}

// advanceCommitIndex tries to advance the commit index based on matchIndex
func (rn *RaftNode) advanceCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N

	lastIndex := rn.raftLog.LastIndex()
	for n := lastIndex; n > rn.commitIndex; n-- {
		term, ok := rn.raftLog.Term(n)
		if !ok || term != rn.currentTerm {
			continue
		}

		// Count how many have this index replicated
		count := 1 // Self
		for _, matchIdx := range rn.matchIndex {
			if matchIdx >= n {
				count++
			}
		}

		if count >= rn.quorumSizeLocked() {
			rn.commitIndex = n
			// Signal that new entries are committed
			select {
			case rn.commitCh <- struct{}{}:
			default:
			}
			break
		}
	}
}

// quorumSizeLocked returns the quorum size (must hold lock)
func (rn *RaftNode) quorumSizeLocked() int {
	return (len(rn.peers)+1)/2 + 1
}

// applyLoop applies committed entries to the state machine
func (rn *RaftNode) applyLoop() {
	defer rn.wg.Done()

	for {
		select {
		case <-rn.stopCh:
			return
		case entry := <-rn.applyCh:
			rn.applyEntry(entry)
		}
	}
}

// commitLoop monitors commits and triggers applies
func (rn *RaftNode) commitLoop() {
	defer rn.wg.Done()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.commitCh:
			rn.applyCommitted()
		}
	}
}

// applyCommitted applies all committed but not yet applied entries
func (rn *RaftNode) applyCommitted() {
	rn.mu.Lock()
	commitIndex := rn.commitIndex
	lastApplied := rn.lastApplied
	rn.mu.Unlock()

	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry, ok := rn.raftLog.Get(i)
		if !ok {
			continue
		}

		// Apply to state machine
		var result interface{}
		if rn.stateMachine != nil {
			result = rn.stateMachine.Apply(entry)
		}

		rn.mu.Lock()
		rn.lastApplied = i

		// Notify any waiting futures
		if future, exists := rn.applyQueue[i]; exists {
			future.resultCh <- ApplyResult{
				Index: i,
				Term:  entry.Term,
				Data:  result,
			}
			delete(rn.applyQueue, i)
		}
		rn.mu.Unlock()
	}
}

// applyEntry applies a single entry
func (rn *RaftNode) applyEntry(entry LogEntry) {
	if rn.stateMachine != nil {
		rn.stateMachine.Apply(entry)
	}
}
