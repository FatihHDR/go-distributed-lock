package raft

// RequestVoteArgs contains arguments for RequestVote RPC
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`           // Candidate's term
	CandidateID  string `json:"candidate_id"`   // Candidate requesting vote
	LastLogIndex uint64 `json:"last_log_index"` // Index of candidate's last log entry
	LastLogTerm  uint64 `json:"last_log_term"`  // Term of candidate's last log entry
}

// RequestVoteReply contains response for RequestVote RPC
type RequestVoteReply struct {
	Term        uint64 `json:"term"`         // currentTerm, for candidate to update itself
	VoteGranted bool   `json:"vote_granted"` // True means candidate received vote
}

// AppendEntriesArgs contains arguments for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`           // Leader's term
	LeaderID     string     `json:"leader_id"`      // So follower can redirect clients
	PrevLogIndex uint64     `json:"prev_log_index"` // Index of log entry preceding new ones
	PrevLogTerm  uint64     `json:"prev_log_term"`  // Term of prevLogIndex entry
	Entries      []LogEntry `json:"entries"`        // Log entries to store (empty for heartbeat)
	LeaderCommit uint64     `json:"leader_commit"`  // Leader's commitIndex
}

// AppendEntriesReply contains response for AppendEntries RPC
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`           // currentTerm, for leader to update itself
	Success       bool   `json:"success"`        // True if follower contained matching entry
	ConflictIndex uint64 `json:"conflict_index"` // First index where conflict occurred
	ConflictTerm  uint64 `json:"conflict_term"`  // Term at conflicting entry
}

// InstallSnapshotArgs contains arguments for InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              uint64 `json:"term"`                // Leader's term
	LeaderID          string `json:"leader_id"`           // So follower can redirect clients
	LastIncludedIndex uint64 `json:"last_included_index"` // Index of last entry in snapshot
	LastIncludedTerm  uint64 `json:"last_included_term"`  // Term of last entry in snapshot
	Offset            int64  `json:"offset"`              // Byte offset where chunk is positioned
	Data              []byte `json:"data"`                // Raw bytes of the snapshot chunk
	Done              bool   `json:"done"`                // True if this is the last chunk
}

// InstallSnapshotReply contains response for InstallSnapshot RPC
type InstallSnapshotReply struct {
	Term uint64 `json:"term"` // currentTerm, for leader to update itself
}

// HandleRequestVote processes a RequestVote RPC
func (rn *RaftNode) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &RequestVoteReply{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rn.currentTerm {
		return reply
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T,
	// convert to follower (§5.1)
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
		reply.Term = rn.currentTerm
	}

	// If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	if (rn.votedFor == "" || rn.votedFor == args.CandidateID) &&
		rn.raftLog.IsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rn.votedFor = args.CandidateID
		reply.VoteGranted = true
		rn.lastHeartbeat = rn.now() // Reset election timer
		rn.persistState()
	}

	return reply
}

// HandleAppendEntries processes an AppendEntries RPC
func (rn *RaftNode) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:    rn.currentTerm,
		Success: false,
	}

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rn.currentTerm {
		return reply
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T,
	// convert to follower (§5.1)
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
		reply.Term = rn.currentTerm
	}

	// Recognize the leader
	if rn.state != StateFollower {
		rn.becomeFollower(args.Term)
	}
	rn.leaderID = args.LeaderID
	rn.lastHeartbeat = rn.now()

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > 0 {
		prevTerm, ok := rn.raftLog.Term(args.PrevLogIndex)
		if !ok {
			// Log too short
			reply.ConflictIndex = rn.raftLog.LastIndex() + 1
			reply.ConflictTerm = 0
			return reply
		}
		if prevTerm != args.PrevLogTerm {
			// Term mismatch
			reply.ConflictTerm = prevTerm
			// Find first index with this term
			reply.ConflictIndex = args.PrevLogIndex
			for i := args.PrevLogIndex - 1; i > rn.raftLog.StartIndex(); i-- {
				t, ok := rn.raftLog.Term(i)
				if !ok || t != prevTerm {
					break
				}
				reply.ConflictIndex = i
			}
			return reply
		}
	}

	// If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	if len(args.Entries) > 0 {
		rn.raftLog.AppendAfter(args.PrevLogIndex, args.Entries)
		if rn.persistence != nil {
			rn.persistence.SaveLog(args.Entries)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rn.commitIndex {
		lastNew := rn.raftLog.LastIndex()
		if args.LeaderCommit < lastNew {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNew
		}
		// Signal commit
		select {
		case rn.commitCh <- struct{}{}:
		default:
		}
	}

	reply.Success = true
	return reply
}

// HandleInstallSnapshot processes an InstallSnapshot RPC
func (rn *RaftNode) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &InstallSnapshotReply{
		Term: rn.currentTerm,
	}

	// Reply immediately if term < currentTerm
	if args.Term < rn.currentTerm {
		return reply
	}

	// If RPC request contains term T > currentTerm
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
		reply.Term = rn.currentTerm
	}

	rn.leaderID = args.LeaderID
	rn.lastHeartbeat = rn.now()

	// If snapshot is done, apply it
	if args.Done {
		snapshot := &Snapshot{
			LastIndex: args.LastIncludedIndex,
			LastTerm:  args.LastIncludedTerm,
			Data:      args.Data,
		}

		// Apply snapshot to state machine
		if rn.stateMachine != nil {
			rn.stateMachine.Restore(args.Data)
		}

		// Discard log entries covered by snapshot
		rn.raftLog.TruncateBefore(args.LastIncludedIndex, args.LastIncludedTerm)

		// Update indices
		if args.LastIncludedIndex > rn.commitIndex {
			rn.commitIndex = args.LastIncludedIndex
		}
		if args.LastIncludedIndex > rn.lastApplied {
			rn.lastApplied = args.LastIncludedIndex
		}

		// Persist snapshot
		if rn.persistence != nil {
			rn.persistence.SaveSnapshot(snapshot)
		}
	}

	return reply
}

// now returns current time (allows mocking in tests)
func (rn *RaftNode) now() interface{} {
	return nil // Will be replaced with time.Now() in actual implementation
}
