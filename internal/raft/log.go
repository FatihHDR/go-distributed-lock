package raft

import (
	"sync"
)

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Index   uint64 // Log position (1-indexed)
	Term    uint64 // Term when entry was received
	Command []byte // Serialized command
}

// RaftLog manages the replicated log
type RaftLog struct {
	mu         sync.RWMutex
	entries    []LogEntry
	startIndex uint64 // First index in entries (for compaction)
	startTerm  uint64 // Term at startIndex
}

// NewRaftLog creates a new empty log
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries:    make([]LogEntry, 0),
		startIndex: 0,
		startTerm:  0,
	}
}

// Append adds entries to the log
func (l *RaftLog) Append(entries ...LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// AppendNew creates and appends a new entry
func (l *RaftLog) AppendNew(term uint64, command []byte) LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := LogEntry{
		Index:   l.lastIndexLocked() + 1,
		Term:    term,
		Command: command,
	}
	l.entries = append(l.entries, entry)
	return entry
}

// Get returns the entry at the given index
func (l *RaftLog) Get(index uint64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getLocked(index)
}

func (l *RaftLog) getLocked(index uint64) (LogEntry, bool) {
	if index <= l.startIndex || index > l.lastIndexLocked() {
		return LogEntry{}, false
	}
	offset := index - l.startIndex - 1
	if int(offset) >= len(l.entries) {
		return LogEntry{}, false
	}
	return l.entries[offset], true
}

// GetRange returns entries from startIdx to endIdx (inclusive)
func (l *RaftLog) GetRange(startIdx, endIdx uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if startIdx > endIdx || startIdx <= l.startIndex {
		return nil
	}

	startOffset := startIdx - l.startIndex - 1
	endOffset := endIdx - l.startIndex

	if int(startOffset) >= len(l.entries) {
		return nil
	}
	if int(endOffset) > len(l.entries) {
		endOffset = uint64(len(l.entries)) + l.startIndex
	}

	result := make([]LogEntry, endOffset-startOffset)
	copy(result, l.entries[startOffset:endOffset])
	return result
}

// GetFrom returns all entries from startIdx onwards
func (l *RaftLog) GetFrom(startIdx uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if startIdx <= l.startIndex {
		startIdx = l.startIndex + 1
	}

	if startIdx > l.lastIndexLocked() {
		return nil
	}

	offset := startIdx - l.startIndex - 1
	result := make([]LogEntry, len(l.entries)-int(offset))
	copy(result, l.entries[offset:])
	return result
}

// LastIndex returns the index of the last entry
func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

func (l *RaftLog) lastIndexLocked() uint64 {
	if len(l.entries) == 0 {
		return l.startIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry
func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastTermLocked()
}

func (l *RaftLog) lastTermLocked() uint64 {
	if len(l.entries) == 0 {
		return l.startTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// Term returns the term at the given index
func (l *RaftLog) Term(index uint64) (uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		return 0, true
	}
	if index == l.startIndex {
		return l.startTerm, true
	}
	if index < l.startIndex || index > l.lastIndexLocked() {
		return 0, false
	}

	entry, ok := l.getLocked(index)
	if !ok {
		return 0, false
	}
	return entry.Term, true
}

// TruncateAfter removes all entries after the given index
func (l *RaftLog) TruncateAfter(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < l.startIndex {
		l.entries = nil
		return
	}

	offset := index - l.startIndex
	if int(offset) < len(l.entries) {
		l.entries = l.entries[:offset]
	}
}

// TruncateBefore removes all entries before the given index (compaction)
func (l *RaftLog) TruncateBefore(index uint64, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.startIndex {
		return
	}

	offset := index - l.startIndex
	if int(offset) >= len(l.entries) {
		l.entries = nil
	} else {
		l.entries = l.entries[offset:]
	}
	l.startIndex = index
	l.startTerm = term
}

// Size returns the number of entries
func (l *RaftLog) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// StartIndex returns the first index (for compaction)
func (l *RaftLog) StartIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.startIndex
}

// MatchTerm checks if the entry at index has the expected term
func (l *RaftLog) MatchTerm(index, term uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		return term == 0
	}
	if index == l.startIndex {
		return term == l.startTerm
	}

	entryTerm, ok := l.Term(index)
	return ok && entryTerm == term
}

// FindConflict finds the first entry that conflicts with given entries
// Returns the index of the conflict, or 0 if no conflict
func (l *RaftLog) FindConflict(entries []LogEntry) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, entry := range entries {
		if existing, ok := l.getLocked(entry.Index); ok {
			if existing.Term != entry.Term {
				return entry.Index
			}
		}
	}
	return 0
}

// AppendAfter appends entries after a given index, truncating any conflicts
func (l *RaftLog) AppendAfter(prevIndex uint64, entries []LogEntry) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(entries) == 0 {
		return true
	}

	// Find where new entries should start
	first := entries[0].Index
	if first <= l.startIndex {
		// All entries are before our start, skip to what we need
		offset := l.startIndex - first + 1
		if int(offset) >= len(entries) {
			return true
		}
		entries = entries[offset:]
		first = entries[0].Index
	}

	// Check for conflicts and truncate
	for i, entry := range entries {
		if existing, ok := l.getLocked(entry.Index); ok {
			if existing.Term != entry.Term {
				// Conflict! Truncate and append rest
				l.entries = l.entries[:entry.Index-l.startIndex-1]
				l.entries = append(l.entries, entries[i:]...)
				return true
			}
		} else {
			// No existing entry, append from here
			l.entries = append(l.entries, entries[i:]...)
			return true
		}
	}

	return true
}

// All returns all entries
func (l *RaftLog) All() []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	result := make([]LogEntry, len(l.entries))
	copy(result, l.entries)
	return result
}

// IsUpToDate checks if candidate's log is at least as up-to-date as ours
func (l *RaftLog) IsUpToDate(lastIndex, lastTerm uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	ourLastTerm := l.lastTermLocked()
	ourLastIndex := l.lastIndexLocked()

	// §5.4.1: Compare terms first, then index
	if lastTerm != ourLastTerm {
		return lastTerm > ourLastTerm
	}
	return lastIndex >= ourLastIndex
}
