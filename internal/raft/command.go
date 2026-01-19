package raft

import (
	"encoding/json"
	"time"
)

// CommandType defines the type of lock operation
type CommandType uint8

const (
	CmdNoop    CommandType = iota // No-op for leader election
	CmdAcquire                    // Acquire a lock
	CmdRenew                      // Renew a lock TTL
	CmdRelease                    // Release a lock
)

func (c CommandType) String() string {
	switch c {
	case CmdNoop:
		return "noop"
	case CmdAcquire:
		return "acquire"
	case CmdRenew:
		return "renew"
	case CmdRelease:
		return "release"
	default:
		return "unknown"
	}
}

// Command represents a lock operation to be replicated
type Command struct {
	Type      CommandType   `json:"type"`
	RequestID string        `json:"request_id"` // For deduplication
	Key       string        `json:"key"`
	Owner     string        `json:"owner"`
	TTL       time.Duration `json:"ttl,omitempty"`
	Timestamp time.Time     `json:"timestamp"`
}

// NewAcquireCommand creates an acquire lock command
func NewAcquireCommand(requestID, key, owner string, ttl time.Duration) *Command {
	return &Command{
		Type:      CmdAcquire,
		RequestID: requestID,
		Key:       key,
		Owner:     owner,
		TTL:       ttl,
		Timestamp: time.Now(),
	}
}

// NewRenewCommand creates a renew lock command
func NewRenewCommand(requestID, key, owner string, ttl time.Duration) *Command {
	return &Command{
		Type:      CmdRenew,
		RequestID: requestID,
		Key:       key,
		Owner:     owner,
		TTL:       ttl,
		Timestamp: time.Now(),
	}
}

// NewReleaseCommand creates a release lock command
func NewReleaseCommand(requestID, key, owner string) *Command {
	return &Command{
		Type:      CmdRelease,
		RequestID: requestID,
		Key:       key,
		Owner:     owner,
		Timestamp: time.Now(),
	}
}

// NewNoopCommand creates a no-op command (used for leader assertion)
func NewNoopCommand() *Command {
	return &Command{
		Type:      CmdNoop,
		Timestamp: time.Now(),
	}
}

// Encode serializes the command to bytes
func (c *Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand deserializes a command from bytes
func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

// CommandResult represents the result of applying a command
type CommandResult struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// CommandTracker tracks pending commands for deduplication
type CommandTracker struct {
	pending map[string]*pendingCommand
	maxAge  time.Duration
}

type pendingCommand struct {
	result    *CommandResult
	createdAt time.Time
}

// NewCommandTracker creates a new command tracker
func NewCommandTracker(maxAge time.Duration) *CommandTracker {
	return &CommandTracker{
		pending: make(map[string]*pendingCommand),
		maxAge:  maxAge,
	}
}

// Track records a command result
func (ct *CommandTracker) Track(requestID string, result *CommandResult) {
	ct.pending[requestID] = &pendingCommand{
		result:    result,
		createdAt: time.Now(),
	}
}

// Get retrieves a tracked command result
func (ct *CommandTracker) Get(requestID string) (*CommandResult, bool) {
	if pc, exists := ct.pending[requestID]; exists {
		if time.Since(pc.createdAt) < ct.maxAge {
			return pc.result, true
		}
		delete(ct.pending, requestID)
	}
	return nil, false
}

// Cleanup removes expired entries
func (ct *CommandTracker) Cleanup() {
	now := time.Now()
	for id, pc := range ct.pending {
		if now.Sub(pc.createdAt) >= ct.maxAge {
			delete(ct.pending, id)
		}
	}
}
