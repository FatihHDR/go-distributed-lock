package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents log level
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Fields is a map of structured log fields
type Fields map[string]interface{}

// Entry represents a structured log entry
type Entry struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	NodeID    string                 `json:"node_id,omitempty"`
	Component string                 `json:"component,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// Logger provides structured logging
type Logger struct {
	mu        sync.Mutex
	output    io.Writer
	level     Level
	nodeID    string
	component string
	fields    Fields
}

// NewLogger creates a new structured logger
func NewLogger(nodeID string) *Logger {
	return &Logger{
		output: os.Stdout,
		level:  LevelInfo,
		nodeID: nodeID,
		fields: make(Fields),
	}
}

// SetOutput sets the output writer
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
}

// SetLevel sets the minimum log level
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// WithComponent creates a logger for a specific component
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{
		output:    l.output,
		level:     l.level,
		nodeID:    l.nodeID,
		component: component,
		fields:    copyFields(l.fields),
	}
}

// WithFields creates a logger with additional fields
func (l *Logger) WithFields(fields Fields) *Logger {
	newFields := copyFields(l.fields)
	for k, v := range fields {
		newFields[k] = v
	}
	return &Logger{
		output:    l.output,
		level:     l.level,
		nodeID:    l.nodeID,
		component: l.component,
		fields:    newFields,
	}
}

func copyFields(src Fields) Fields {
	dst := make(Fields)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// log writes a log entry
func (l *Logger) log(level Level, msg string, fields Fields) {
	if level < l.level {
		return
	}

	entry := Entry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level.String(),
		Message:   msg,
		NodeID:    l.nodeID,
		Component: l.component,
	}

	// Merge fields
	if len(l.fields) > 0 || len(fields) > 0 {
		entry.Fields = make(map[string]interface{})
		for k, v := range l.fields {
			entry.Fields[k] = v
		}
		for k, v := range fields {
			entry.Fields[k] = v
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(l.output, "ERROR marshaling log: %v\n", err)
		return
	}
	fmt.Fprintln(l.output, string(data))
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(LevelDebug, msg, f)
}

// Info logs an info message
func (l *Logger) Info(msg string, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(LevelInfo, msg, f)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(LevelWarn, msg, f)
}

// Error logs an error message
func (l *Logger) Error(msg string, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(LevelError, msg, f)
}

// LogLockAcquire logs a lock acquisition event
func (l *Logger) LogLockAcquire(key, owner string, success bool, err error, duration time.Duration) {
	fields := Fields{
		"key":         key,
		"owner":       owner,
		"success":     success,
		"duration_ms": duration.Milliseconds(),
	}
	if err != nil {
		fields["error"] = err.Error()
	}

	if success {
		l.Info("Lock acquired", fields)
	} else {
		l.Warn("Lock acquisition failed", fields)
	}
}

// LogLockRenew logs a lock renewal event
func (l *Logger) LogLockRenew(key, owner string, success bool, err error, duration time.Duration) {
	fields := Fields{
		"key":         key,
		"owner":       owner,
		"success":     success,
		"duration_ms": duration.Milliseconds(),
	}
	if err != nil {
		fields["error"] = err.Error()
	}

	if success {
		l.Debug("Lock renewed", fields)
	} else {
		l.Warn("Lock renewal failed", fields)
	}
}

// LogLockRelease logs a lock release event
func (l *Logger) LogLockRelease(key, owner string, success bool, err error) {
	fields := Fields{
		"key":     key,
		"owner":   owner,
		"success": success,
	}
	if err != nil {
		fields["error"] = err.Error()
	}

	if success {
		l.Info("Lock released", fields)
	} else {
		l.Warn("Lock release failed", fields)
	}
}

// LogLockExpired logs a lock expiration event
func (l *Logger) LogLockExpired(key, owner string) {
	l.Info("Lock expired", Fields{
		"key":   key,
		"owner": owner,
	})
}

// LogLeaderChange logs a leader change event
func (l *Logger) LogLeaderChange(oldLeader, newLeader string) {
	l.Info("Leader changed", Fields{
		"old_leader": oldLeader,
		"new_leader": newLeader,
	})
}

// LogElectionStarted logs an election start event
func (l *Logger) LogElectionStarted(term uint64) {
	l.Info("Election started", Fields{
		"term": term,
	})
}

// LogHeartbeat logs a heartbeat event
func (l *Logger) LogHeartbeat(leaderID string, term uint64, sent bool) {
	action := "received"
	if sent {
		action = "sent"
	}
	l.Debug("Heartbeat "+action, Fields{
		"leader_id": leaderID,
		"term":      term,
	})
}

// LogRetry logs a retry event
func (l *Logger) LogRetry(operation string, attempt int, maxAttempts int, err error, delay time.Duration) {
	l.Warn("Retrying operation", Fields{
		"operation":    operation,
		"attempt":      attempt,
		"max_attempts": maxAttempts,
		"error":        err.Error(),
		"delay_ms":     delay.Milliseconds(),
	})
}

// LogCircuitBreakerStateChange logs a circuit breaker state change
func (l *Logger) LogCircuitBreakerStateChange(oldState, newState string, failures int) {
	l.Warn("Circuit breaker state changed", Fields{
		"old_state": oldState,
		"new_state": newState,
		"failures":  failures,
	})
}

// LogForwardRequest logs request forwarding
func (l *Logger) LogForwardRequest(requestType, leaderID string, success bool, err error) {
	fields := Fields{
		"request_type": requestType,
		"leader_id":    leaderID,
		"success":      success,
	}
	if err != nil {
		fields["error"] = err.Error()
	}

	if success {
		l.Debug("Request forwarded", fields)
	} else {
		l.Error("Request forward failed", fields)
	}
}
