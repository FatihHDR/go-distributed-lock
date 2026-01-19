package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrWALClosed    = errors.New("WAL is closed")
	ErrCorruptEntry = errors.New("corrupt WAL entry")
)

// WALEntryType defines the type of WAL entry
type WALEntryType string

const (
	WALAcquire WALEntryType = "acquire"
	WALRenew   WALEntryType = "renew"
	WALRelease WALEntryType = "release"
	WALExpire  WALEntryType = "expire"
)

// WALEntry represents a single entry in the write-ahead log
type WALEntry struct {
	Sequence  uint64       `json:"seq"`
	Timestamp time.Time    `json:"ts"`
	Type      WALEntryType `json:"type"`
	Key       string       `json:"key"`
	Owner     string       `json:"owner,omitempty"`
	TTL       int64        `json:"ttl,omitempty"` // nanoseconds
	ExpiresAt time.Time    `json:"expires_at,omitempty"`
}

// WALConfig configures the write-ahead log
type WALConfig struct {
	Dir           string
	MaxFileSize   int64         // Max size before rotation
	SyncOnWrite   bool          // fsync after each write
	FlushInterval time.Duration // Periodic flush interval
}

// DefaultWALConfig returns sensible defaults
func DefaultWALConfig(dir string) *WALConfig {
	return &WALConfig{
		Dir:           dir,
		MaxFileSize:   64 * 1024 * 1024, // 64MB
		SyncOnWrite:   true,
		FlushInterval: 100 * time.Millisecond,
	}
}

// WAL implements a write-ahead log for durability
type WAL struct {
	config   *WALConfig
	file     *os.File
	writer   *bufio.Writer
	sequence uint64
	mu       sync.Mutex
	closed   bool
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewWAL creates a new write-ahead log
func NewWAL(config *WALConfig) (*WAL, error) {
	// Ensure directory exists
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WAL{
		config: config,
		stopCh: make(chan struct{}),
	}

	return wal, nil
}

// Open opens the WAL for writing
func (w *WAL) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return nil // Already open
	}

	// Create or open the current WAL file
	filename := filepath.Join(w.config.Dir, "wal.log")
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	w.closed = false

	// Start periodic flush
	if w.config.FlushInterval > 0 {
		w.wg.Add(1)
		go w.flushLoop()
	}

	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	close(w.stopCh)

	w.wg.Wait()

	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// flushLoop periodically flushes the WAL buffer
func (w *WAL) flushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.Flush()
		case <-w.stopCh:
			return
		}
	}
}

// Append appends an entry to the WAL
func (w *WAL) Append(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	w.sequence++
	entry.Sequence = w.sequence
	entry.Timestamp = time.Now()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	// Write entry followed by newline
	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}
	if _, err := w.writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	if w.config.SyncOnWrite {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}
	}

	return nil
}

// Flush flushes buffered writes to disk
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.writer == nil {
		return nil
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// LogAcquire logs a lock acquisition
func (w *WAL) LogAcquire(key, owner string, ttl time.Duration, expiresAt time.Time) error {
	return w.Append(&WALEntry{
		Type:      WALAcquire,
		Key:       key,
		Owner:     owner,
		TTL:       int64(ttl),
		ExpiresAt: expiresAt,
	})
}

// LogRenew logs a lock renewal
func (w *WAL) LogRenew(key, owner string, ttl time.Duration, expiresAt time.Time) error {
	return w.Append(&WALEntry{
		Type:      WALRenew,
		Key:       key,
		Owner:     owner,
		TTL:       int64(ttl),
		ExpiresAt: expiresAt,
	})
}

// LogRelease logs a lock release
func (w *WAL) LogRelease(key, owner string) error {
	return w.Append(&WALEntry{
		Type:  WALRelease,
		Key:   key,
		Owner: owner,
	})
}

// LogExpire logs a lock expiration
func (w *WAL) LogExpire(key, owner string) error {
	return w.Append(&WALEntry{
		Type:  WALExpire,
		Key:   key,
		Owner: owner,
	})
}

// ReadAll reads all entries from the WAL
func (w *WAL) ReadAll() ([]*WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, fmt.Errorf("WAL not open")
	}

	// Seek to beginning
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	entries := make([]*WALEntry, 0)
	scanner := bufio.NewScanner(w.file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry WALEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			// Skip corrupt entries
			continue
		}
		entries = append(entries, &entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL: %w", err)
	}

	// Seek back to end for appending
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return entries, nil
}

// Truncate truncates the WAL (after snapshot)
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	// Flush and close current file
	w.writer.Flush()
	w.file.Close()

	// Truncate file
	filename := filepath.Join(w.config.Dir, "wal.log")
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	w.sequence = 0

	return nil
}

// GetSequence returns the current sequence number
func (w *WAL) GetSequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}

// WALReader reads WAL entries from a file
type WALReader struct {
	file    *os.File
	scanner *bufio.Scanner
}

// NewWALReader creates a WAL reader for a specific file
func NewWALReader(filename string) (*WALReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No WAL file exists
		}
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	return &WALReader{
		file:    file,
		scanner: bufio.NewScanner(file),
	}, nil
}

// ReadNext reads the next entry
func (r *WALReader) ReadNext() (*WALEntry, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	line := r.scanner.Bytes()
	if len(line) == 0 {
		return r.ReadNext() // Skip empty lines
	}

	var entry WALEntry
	if err := json.Unmarshal(line, &entry); err != nil {
		return nil, ErrCorruptEntry
	}

	return &entry, nil
}

// Close closes the reader
func (r *WALReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
