package metrics

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is a simple monotonically increasing counter
type Counter struct {
	value uint64
}

func NewCounter() *Counter {
	return &Counter{}
}

func (c *Counter) Inc() {
	atomic.AddUint64(&c.value, 1)
}

func (c *Counter) Add(delta uint64) {
	atomic.AddUint64(&c.value, delta)
}

func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.value)
}

// Gauge is a value that can go up or down
type Gauge struct {
	value int64
}

func NewGauge() *Gauge {
	return &Gauge{}
}

func (g *Gauge) Set(value int64) {
	atomic.StoreInt64(&g.value, value)
}

func (g *Gauge) Inc() {
	atomic.AddInt64(&g.value, 1)
}

func (g *Gauge) Dec() {
	atomic.AddInt64(&g.value, -1)
}

func (g *Gauge) Add(delta int64) {
	atomic.AddInt64(&g.value, delta)
}

func (g *Gauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}

// Histogram tracks value distribution
type Histogram struct {
	mu      sync.RWMutex
	count   uint64
	sum     float64
	min     float64
	max     float64
	buckets map[float64]uint64
}

func NewHistogram(bucketBoundaries []float64) *Histogram {
	buckets := make(map[float64]uint64)
	for _, b := range bucketBoundaries {
		buckets[b] = 0
	}
	return &Histogram{
		buckets: buckets,
		min:     0,
		max:     0,
	}
}

func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++
	h.sum += value

	if h.count == 1 || value < h.min {
		h.min = value
	}
	if value > h.max {
		h.max = value
	}

	for boundary := range h.buckets {
		if value <= boundary {
			h.buckets[boundary]++
		}
	}
}

func (h *Histogram) Count() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}

func (h *Histogram) Sum() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sum
}

func (h *Histogram) Avg() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.count == 0 {
		return 0
	}
	return h.sum / float64(h.count)
}

func (h *Histogram) Min() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.min
}

func (h *Histogram) Max() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.max
}

// Timer measures duration of operations
type Timer struct {
	histogram *Histogram
}

func NewTimer() *Timer {
	// Default buckets in milliseconds
	buckets := []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
	return &Timer{
		histogram: NewHistogram(buckets),
	}
}

func (t *Timer) Time(fn func()) time.Duration {
	start := time.Now()
	fn()
	duration := time.Since(start)
	t.histogram.Observe(float64(duration.Milliseconds()))
	return duration
}

func (t *Timer) Record(duration time.Duration) {
	t.histogram.Observe(float64(duration.Milliseconds()))
}

func (t *Timer) Count() uint64 {
	return t.histogram.Count()
}

func (t *Timer) AvgMs() float64 {
	return t.histogram.Avg()
}

// LockMetrics tracks lock-specific metrics
type LockMetrics struct {
	// Counters
	AcquireAttempts  *Counter
	AcquireSuccesses *Counter
	AcquireFailures  *Counter
	RenewAttempts    *Counter
	RenewSuccesses   *Counter
	RenewFailures    *Counter
	ReleaseAttempts  *Counter
	ReleaseSuccesses *Counter
	Expirations      *Counter

	// Gauges
	ActiveLocks *Gauge

	// Timers
	AcquireLatency *Timer
	RenewLatency   *Timer
	ReleaseLatency *Timer // Renamed to ReleaseLatency for consistency
}

func NewLockMetrics() *LockMetrics {
	return &LockMetrics{
		AcquireAttempts:  NewCounter(),
		AcquireSuccesses: NewCounter(),
		AcquireFailures:  NewCounter(),
		RenewAttempts:    NewCounter(),
		RenewSuccesses:   NewCounter(),
		RenewFailures:    NewCounter(),
		ReleaseAttempts:  NewCounter(),
		ReleaseSuccesses: NewCounter(),
		Expirations:      NewCounter(),
		ActiveLocks:      NewGauge(),
		AcquireLatency:   NewTimer(),
		RenewLatency:     NewTimer(),
		ReleaseLatency:   NewTimer(),
	}
}

func (m *LockMetrics) RecordAcquire(success bool, duration time.Duration) {
	m.AcquireAttempts.Inc()
	m.AcquireLatency.Record(duration)
	if success {
		m.AcquireSuccesses.Inc()
		m.ActiveLocks.Inc()
	} else {
		m.AcquireFailures.Inc()
	}
}

func (m *LockMetrics) RecordRenew(success bool, duration time.Duration) {
	m.RenewAttempts.Inc()
	m.RenewLatency.Record(duration)
	if success {
		m.RenewSuccesses.Inc()
	} else {
		m.RenewFailures.Inc()
	}
}

func (m *LockMetrics) RecordRelease(success bool, duration time.Duration) {
	m.ReleaseAttempts.Inc()
	m.ReleaseLatency.Record(duration)
	if success {
		m.ReleaseSuccesses.Inc()
		m.ActiveLocks.Dec()
	}
}

func (m *LockMetrics) RecordExpiration() {
	m.Expirations.Inc()
	m.ActiveLocks.Dec()
}

// Snapshot returns a point-in-time snapshot of metrics
func (m *LockMetrics) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"acquire_attempts":   m.AcquireAttempts.Value(),
		"acquire_successes":  m.AcquireSuccesses.Value(),
		"acquire_failures":   m.AcquireFailures.Value(),
		"acquire_latency_ms": m.AcquireLatency.AvgMs(),
		"renew_attempts":     m.RenewAttempts.Value(),
		"renew_successes":    m.RenewSuccesses.Value(),
		"renew_failures":     m.RenewFailures.Value(),
		"renew_latency_ms":   m.RenewLatency.AvgMs(),
		"release_attempts":   m.ReleaseAttempts.Value(),
		"release_successes":  m.ReleaseSuccesses.Value(),
		"release_latency_ms": m.ReleaseLatency.AvgMs(),
		"expirations":        m.Expirations.Value(),
		"active_locks":       m.ActiveLocks.Value(),
	}
}

// ClusterMetrics tracks cluster-wide metrics
type ClusterMetrics struct {
	// Counters
	HeartbeatsSent     *Counter
	HeartbeatsReceived *Counter
	ElectionsStarted   *Counter
	LeaderChanges      *Counter
	ForwardedRequests  *Counter
	ForwardFailures    *Counter

	// Gauges
	PeerCount   *Gauge
	AlivePeers  *Gauge
	IsLeader    *Gauge // 1 if leader, 0 otherwise
	CurrentTerm *Gauge
}

func NewClusterMetrics() *ClusterMetrics {
	return &ClusterMetrics{
		HeartbeatsSent:     NewCounter(),
		HeartbeatsReceived: NewCounter(),
		ElectionsStarted:   NewCounter(),
		LeaderChanges:      NewCounter(),
		ForwardedRequests:  NewCounter(),
		ForwardFailures:    NewCounter(),
		PeerCount:          NewGauge(),
		AlivePeers:         NewGauge(),
		IsLeader:           NewGauge(),
		CurrentTerm:        NewGauge(),
	}
}

func (m *ClusterMetrics) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"heartbeats_sent":     m.HeartbeatsSent.Value(),
		"heartbeats_received": m.HeartbeatsReceived.Value(),
		"elections_started":   m.ElectionsStarted.Value(),
		"leader_changes":      m.LeaderChanges.Value(),
		"forwarded_requests":  m.ForwardedRequests.Value(),
		"forward_failures":    m.ForwardFailures.Value(),
		"peer_count":          m.PeerCount.Value(),
		"alive_peers":         m.AlivePeers.Value(),
		"is_leader":           m.IsLeader.Value(),
		"current_term":        m.CurrentTerm.Value(),
	}
}

// MetricsRegistry holds all metrics
type MetricsRegistry struct {
	Lock    *LockMetrics
	Cluster *ClusterMetrics
	nodeID  string
}

func NewMetricsRegistry(nodeID string) *MetricsRegistry {
	return &MetricsRegistry{
		Lock:    NewLockMetrics(),
		Cluster: NewClusterMetrics(),
		nodeID:  nodeID,
	}
}

// AllMetrics returns all metrics as a map
func (r *MetricsRegistry) AllMetrics() map[string]interface{} {
	return map[string]interface{}{
		"node_id":   r.nodeID,
		"lock":      r.Lock.Snapshot(),
		"cluster":   r.Cluster.Snapshot(),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
}

// JSON returns all metrics as JSON
func (r *MetricsRegistry) JSON() ([]byte, error) {
	return json.MarshalIndent(r.AllMetrics(), "", "  ")
}
