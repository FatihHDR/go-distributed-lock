package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"distributed-lock/internal/leader"
	"distributed-lock/internal/lock"
	"distributed-lock/internal/node"
)

// MessageType defines the type of RPC message
type MessageType string

const (
	// Lock operations
	MsgAcquireLock MessageType = "acquire_lock"
	MsgRenewLock   MessageType = "renew_lock"
	MsgReleaseLock MessageType = "release_lock"
	MsgGetLock     MessageType = "get_lock"

	// Leader operations
	MsgGetLeader    MessageType = "get_leader"
	MsgHeartbeat    MessageType = "heartbeat"
	MsgRequestVote  MessageType = "request_vote"
	MsgVoteResponse MessageType = "vote_response"

	// Cluster operations
	MsgGetClusterInfo MessageType = "get_cluster_info"
	MsgPing           MessageType = "ping"
	MsgPong           MessageType = "pong"
)

// Request represents an RPC request
type Request struct {
	Type      MessageType     `json:"type"`
	RequestID string          `json:"request_id"`
	Payload   json.RawMessage `json:"payload"`
}

// Response represents an RPC response
type Response struct {
	Type      MessageType     `json:"type"`
	RequestID string          `json:"request_id"`
	Success   bool            `json:"success"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// GetLeaderResponse contains leader information
type GetLeaderResponse struct {
	LeaderID string `json:"leader_id"`
	IsLeader bool   `json:"is_leader"`
	Term     uint64 `json:"term"`
}

// Server handles RPC communication over TCP
type Server struct {
	address  string
	listener net.Listener

	nodeState  *node.State
	lockEngine *lock.Engine
	election   *leader.Election

	connections map[string]net.Conn
	mu          sync.RWMutex
	running     bool
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewServer creates a new RPC server
func NewServer(address string, nodeState *node.State, lockEngine *lock.Engine, election *leader.Election) *Server {
	return &Server{
		address:     address,
		nodeState:   nodeState,
		lockEngine:  lockEngine,
		election:    election,
		connections: make(map[string]net.Conn),
	}
}

// Start begins listening for connections
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	s.wg.Add(1)
	go s.acceptLoop()

	log.Printf("RPC server listening on %s", s.address)
	return nil
}

// Stop halts the server
func (s *Server) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.stopCh)

	// Close all connections
	for _, conn := range s.connections {
		conn.Close()
	}
	s.connections = make(map[string]net.Conn)

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	log.Println("RPC server stopped")
	return nil
}

// acceptLoop accepts incoming connections
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		// Set accept deadline to allow checking stopCh
		s.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))

		conn, err := s.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-s.stopCh:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		remoteAddr := conn.RemoteAddr().String()
		s.mu.Lock()
		s.connections[remoteAddr] = conn
		s.mu.Unlock()

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection processes incoming requests on a connection
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		remoteAddr := conn.RemoteAddr().String()
		s.mu.Lock()
		delete(s.connections, remoteAddr)
		s.mu.Unlock()
		conn.Close()
	}()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		var req Request
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Decode error: %v", err)
			return
		}

		resp := s.handleRequest(&req)
		if err := encoder.Encode(resp); err != nil {
			log.Printf("Encode error: %v", err)
			return
		}
	}
}

// handleRequest routes and processes a request
func (s *Server) handleRequest(req *Request) *Response {
	resp := &Response{
		Type:      req.Type,
		RequestID: req.RequestID,
	}

	switch req.Type {
	case MsgAcquireLock:
		s.handleAcquireLock(req, resp)
	case MsgRenewLock:
		s.handleRenewLock(req, resp)
	case MsgReleaseLock:
		s.handleReleaseLock(req, resp)
	case MsgGetLock:
		s.handleGetLock(req, resp)
	case MsgGetLeader:
		s.handleGetLeader(req, resp)
	case MsgHeartbeat:
		s.handleHeartbeat(req, resp)
	case MsgGetClusterInfo:
		s.handleGetClusterInfo(req, resp)
	case MsgPing:
		s.handlePing(req, resp)
	default:
		resp.Success = false
		resp.Error = fmt.Sprintf("unknown message type: %s", req.Type)
	}

	return resp
}

// handleAcquireLock processes a lock acquisition request
func (s *Server) handleAcquireLock(req *Request, resp *Response) {
	var payload lock.AcquireRequest
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("invalid payload: %v", err)
		return
	}

	result := s.lockEngine.AcquireLock(&payload)
	resp.Success = result.Success

	if result.Success {
		data, _ := json.Marshal(result)
		resp.Payload = data
	} else {
		resp.Error = result.Error
	}
}

// handleRenewLock processes a lock renewal request
func (s *Server) handleRenewLock(req *Request, resp *Response) {
	var payload lock.RenewRequest
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("invalid payload: %v", err)
		return
	}

	result := s.lockEngine.RenewLock(&payload)
	resp.Success = result.Success

	if result.Success {
		data, _ := json.Marshal(result)
		resp.Payload = data
	} else {
		resp.Error = result.Error
	}
}

// handleReleaseLock processes a lock release request
func (s *Server) handleReleaseLock(req *Request, resp *Response) {
	var payload lock.ReleaseRequest
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("invalid payload: %v", err)
		return
	}

	result := s.lockEngine.ReleaseLock(&payload)
	resp.Success = result.Success

	if !result.Success {
		resp.Error = result.Error
	}
}

// handleGetLock processes a lock query request
func (s *Server) handleGetLock(req *Request, resp *Response) {
	type getRequest struct {
		Key string `json:"key"`
	}

	var payload getRequest
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("invalid payload: %v", err)
		return
	}

	lockInfo, err := s.lockEngine.GetLock(payload.Key)
	if err != nil {
		resp.Success = false
		resp.Error = err.Error()
		return
	}

	resp.Success = true
	data, _ := json.Marshal(lockInfo)
	resp.Payload = data
}

// handleGetLeader processes a get leader request
func (s *Server) handleGetLeader(req *Request, resp *Response) {
	leaderID, err := s.election.GetLeader()

	result := GetLeaderResponse{
		LeaderID: leaderID,
		IsLeader: s.election.IsLeader(),
		Term:     s.election.GetTerm(),
	}

	if err != nil && err != leader.ErrNoLeader {
		resp.Success = false
		resp.Error = err.Error()
		return
	}

	resp.Success = true
	data, _ := json.Marshal(result)
	resp.Payload = data
}

// handleHeartbeat processes a heartbeat message
func (s *Server) handleHeartbeat(req *Request, resp *Response) {
	var payload leader.HeartbeatMessage
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("invalid payload: %v", err)
		return
	}

	s.election.ReceiveHeartbeat(payload.LeaderID, payload.Term)
	s.nodeState.UpdatePeerSeen(payload.LeaderID)

	resp.Success = true
}

// handleGetClusterInfo processes a cluster info request
func (s *Server) handleGetClusterInfo(req *Request, resp *Response) {
	info := s.nodeState.GetClusterInfo()
	resp.Success = true
	data, _ := json.Marshal(info)
	resp.Payload = data
}

// handlePing processes a ping request
func (s *Server) handlePing(req *Request, resp *Response) {
	resp.Type = MsgPong
	resp.Success = true

	data, _ := json.Marshal(map[string]interface{}{
		"node_id": s.nodeState.GetID(),
		"time":    time.Now().UnixNano(),
	})
	resp.Payload = data
}

// Client provides RPC client functionality
type Client struct {
	address string
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	mu      sync.Mutex
	reqID   uint64
}

// NewClient creates a new RPC client
func NewClient(address string) *Client {
	return &Client{
		address: address,
	}
}

// Connect establishes connection to the server
func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.address, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.address, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.encoder = json.NewEncoder(conn)
	c.decoder = json.NewDecoder(conn)
	c.mu.Unlock()

	return nil
}

// Close closes the connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Call sends a request and waits for response
func (c *Client) Call(msgType MessageType, payload interface{}) (*Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reqID++
	reqID := fmt.Sprintf("req-%d", c.reqID)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	req := &Request{
		Type:      msgType,
		RequestID: reqID,
		Payload:   payloadBytes,
	}

	if err := c.encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	var resp Response
	if err := c.decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to receive response: %w", err)
	}

	return &resp, nil
}

// AcquireLock acquires a lock via RPC
func (c *Client) AcquireLock(key, owner string, ttl time.Duration) (*lock.AcquireResponse, error) {
	req := &lock.AcquireRequest{
		Key:   key,
		Owner: owner,
		TTL:   ttl,
	}

	resp, err := c.Call(MsgAcquireLock, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return &lock.AcquireResponse{Success: false, Error: resp.Error}, nil
	}

	var result lock.AcquireResponse
	if err := json.Unmarshal(resp.Payload, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

// RenewLock renews a lock via RPC
func (c *Client) RenewLock(key, owner string, ttl time.Duration) (*lock.RenewResponse, error) {
	req := &lock.RenewRequest{
		Key:   key,
		Owner: owner,
		TTL:   ttl,
	}

	resp, err := c.Call(MsgRenewLock, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return &lock.RenewResponse{Success: false, Error: resp.Error}, nil
	}

	var result lock.RenewResponse
	if err := json.Unmarshal(resp.Payload, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

// ReleaseLock releases a lock via RPC
func (c *Client) ReleaseLock(key, owner string) (*lock.ReleaseResponse, error) {
	req := &lock.ReleaseRequest{
		Key:   key,
		Owner: owner,
	}

	resp, err := c.Call(MsgReleaseLock, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return &lock.ReleaseResponse{Success: false, Error: resp.Error}, nil
	}

	return &lock.ReleaseResponse{Success: true}, nil
}

// GetLeader retrieves the current leader via RPC
func (c *Client) GetLeader() (*GetLeaderResponse, error) {
	resp, err := c.Call(MsgGetLeader, nil)
	if err != nil {
		return nil, err
	}

	var result GetLeaderResponse
	if err := json.Unmarshal(resp.Payload, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}
