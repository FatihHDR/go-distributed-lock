package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// TCPTransport implements Transport over TCP
type TCPTransport struct {
	mu sync.RWMutex

	nodeID      string
	address     string
	listener    net.Listener
	connections map[string]net.Conn

	raftNode *RaftNode
	timeout  time.Duration

	stopCh  chan struct{}
	running bool
	wg      sync.WaitGroup
}

// NewTCPTransport creates a new TCP transport
func NewTCPTransport(nodeID, address string, timeout time.Duration) *TCPTransport {
	return &TCPTransport{
		nodeID:      nodeID,
		address:     address,
		connections: make(map[string]net.Conn),
		timeout:     timeout,
	}
}

// SetRaftNode sets the Raft node for handling incoming requests
func (t *TCPTransport) SetRaftNode(rn *RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.raftNode = rn
}

// Start begins listening for connections
func (t *TCPTransport) Start() error {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return nil
	}
	t.running = true
	t.stopCh = make(chan struct{})
	t.mu.Unlock()

	listener, err := net.Listen("tcp", t.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	t.listener = listener

	t.wg.Add(1)
	go t.acceptLoop()

	log.Printf("[Transport %s] Listening on %s", t.nodeID, t.address)
	return nil
}

// Stop halts the transport
func (t *TCPTransport) Stop() error {
	t.mu.Lock()
	if !t.running {
		t.mu.Unlock()
		return nil
	}
	t.running = false
	close(t.stopCh)
	t.mu.Unlock()

	if t.listener != nil {
		t.listener.Close()
	}

	// Close all connections
	t.mu.Lock()
	for _, conn := range t.connections {
		conn.Close()
	}
	t.connections = make(map[string]net.Conn)
	t.mu.Unlock()

	t.wg.Wait()
	return nil
}

// acceptLoop accepts incoming connections
func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				log.Printf("[Transport %s] Accept error: %v", t.nodeID, err)
				continue
			}
		}

		t.wg.Add(1)
		go t.handleConnection(conn)
	}
}

// RaftMessage types
const (
	MsgRequestVote          = "request_vote"
	MsgRequestVoteReply     = "request_vote_reply"
	MsgAppendEntries        = "append_entries"
	MsgAppendEntriesReply   = "append_entries_reply"
	MsgInstallSnapshot      = "install_snapshot"
	MsgInstallSnapshotReply = "install_snapshot_reply"
)

// RaftMessage is the wire format for Raft RPCs
type RaftMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// handleConnection handles a single connection
func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		select {
		case <-t.stopCh:
			return
		default:
		}

		conn.SetDeadline(time.Now().Add(t.timeout))

		var msg RaftMessage
		if err := decoder.Decode(&msg); err != nil {
			return
		}

		var response interface{}

		switch msg.Type {
		case MsgRequestVote:
			var args RequestVoteArgs
			if err := json.Unmarshal(msg.Payload, &args); err != nil {
				continue
			}
			t.mu.RLock()
			rn := t.raftNode
			t.mu.RUnlock()
			if rn != nil {
				response = rn.HandleRequestVote(&args)
			}

		case MsgAppendEntries:
			var args AppendEntriesArgs
			if err := json.Unmarshal(msg.Payload, &args); err != nil {
				continue
			}
			t.mu.RLock()
			rn := t.raftNode
			t.mu.RUnlock()
			if rn != nil {
				response = rn.HandleAppendEntries(&args)
			}

		case MsgInstallSnapshot:
			var args InstallSnapshotArgs
			if err := json.Unmarshal(msg.Payload, &args); err != nil {
				continue
			}
			t.mu.RLock()
			rn := t.raftNode
			t.mu.RUnlock()
			if rn != nil {
				response = rn.HandleInstallSnapshot(&args)
			}
		}

		if response != nil {
			encoder.Encode(response)
		}
	}
}

// getConnection gets or creates a connection to a peer
func (t *TCPTransport) getConnection(peer PeerInfo) (net.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, exists := t.connections[peer.ID]; exists {
		return conn, nil
	}

	conn, err := net.DialTimeout("tcp", peer.Address, t.timeout)
	if err != nil {
		return nil, err
	}

	t.connections[peer.ID] = conn
	return conn, nil
}

// closeConnection closes a connection to a peer
func (t *TCPTransport) closeConnection(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, exists := t.connections[peerID]; exists {
		conn.Close()
		delete(t.connections, peerID)
	}
}

// SendRequestVote sends a RequestVote RPC
func (t *TCPTransport) SendRequestVote(peer PeerInfo, args *RequestVoteArgs) (*RequestVoteReply, error) {
	conn, err := t.getConnection(peer)
	if err != nil {
		return nil, err
	}

	payload, _ := json.Marshal(args)
	msg := RaftMessage{Type: MsgRequestVote, Payload: payload}

	conn.SetDeadline(time.Now().Add(t.timeout))
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(msg); err != nil {
		t.closeConnection(peer.ID)
		return nil, err
	}

	var reply RequestVoteReply
	if err := decoder.Decode(&reply); err != nil {
		t.closeConnection(peer.ID)
		return nil, err
	}

	return &reply, nil
}

// SendAppendEntries sends an AppendEntries RPC
func (t *TCPTransport) SendAppendEntries(peer PeerInfo, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	conn, err := t.getConnection(peer)
	if err != nil {
		return nil, err
	}

	payload, _ := json.Marshal(args)
	msg := RaftMessage{Type: MsgAppendEntries, Payload: payload}

	conn.SetDeadline(time.Now().Add(t.timeout))
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(msg); err != nil {
		t.closeConnection(peer.ID)
		return nil, err
	}

	var reply AppendEntriesReply
	if err := decoder.Decode(&reply); err != nil {
		t.closeConnection(peer.ID)
		return nil, err
	}

	return &reply, nil
}

// SendInstallSnapshot sends an InstallSnapshot RPC
func (t *TCPTransport) SendInstallSnapshot(peer PeerInfo, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	conn, err := t.getConnection(peer)
	if err != nil {
		return nil, err
	}

	payload, _ := json.Marshal(args)
	msg := RaftMessage{Type: MsgInstallSnapshot, Payload: payload}

	conn.SetDeadline(time.Now().Add(t.timeout * 10)) // Longer timeout for snapshots
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(msg); err != nil {
		t.closeConnection(peer.ID)
		return nil, err
	}

	var reply InstallSnapshotReply
	if err := decoder.Decode(&reply); err != nil {
		t.closeConnection(peer.ID)
		return nil, err
	}

	return &reply, nil
}
