package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"distributed-lock/internal/leader"
	"distributed-lock/internal/lock"
	"distributed-lock/internal/node"
	"distributed-lock/internal/rpc"
)

// Config holds node configuration
type Config struct {
	NodeID    string
	Address   string
	Port      int
	Peers     []string
	Bootstrap bool
}

func main() {
	config := parseFlags()

	log.Printf("Starting distributed lock node: %s", config.NodeID)
	log.Printf("Listening on %s:%d", config.Address, config.Port)

	// Initialize node state
	nodeState := node.NewState(config.NodeID, config.Address, config.Port)
	nodeState.SetOnStatusChange(func(old, new node.Status) {
		log.Printf("Node status changed: %s -> %s", old, new)
	})

	// Initialize lock engine
	lockEngine := lock.NewEngine()
	lockEngine.Start()
	defer lockEngine.Stop()

	// Initialize leader election
	electionConfig := leader.DefaultElectionConfig(config.NodeID)
	election := leader.NewElection(electionConfig)

	election.SetOnStateChange(func(old, new leader.State) {
		log.Printf("Election state changed: %s -> %s", old, new)
	})

	election.SetOnLeaderChange(func(leaderID string) {
		log.Printf("Leader changed to: %s", leaderID)
		isLeader := leaderID == config.NodeID
		nodeState.SetLeader(isLeader, leaderID)
	})

	// Add peers
	for _, peer := range config.Peers {
		parts := strings.Split(peer, ":")
		if len(parts) >= 2 {
			nodeState.AddPeer(parts[0], parts[0], 0)
		}
	}
	election.SetPeers(nodeState.GetPeerIDs())

	// Initialize RPC server
	listenAddr := fmt.Sprintf("%s:%d", config.Address, config.Port)
	rpcServer := rpc.NewServer(listenAddr, nodeState, lockEngine, election)

	// Start services
	if err := rpcServer.Start(); err != nil {
		log.Fatalf("Failed to start RPC server: %v", err)
	}
	defer rpcServer.Stop()

	election.Start()
	defer election.Stop()

	// Initialize heartbeat
	heartbeat := leader.NewHeartbeat(config.NodeID, election, electionConfig.HeartbeatInterval)
	heartbeat.SetPeers(nodeState.GetPeerIDs())
	heartbeat.Start()
	defer heartbeat.Stop()

	// Bootstrap as leader if specified
	if config.Bootstrap {
		log.Println("Bootstrapping as leader...")
		election.ForceLeader()
	}

	// Update node status to ready
	nodeState.SetStatus(node.StatusReady)
	log.Println("Node is ready")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down...")
	nodeState.SetStatus(node.StatusLeaving)
}

func parseFlags() *Config {
	config := &Config{}

	flag.StringVar(&config.NodeID, "id", "", "Node ID (required)")
	flag.StringVar(&config.Address, "address", "127.0.0.1", "Listen address")
	flag.IntVar(&config.Port, "port", 9000, "Listen port")
	flag.BoolVar(&config.Bootstrap, "bootstrap", false, "Bootstrap as initial leader")

	var peersStr string
	flag.StringVar(&peersStr, "peers", "", "Comma-separated list of peer addresses (id:host:port)")

	flag.Parse()

	// Generate node ID if not provided
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("node-%d", config.Port)
	}

	// Parse peers
	if peersStr != "" {
		config.Peers = strings.Split(peersStr, ",")
	}

	return config
}
