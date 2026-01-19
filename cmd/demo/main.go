package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"distributed-lock/internal/cluster"
	"distributed-lock/internal/lock"
	"distributed-lock/internal/rpc"
)

func main() {
	mode := flag.String("mode", "node", "Mode: node, demo, or client")
	nodeID := flag.String("id", "", "Node ID")
	address := flag.String("address", "127.0.0.1", "Listen address")
	port := flag.Int("port", 9000, "Listen port")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap as initial leader")
	peersStr := flag.String("peers", "", "Comma-separated peers (id:host:port)")

	flag.Parse()

	switch *mode {
	case "node":
		runNode(*nodeID, *address, *port, *bootstrap, *peersStr)
	case "demo":
		runDemo()
	case "client":
		runClient(*address, *port)
	default:
		log.Fatalf("Unknown mode: %s", *mode)
	}
}

func runNode(nodeID, address string, port int, bootstrap bool, peersStr string) {
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%d", port)
	}

	log.Printf("Starting node %s on %s:%d", nodeID, address, port)

	config := cluster.DefaultCoordinatorConfig(nodeID, address, port)
	coord := cluster.NewCoordinator(config)

	// Add peers
	if peersStr != "" {
		for _, peer := range strings.Split(peersStr, ",") {
			parts := strings.Split(peer, ":")
			if len(parts) == 3 {
				peerPort, _ := strconv.Atoi(parts[2])
				coord.AddPeer(parts[0], parts[1], peerPort)
				log.Printf("Added peer: %s at %s:%d", parts[0], parts[1], peerPort)
			}
		}
	}

	// Start RPC server
	rpcAddr := fmt.Sprintf("%s:%d", address, port)
	rpcServer := rpc.NewServer(
		rpcAddr,
		coord.GetNodeState(),
		coord.GetLockEngine(),
		coord.GetElection(),
	)

	if err := rpcServer.Start(); err != nil {
		log.Fatalf("Failed to start RPC server: %v", err)
	}
	defer rpcServer.Stop()

	// Start coordinator
	if err := coord.Start(); err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}
	defer coord.Stop()

	// Bootstrap if specified
	if bootstrap {
		log.Println("Bootstrapping as leader...")
		coord.Bootstrap()
	}

	log.Printf("Node %s is ready", nodeID)

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}

// runDemo runs a 3-node cluster demo
func runDemo() {
	fmt.Println("╔═══════════════════════════════════════════════════════╗")
	fmt.Println("║     Distributed Lock System - 3 Node Demo             ║")
	fmt.Println("╚═══════════════════════════════════════════════════════╝")
	fmt.Println()

	// Create 3 nodes
	nodes := make([]*demoNode, 3)
	ports := []int{9001, 9002, 9003}

	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		port := ports[i]

		config := cluster.DefaultCoordinatorConfig(nodeID, "127.0.0.1", port)
		config.HeartbeatInterval = 1 * time.Second
		config.ElectionTimeout = 3 * time.Second

		coord := cluster.NewCoordinator(config)

		rpcAddr := fmt.Sprintf("127.0.0.1:%d", port)
		rpcServer := rpc.NewServer(
			rpcAddr,
			coord.GetNodeState(),
			coord.GetLockEngine(),
			coord.GetElection(),
		)

		nodes[i] = &demoNode{
			id:        nodeID,
			port:      port,
			coord:     coord,
			rpcServer: rpcServer,
		}
	}

	// Connect nodes as peers
	for i, node := range nodes {
		for j, peer := range nodes {
			if i != j {
				node.coord.AddPeer(peer.id, "127.0.0.1", peer.port)
			}
		}
	}

	// Start all nodes
	fmt.Println("Starting 3 nodes...")
	for _, node := range nodes {
		if err := node.rpcServer.Start(); err != nil {
			log.Fatalf("Failed to start RPC for %s: %v", node.id, err)
		}
		if err := node.coord.Start(); err != nil {
			log.Fatalf("Failed to start coordinator for %s: %v", node.id, err)
		}
	}
	fmt.Println("✓ All nodes started")

	// Bootstrap node-1 as leader
	fmt.Println("\nBootstrapping node-1 as leader...")
	nodes[0].coord.Bootstrap()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("✓ node-1 is now the leader")

	// Print cluster status
	printClusterStatus(nodes)

	// Demo: Acquire lock via leader
	fmt.Println("\n--- Demo: Lock Operations ---")
	ctx := context.Background()

	fmt.Println("\n1. Acquiring lock 'resource-1' via leader (node-1)...")
	resp := nodes[0].coord.AcquireLock(ctx, &lock.AcquireRequest{
		Key:   "resource-1",
		Owner: "demo-client",
		TTL:   30 * time.Second,
	})
	if resp.Success {
		fmt.Println("   ✓ Lock acquired successfully!")
		fmt.Printf("   Lock: key=%s, owner=%s, expires=%v\n",
			resp.Lock.Key, resp.Lock.Owner, resp.Lock.Remaining())
	} else {
		fmt.Printf("   ✗ Failed: %s\n", resp.Error)
	}

	// Demo: Try to acquire same lock via follower
	fmt.Println("\n2. Trying to acquire same lock via follower (node-2)...")
	resp2 := nodes[1].coord.AcquireLock(ctx, &lock.AcquireRequest{
		Key:   "resource-1",
		Owner: "another-client",
		TTL:   30 * time.Second,
	})
	if resp2.Success {
		fmt.Println("   ✓ Lock acquired (unexpected!)")
	} else {
		fmt.Println("   ✓ Lock correctly rejected (already held)")
	}

	// Demo: Release lock
	fmt.Println("\n3. Releasing lock via node-3...")
	releaseResp := nodes[2].coord.ReleaseLock(ctx, &lock.ReleaseRequest{
		Key:   "resource-1",
		Owner: "demo-client",
	})
	if releaseResp.Success {
		fmt.Println("   ✓ Lock released successfully!")
	} else {
		fmt.Printf("   ✗ Failed: %s\n", releaseResp.Error)
	}

	// Demo: Now another client can acquire
	fmt.Println("\n4. Another client acquiring the released lock...")
	resp3 := nodes[1].coord.AcquireLock(ctx, &lock.AcquireRequest{
		Key:   "resource-1",
		Owner: "another-client",
		TTL:   30 * time.Second,
	})
	if resp3.Success {
		fmt.Println("   ✓ Lock acquired by new client!")
	} else {
		fmt.Printf("   ✗ Failed: %s\n", resp3.Error)
	}

	// Print final status
	printClusterStatus(nodes)

	fmt.Println("\n--- Demo Complete ---")
	fmt.Println("\nPress Ctrl+C to stop all nodes...")

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down nodes...")
	for _, node := range nodes {
		node.coord.Stop()
		node.rpcServer.Stop()
	}
	fmt.Println("Done!")
}

type demoNode struct {
	id        string
	port      int
	coord     *cluster.Coordinator
	rpcServer *rpc.Server
}

func printClusterStatus(nodes []*demoNode) {
	fmt.Println("\n┌─────────────────────────────────────────────────────┐")
	fmt.Println("│                  Cluster Status                     │")
	fmt.Println("├─────────────────────────────────────────────────────┤")

	for _, node := range nodes {
		info := node.coord.GetClusterInfo()
		role := "Follower"
		if info.IsLeader {
			role = "LEADER"
		}
		fmt.Printf("│ %-10s [%-8s] Port: %d  Peers: %d alive    │\n",
			info.NodeID, role, node.port, info.AlivePeers)
	}

	fmt.Println("└─────────────────────────────────────────────────────┘")
}

func runClient(address string, port int) {
	serverAddr := fmt.Sprintf("%s:%d", address, port)
	fmt.Printf("Connecting to %s...\n", serverAddr)

	client := rpc.NewClient(serverAddr)
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected! Running lock test...")

	// Get leader
	leader, err := client.GetLeader()
	if err != nil {
		log.Printf("Warning: could not get leader: %v", err)
	} else {
		fmt.Printf("Leader: %s (term: %d)\n", leader.LeaderID, leader.Term)
	}

	// Acquire lock
	resp, err := client.AcquireLock("test-key", "test-client", 30*time.Second)
	if err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}

	if resp.Success {
		fmt.Println("✓ Lock acquired!")
	} else {
		fmt.Printf("✗ Lock failed: %s\n", resp.Error)
	}

	// Release lock
	releaseResp, err := client.ReleaseLock("test-key", "test-client")
	if err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}

	if releaseResp.Success {
		fmt.Println("✓ Lock released!")
	} else {
		fmt.Printf("✗ Release failed: %s\n", releaseResp.Error)
	}
}
