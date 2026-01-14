# Distributed Lock System - Architecture

## Overview

This is an in-memory distributed lock system designed for multi-node simulation without persistence or Raft consensus (Phase 0).

## Components

```
┌─────────────────────────────────────────────────────────────┐
│                         Node                                │
├──────────────┬──────────────┬──────────────┬───────────────┤
│   Lock       │   Leader     │     RPC      │    Node       │
│   Engine     │   Election   │    Server    │    State      │
├──────────────┼──────────────┼──────────────┼───────────────┤
│ - Acquire    │ - Bully      │ - TCP        │ - Status      │
│ - Renew      │   Algorithm  │ - JSON       │ - Peers       │
│ - Release    │ - Heartbeat  │   Protocol   │ - Leader      │
│ - TTL Mgmt   │ - Term       │              │   Tracking    │
└──────────────┴──────────────┴──────────────┴───────────────┘
```

## Package Structure

### `internal/lock/`
- **model.go**: Lock data structures and in-memory store
- **engine.go**: Core lock operations (acquire, renew, release)
- **ttl.go**: TTL management and automatic expiration

### `internal/leader/`
- **election.go**: Simple leader election (bully algorithm variant)
- **heartbeat.go**: Leader liveness and peer health tracking

### `internal/rpc/`
- **server.go**: TCP-based RPC server and client

### `internal/node/`
- **state.go**: Node state management and cluster info

### `cmd/node/`
- **main.go**: Node entry point

## API Protocol

### Message Format (JSON over TCP)

**Request:**
```json
{
  "type": "acquire_lock",
  "request_id": "req-1",
  "payload": { ... }
}
```

**Response:**
```json
{
  "type": "acquire_lock",
  "request_id": "req-1",
  "success": true,
  "payload": { ... }
}
```

### Lock Operations

| Operation | Type | Payload |
|-----------|------|---------|
| AcquireLock | `acquire_lock` | `{key, owner, ttl}` |
| RenewLock | `renew_lock` | `{key, owner, ttl}` |
| ReleaseLock | `release_lock` | `{key, owner}` |
| GetLock | `get_lock` | `{key}` |

### Leader Operations

| Operation | Type | Payload |
|-----------|------|---------|
| GetLeader | `get_leader` | `null` |
| Heartbeat | `heartbeat` | `{leader_id, term, timestamp}` |

### Cluster Operations

| Operation | Type | Description |
|-----------|------|-------------|
| GetClusterInfo | `get_cluster_info` | Returns node and cluster status |
| Ping | `ping` | Health check |

## Lock Lifecycle

```
┌─────────┐  AcquireLock  ┌──────────┐
│  Free   │ ────────────► │  Held    │
└─────────┘               └────┬─────┘
     ▲                         │
     │         ReleaseLock     │ RenewLock
     │◄────────────────────────┤◄─────┐
     │                         │      │
     │         TTL Expired     │      │
     │◄────────────────────────┘      │
                                      │
                               ┌──────┴─────┐
                               │  Extended  │
                               └────────────┘
```

## Leader Election

Simple bully algorithm variant:
1. Each node starts as **Follower**
2. If no heartbeat received within timeout, become **Candidate**
3. Request votes from peers
4. With majority votes, become **Leader**
5. Leader sends periodic heartbeats

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-id` | node-{port} | Node identifier |
| `-address` | 127.0.0.1 | Listen address |
| `-port` | 9000 | Listen port |
| `-bootstrap` | false | Start as leader |
| `-peers` | "" | Comma-separated peer list |

## Future Phases

- **Phase 1**: Raft consensus for strong consistency
- **Phase 2**: Persistence (WAL + snapshots)
- **Phase 3**: Production features (auth, metrics, TLS)
