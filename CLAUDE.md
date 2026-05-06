# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build node binary
go build -o bin/dkvs-node ./cmd/node

# Run the simulation backend (spawns nodes, serves HTTP + WebSocket)
go run ./cmd/sim -addr localhost:8080

# Run all tests
go test ./...

# Run tests for a specific package
go test ./pkg/raft
go test ./pkg/wal

# Run a single test
go test ./pkg/raft -run TestTallyVotes

# Web frontend
cd web && npm install && npm run dev   # dev server on :5173
cd web && npm run build
```

## Architecture Overview

This is a distributed key-value store built on Raft consensus. There are two separate binaries:

- **`cmd/node`** — a single Raft node (gRPC server + HTTP control API)
- **`cmd/sim`** — a simulation manager that spawns multiple node processes, manages network partitions, and exposes an HTTP/WebSocket API consumed by the React frontend

### Data path

```
sim HTTP API → node HTTP control API → WAL → leader AppendEntries RPCs → follower WALs → commitIndex advance → HashMap apply
```

Reads go directly to the local `store.HashMap` (no linearizability guarantee — not yet).

### Package roles

| Package | Role |
|---|---|
| `pkg/raft` | Raft state machine: election, replication, log management, node lifecycle |
| `pkg/wal` | Append-only write-ahead log on disk; one entry per line (`SET:k,v:term\n`) |
| `pkg/store` | Thread-safe in-memory HashMap; the applied state machine |
| `pkg/transport` | gRPC protobuf definitions (`RequestVote`, `AppendEntries`) and generated code |
| `pkg/sim` | Process manager, network partition simulation, event bus, HTTP API |

### Raft node internals (`pkg/raft/`)

The `Node` struct in [node.go](pkg/raft/node.go) is the central type. Key relationships:
- `*wal.Wal` — the durable log; every leader or follower write goes here first
- `*PersistentState` — `currentTerm` + `votedFor` persisted to `data/{id}/state.json` as JSON; updated atomically before any RPC response
- `*store.HashMap` — applied only after `commitIndex` advances
- `nextIndex`/`matchIndex` maps — tracked per-peer, only valid on the leader

**Election** ([election.go](pkg/raft/election.go), [candidate.go](pkg/raft/candidate.go)): randomized 150–300ms timeout; majority quorum of `len(peers)+1`

**Replication** ([replication.go](pkg/raft/replication.go), [sendEntry.go](pkg/raft/sendEntry.go)): leader ticks every 50ms, sends `AppendEntries` to all peers; followers verify `prevLogIndex`/`prevLogTerm` before appending; `tryAdvanceCommitIndex` advances commit when a majority matches

**Network partition simulation**: blocked peers return `codes.Unavailable`; the sim manager sets `blockedPeers` via `PUT /network` on each node's control API

### WAL format

Each line: `METHOD:params:term\n`
- `SET:mykey,myvalue:5`
- `DELETE:mykey:7`

`LastLogIndex()` = line count. `SpliceInPlace(index)` truncates the file for follower conflict resolution. No snapshotting — the log grows unbounded.

### Simulation event bus (`pkg/sim/events.go`)

Pub/sub with a 500-event circular buffer. Events (node.started, node.stopped, node.log, partition.applied, etc.) are streamed to the React frontend over WebSocket at `GET /api/ws`.

### Test helpers

`pkg/raft/test_helpers_test.go` provides `newTestNode()` (temp WAL + state file) and `newTestNodeWithEntries()`. Tests are table-driven and split by behavior across files (`election_test.go`, `replication_test.go`, `leader_follower_test.go`, etc.).
