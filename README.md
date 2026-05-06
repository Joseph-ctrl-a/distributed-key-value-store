# Distributed Key-Value Store

A distributed key-value store built from scratch on the Raft consensus algorithm. The system runs as a cluster of replicated nodes that elect a leader, replicate writes across the cluster, and survive node failures and network partitions.

## Why I built this

Coming from a background in CRUD APIs, I wanted to push further into backend development and understand how distributed systems actually work under the hood. Raft felt like the right place to start — it is a consensus algorithm specifically designed to be understandable, yet it still forces you to reason carefully about failure modes, race conditions, and state that has to survive crashes.

I chose Go deliberately. Beyond the project itself, I wanted real practice with a language that has built-in concurrency primitives and a strong type system, coming from JavaScript. Working through leader election, log replication, and network partition handling in Go made both the language and the distributed systems concepts stick in a way that reading about them never would.

## What it does

- Stores string key-value pairs across a cluster of nodes
- Elects a leader via Raft and routes all writes through it
- Replicates every write to a majority before acknowledging it
- Persists entries to a write-ahead log (WAL) on each node
- Survives node crashes and network partitions
- Simulates faults and visualises cluster state in a live dashboard

## Architecture

```
sim HTTP API  -> node HTTP control API ->  WAL ->  leader AppendEntries RPCs  →  follower WALs  →  commitIndex advance  →  HashMap apply
```

There are two binaries:

**`cmd/node`** a single Raft node. Runs a gRPC server for peer communication and an HTTP control API for the sim to manage it.

**`cmd/sim`** the simulation manager. Spawns and monitors node processes, simulates network partitions, and serves the HTTP/WebSocket API consumed by the frontend.

### Package overview

| Package         | Role                                                                               |
| --------------- | ---------------------------------------------------------------------------------- |
| `pkg/raft`      | Raft state machine: election, replication, log management, node lifecycle          |
| `pkg/wal`       | Append-only write-ahead log. One entry per line: `METHOD:params:term`              |
| `pkg/store`     | Thread-safe in-memory HashMap. The applied state machine                           |
| `pkg/transport` | gRPC protobuf definitions and generated code for `RequestVote` and `AppendEntries` |
| `pkg/sim`       | Process manager, network partition simulation, event bus, HTTP and WebSocket API   |

## Running it

**Start the simulation backend:**

```
go run ./cmd/sim -addr localhost:8080
```

**Start the frontend dev server:**

```
cd web && npm install && npm run dev
```

Then open `http://localhost:5173` in your browser.

## Dashboard

The React frontend visualises the live cluster state:

- **Node cards** show each node's role (leader / candidate / follower), current term, commit index, applied index, and log length. Click a node to open a detail panel showing its full WAL log and applied KV state.
- **Topology** renders the cluster as an SVG graph. Edges turn red when a network partition is active between two nodes.
- **Replication bars** show how far each follower's log is behind the leader.
- **Event log** streams real-time events (elections, heartbeats, commits) over WebSocket.
- **KV console** lets you submit SET and DELETE commands to the cluster.
- **Fault controls** let you apply network partitions with one click to observe how Raft handles split-brain scenarios and leader re-election.

## How Raft works here

**Leader election**: nodes use a randomised election timeout. When a follower stops hearing from the leader it becomes a candidate, increments its term, and requests votes from peers. A node wins if it gets a majority and its log is at least as up-to-date as each voter's log.

**Log replication**: the leader appends each write to its WAL and replicates it to followers via `AppendEntries` RPCs every 500ms. A follower only accepts an entry if its log matches at `prevLogIndex`/`prevLogTerm`. The leader advances `commitIndex` once a majority of nodes have matched the entry.

**Catch-up**: when a partitioned node rejoins, the leader backtracks `nextIndex` for that peer one step per tick until it finds the common point, then sends all missing entries in a single batch.

**Persistence**: `currentTerm` and `votedFor` are written to `data/{id}/state.json` before any RPC response. The WAL is append-only and used to reconstruct state on restart.

## Tests

```
go test ./...
```
