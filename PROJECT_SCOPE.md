# Project Scope: Distributed Key-Value Store

## Summary

A fully functional distributed key-value store built from scratch, implementing the Raft consensus algorithm without the use of any consensus libraries. The system runs as a cluster of replicated nodes that coordinate through leader election, replicate every write across the cluster before acknowledging it, and recover correctly from node failures and network partitions.

A React-based simulation dashboard visualises the live cluster state, allows fault injection, and lets you submit reads and writes in real time.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Core language | Go 1.22 |
| Inter-node communication | gRPC (Protocol Buffers) |
| Frontend | React 18, TypeScript, Vite |
| Data fetching | TanStack Query |
| Real-time updates | WebSocket |
| Schema validation | Zod |
| Styling | CSS Modules |

---

## What was implemented from scratch

**Raft consensus algorithm**
The full core of the Raft paper was implemented without any third-party consensus library:
* Randomised election timeouts and leader election via majority quorum
* Vote restriction: a node only grants a vote if the candidate's log is at least as up-to-date as its own, preventing stale nodes from becoming leader
* Log replication: the leader replicates entries to followers via `AppendEntries` RPCs and advances `commitIndex` once a majority has matched
* Log consistency checks: followers verify `prevLogIndex` and `prevLogTerm` before appending, and truncate conflicting entries
* Follower catch-up: the leader backtracks `nextIndex` per peer until it finds a common log point, then replays all missing entries in a single batch
* Persistent state: `currentTerm` and `votedFor` are written to disk before any RPC response, ensuring correctness across crashes

**Write-ahead log (WAL)**
An append-only file-backed log with concurrent read/write access. Supports entry appending, index-based reads, term lookups, and in-place truncation for conflict resolution.

**Simulation manager**
A process manager that spawns real OS child processes for each node, monitors them, and exposes a REST API over them. Network partitions are simulated at the application layer by maintaining a blocked peer list on each node — blocked peers return `gRPC Unavailable` immediately, without stopping the process.

**Network partition fault injection**
The frontend lets you split the cluster into arbitrary groups with one click. The sim computes which peers each node should block and pushes the configuration to every node via HTTP. This lets you observe split-brain scenarios, isolated candidates, and leader re-election in real time.

**Full-stack dashboard**
Built without any graph library dependency. The topology view is a hand-written SVG that colours edges red when a partition is active. The node detail panel queries the live WAL log and applied key-value state for each node on demand.

---

## Architecture

```
React frontend
      |
      | HTTP + WebSocket
      v
pkg/sim  (simulation manager, HTTP API, event bus, process lifecycle)
      |
      | HTTP control API (per node)
      v
pkg/raft  (Raft state machine: election, replication, commit, apply)
      |
      | writes
      v
pkg/wal   (append-only write-ahead log)
pkg/store (thread-safe in-memory HashMap — the applied state machine)

  node <──gRPC──> node   (RequestVote, AppendEntries)
```

---

## Complexity highlights

* **Concurrency throughout**: every node runs multiple goroutines concurrently (election timer, replication ticker, gRPC server, HTTP server). All shared state is protected with `sync.RWMutex`.
* **Correctness under failure**: the implementation handles the subtle Figure 8 case from the Raft paper — a leader will not commit entries from a previous term directly, only indirectly by committing a current-term entry past them.
* **Real process isolation**: nodes run as separate OS processes, not goroutines. The sim communicates with them over HTTP and gRPC, the same way a production orchestrator would.
* **End-to-end data path**: a `SET` command travels from the browser through the sim HTTP API, to the leader's control API, into the WAL, across the network via gRPC to follower WALs, advances `commitIndex` on a majority match, and is then applied to the in-memory HashMap — all observable in the dashboard.

---

## What it does not include

* Log snapshotting / compaction (the WAL grows unbounded)
* Linearizable reads (reads are served directly from the local HashMap without a leadership confirmation round-trip)
* Dynamic cluster membership changes
