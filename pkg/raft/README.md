# pkg/raft

The Raft consensus state machine. Handles leader election, log replication, and the node lifecycle.

## Key type

`Node` is the central type. One instance runs per process. Its fields map directly to the Raft paper:

| Field          | Role                                                             |
| -------------- | ---------------------------------------------------------------- |
| `role`         | `"follower"`, `"candidate"`, or `"leader"`                       |
| `currentTerm`  | Monotonically increasing term number, persisted to disk          |
| `votedFor`     | Candidate voted for this term, persisted to disk                 |
| `log`          | The WAL — durable, append-only entry log                         |
| `commitIndex`  | Highest log index known to be committed                          |
| `lastApplied`  | Highest log index applied to the state machine                   |
| `nextIndex`    | Per-peer: next log index to send (leader only)                   |
| `matchIndex`   | Per-peer: highest log index known to be replicated (leader only) |
| `blockedPeers` | Set of peer addresses blocked by simulated network policy        |

## Election

Implemented in `election.go` and `candidate.go`. The election timer fires after a randomized timeout. The candidate increments its term, votes for itself, and broadcasts `RequestVote` to all peers. A vote is granted only if the candidate's log is at least as up-to-date as the voter's (compared by `lastLogTerm` then `lastLogIndex`). The candidate wins when it has a majority.

## Replication

Implemented in `replication.go` and `sendEntry.go`. The leader ticks every 500ms and sends `AppendEntries` to every peer. Each request carries the entries from `nextIndex[peer]` to the end of the leader's log, plus `prevLogIndex` and `prevLogTerm` for the follower to verify consistency. On rejection, the leader decrements `nextIndex` by one and retries — this is how a rejoining node catches up.

`commitIndex` advances when a majority of `matchIndex` values reach a given entry, and only for entries written in the current term.

## Persistence

`currentTerm` and `votedFor` are written to `data/{id}/state.json` before responding to any RPC. This ensures they survive crashes. The WAL itself is durable by virtue of being an append-only file.

## HTTP control API

`control.go` exposes a small HTTP API used by the sim to manage the node:

| Endpoint          | Purpose                                             |
| ----------------- | --------------------------------------------------- |
| `GET /status`     | Returns the node's current Raft state               |
| `PUT /network`    | Sets the blocked peer list for partition simulation |
| `POST /kv/set`    | Submits a SET entry to the leader                   |
| `POST /kv/delete` | Submits a DELETE entry to the leader                |
| `GET /kv`         | Dumps the node's applied key-value state            |
| `GET /log`        | Dumps the node's WAL as structured JSON             |

## Tests

Tests are split by behavior across several files. `test_helpers_test.go` provides `newTestNode()` (temp WAL and state file) for use across all test files.

| File                      | What it covers                                       |
| ------------------------- | ---------------------------------------------------- |
| `election_test.go`        | Vote granting, term rejection, already-voted guard   |
| `replication_test.go`     | AppendEntries consistency checks, commit advancement |
| `leader_follower_test.go` | Full leader-to-follower replication cycle            |
| `heartbeat_test.go`       | Heartbeat acceptance and rejection                   |
| `state_test.go`           | Persistent state read/write                          |
