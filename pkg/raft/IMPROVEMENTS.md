# Future Improvements

## Raft Replication

- Add conflict index / conflict term information to AppendEntriesResponse to speed up log backtracking.
- Batch AppendEntries requests instead of always sending all missing entries.
- Track the last index sent in each replication result instead of using the leader's current LastLogIndex during response handling.
- Add retry scheduling for followers that fail AppendEntries.

## Durability

- Call file.Sync after WAL and persistent state writes.
- Add crash recovery tests for WAL replay and persistent term/vote restoration.

## Operations

- Add structured logging around elections, leadership changes, and replication.
- Add a small CLI or HTTP client interface for SET/GET/DELETE requests.

## Testing

- Add multi-node integration tests.
- Add network partition simulations.
- Add stale leader and log conflict scenarios.
