# pkg/store

A thread-safe in-memory key-value store. This is the applied state machine — entries are written here only after they have been committed by Raft.

## API

| Method | Purpose |
|---|---|
| `NewHashMap()` | Returns an empty HashMap |
| `Set(key, value)` | Writes a key-value pair |
| `Get(key)` | Returns the value and a boolean indicating whether the key exists |
| `Delete(key)` | Removes a key |
| `Has(key)` | Returns true if the key exists |
| `All()` | Returns a snapshot copy of all key-value pairs |

All methods are safe to call concurrently. Reads use a shared read lock and writes use an exclusive lock.

## Role in the system

`Node` holds a single `*HashMap`. The replication layer calls `Set` and `Delete` only when `lastApplied` is advanced past a committed entry. Reads bypass Raft entirely and go straight to this map, so they are not linearizable — a follower may return stale data.
