# pkg/wal

An append-only write-ahead log backed by a single file. Used by every Raft node to durably record log entries before they are applied to the state machine.

## Entry format

Each line in the file is one log entry:

```
METHOD:param1,param2:term
```

Examples:
```
SET:mykey,myvalue:5
DELETE:mykey:7
```

The log index of an entry is its 1-based line number. There is no header or binary framing — the file is plain text and human-readable.

## Key methods

| Method | Purpose |
|---|---|
| `NewWal(path)` | Opens or creates a WAL file. Replays existing lines to restore `lineCount` and `lastEntry` |
| `Append(entry)` | Serialises a `LogEntry` and appends it to the file |
| `EntriesFromIndex(i)` | Returns all raw entry strings from index `i` to the end |
| `GetTermAtIndex(i)` | Returns the term stored in the entry at index `i` |
| `LastLogIndex()` | Returns the current line count (the last written index) |
| `LastLogTerm()` | Returns the term field of the last entry |
| `SpliceInPlace(i)` | Truncates the file to keep only entries up to index `i`. Used by followers to resolve log conflicts |
| `ForEach(fn)` | Iterates every entry with its index, stopping if `fn` returns an error |

## Concurrency

All public methods are safe to call concurrently. `Append` and `SpliceInPlace` take a write lock. All reads take a read lock.

## Limitations

The log grows unbounded. There is no snapshotting or log compaction. On restart, the entire file is scanned to restore `lineCount` and `lastEntry`.
