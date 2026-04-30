# TODO

## Election

- [ DONE ] Fix remaining `RequestVote` gaps (role update, leader reset, election timer reset on vote grant, error handling in `voteForCandidate`)
- [ ] `startElection` — become candidate, fan out `RequestVote` RPCs, collect majority, become leader
- [ ] Reset `votedFor` between elections (new term)

## Replication

- [ ] `AppendEntries` — heartbeat + log replication handler
- [ ] Leader sending entries to followers
- [ ] Commit index tracking
- [ ] Apply committed entries to the hashmap

## Leader responsibilities

- [ ] Send periodic heartbeats to followers
- [ ] Track `nextIndex` and `matchIndex` per follower
- [ ] Handle follower log inconsistencies

## Startup & Recovery

- [ ] Restore node state from WAL and `PersistentState` on restart
- [ ] `file.Sync()` after WAL writes for durability

## Client Interface

- [ ] Accept read/write requests from clients
- [ ] Redirect clients to the leader

## Testing

- [ ] Multi-node simulation
- [ ] Election correctness
- [ ] Network partition scenarios
