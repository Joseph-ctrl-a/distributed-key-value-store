# pkg/transport

Generated gRPC code for the Raft RPC protocol. Do not edit these files directly — they are produced from the proto definition.

## RPCs

**`RequestVote`** — sent by a candidate to request a vote from a peer. The response includes whether the vote was granted and the peer's current term.

**`AppendEntries`** — sent by the leader to replicate log entries and deliver heartbeats. An empty `Entries` slice is a heartbeat. The response tells the leader whether the follower accepted the entries and its current term.
