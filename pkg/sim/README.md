# pkg/sim

The simulation layer. Manages node processes, simulates network partitions, publishes cluster events, and serves the HTTP and WebSocket API consumed by the frontend.

## Manager

`Manager` is the central type. It is constructed with `NewManager()` and accepts functional options:

| Option | Purpose |
|---|---|
| `WithRootDir(path)` | Sets the working directory for builds and binary paths |
| `WithBinaryPath(path)` | Overrides the default node binary output path |
| `WithNodes(configs)` | Replaces the default three-node cluster config |
| `WithRunner(runner)` | Substitutes the OS process runner (used in tests) |
| `WithHTTPClient(client)` | Substitutes the HTTP client used to poll node status |

The manager builds the node binary on demand via `EnsureNodeBinary`, which only rebuilds if any `.go` source file under `cmd/node` or `pkg` is newer than the binary.

## Process lifecycle

Each node runs as a child OS process. `StartNode` spawns the binary with the node's addresses and peer list as flags, captures stdout and stderr as events, and tracks the process in `m.processes`. `StopNode` cancels the context and kills the process. `waitForProcess` runs in a goroutine and publishes a `node.exited` event when the process terminates.

## Network partitions

`ApplyPartitions(groups)` takes a list of node name groups and computes which peers each node should block. It then calls `PUT /network` on each node's control API with its blocked peer list. Blocked peers return `codes.Unavailable` at the gRPC level, simulating a network partition without stopping the process.

`ClearPartitions` sends an empty blocked list to every node, restoring full connectivity.

## Event bus

`EventBus` is a pub/sub bus with a 500-event circular buffer. Events are published for node starts, stops, exits, log lines, and partition changes. The WebSocket handler in `api.go` streams these events to the frontend in real time.

## HTTP API

| Route | Purpose |
|---|---|
| `GET /api/cluster` | Full cluster snapshot including node status |
| `POST /api/cluster/start` | Start all nodes |
| `POST /api/cluster/stop` | Stop all nodes |
| `POST /api/nodes/{id}/start` | Start a single node |
| `POST /api/nodes/{id}/stop` | Stop a single node |
| `GET /api/nodes/{id}/kv` | Proxy to the node's `/kv` endpoint |
| `GET /api/nodes/{id}/log` | Proxy to the node's `/log` endpoint |
| `POST /api/kv/set` | Submit a SET to the leader |
| `POST /api/kv/delete` | Submit a DELETE to the leader |
| `POST /api/partitions` | Apply a partition configuration |
| `DELETE /api/partitions` | Clear all partitions |
| `GET /api/events` | Return buffered events |
| `GET /api/ws` | WebSocket stream of live events |
