# raftkv

A replicated key-value store built on [hashicorp/raft](https://github.com/hashicorp/raft), with a
gRPC client API. Three nodes, one raft group, an in-memory state machine persisted through raft
snapshots. No external database.

Writes go through the raft log and are linearizable. Reads are served from whichever node you asked
and may be stale. That tradeoff is deliberate and explained below.

## Architecture

```
                         clients (raftkvctl, or any gRPC client)
                                |                        |
                     Get (served locally)          Set / Delete
                                |                        |
          +---------------------+------------------------+--------------------+
          |                                              |                    |
  +-------v-------------+                    +-----------v---------+   +------v--------------+
  |  node2  follower    |                    |  node3  follower    |   |  node1  LEADER      |
  |  gRPC :50051        |                    |  gRPC :50051        |   |  gRPC :50051        |
  |  raft :9001         |                    |  raft :9001         |   |  raft :9001         |
  |                     |   write forwarded  |                     |   |                     |
  |  api.Server  -------+-------------------->  api.Server  -------+--->  store.Set          |
  |  fsm: map[str]str   |                    |  fsm: map[str]str   |   |  fsm: map[str]str   |
  +----------^----------+                    +----------^----------+   +----------+----------+
             |                                          |                         |
             |            AppendEntries / RequestVote (raft TCP)                   |
             +------------------------------------------+-------------------------+
                                                        |
                              per-node volume: raft.db (boltdb log + stable store)
                                                        snapshots/ (FSM state)
```

Write path: client hits any node, a follower proxies the call to the leader, the leader appends to
the raft log, replicates to a majority, and only then does every node's `FSM.Apply` mutate its map.

Read path: `FSM.Get` reads the local map directly. No raft round trip, no leader involvement.

### Leader discovery

Raft only knows each peer's *raft* address, which is not where clients connect. Rather than
inventing a naming convention or a side-channel registry, each node's gRPC address is replicated
through the log itself as an `OpPeer` entry — written by the leader when it wins a term and when it
admits a joiner. Every node therefore has a consistent node id to gRPC address map in its FSM and
can resolve `LeaderWithID()` into a dialable address. It survives restarts via snapshots for free.

## Layout

```
cmd/raftkv        node entrypoint, flag parsing
cmd/raftkvctl     client CLI: get/set/del/status/leader/snapshot/bench
internal/fsm      raft.FSM: Apply, Snapshot, Restore over the KV map
internal/store    raft node setup, bootstrap/join, membership, write path
internal/api      kv.proto, generated stubs, gRPC server with leader forwarding
internal/cluster  join an existing cluster with retry/backoff
scripts           failover demo and throughput check
```

## Consistency

**Writes are linearizable.** A `Set` or `Delete` is committed by a single leader, replicated to a
majority, and applied in log order on every node. A write that returns success is durable against
the loss of any one node in a three-node cluster. If leadership is lost mid-write the call fails
with `FAILED_PRECONDITION` rather than silently succeeding.

**Reads are not linearizable — they are stale-tolerant local reads.** `Get` returns whatever the
contacted node's FSM currently holds. It costs one network hop and no disk or consensus work, and
read throughput scales with the number of nodes. The costs:

- A read on node B immediately after a write acknowledged by leader A may not see that write, since
  the entry only has to reach a *majority*, not all nodes, before being acknowledged.
- A partitioned or lagging follower will happily serve arbitrarily old data. There is no bound on
  staleness.

Making reads linearizable would mean routing them to the leader behind a `raft.Barrier()`, or
implementing the ReadIndex / leader-lease optimisation. Both trade read latency and leader load for
freshness. For a cache-like KV store, stale local reads are usually the right default; this is the
same choice Consul exposes as its `stale` query mode.

**Writes to a follower are transparently forwarded.** A follower resolves the leader's gRPC address
from its FSM, dials it (connections are cached), and replays the request with a
`raftkv-forwarded: 1` metadata marker. If a forwarded request somehow reaches another non-leader,
that marker makes it fail fast with `not leader, leader is at <addr>` instead of looping between
nodes. The response reports which leader absorbed the write.

## Running a 3-node cluster

### Docker

```sh
docker compose up -d --build
docker compose ps

docker compose exec node1 raftkvctl -addr 127.0.0.1:50051 status
```

`node1` bootstraps; `node2` and `node3` wait for it to pass its healthcheck and then call the `Join`
RPC. Each node gets its own named volume, so raft state survives `docker compose restart`.

Host ports are `50051`, `50052`, `50053` for node1/2/3. From the host:

```sh
go run ./cmd/raftkvctl -addr 127.0.0.1:50052 set greeting hello
go run ./cmd/raftkvctl -addr 127.0.0.1:50053 get greeting
```

### Without Docker

```sh
go build -o bin/ ./cmd/...

bin/raftkv -id=node1 -bootstrap \
  -raft-addr=127.0.0.1:9001 -grpc-addr=127.0.0.1:50051 -data-dir=tmp/n1 &

bin/raftkv -id=node2 -join=127.0.0.1:50051 \
  -raft-addr=127.0.0.1:9002 -grpc-addr=127.0.0.1:50052 -data-dir=tmp/n2 &

bin/raftkv -id=node3 -join=127.0.0.1:50051 \
  -raft-addr=127.0.0.1:9003 -grpc-addr=127.0.0.1:50053 -data-dir=tmp/n3 &

bin/raftkvctl -addr 127.0.0.1:50052 set alpha one   # OK (forwarded to 127.0.0.1:50051)
bin/raftkvctl -addr 127.0.0.1:50053 get alpha       # one
```

### Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-id` | `node1` | Node id, unique and stable across restarts |
| `-raft-addr` | `127.0.0.1:9001` | Raft bind address |
| `-raft-advertise` | `-raft-addr` | Raft address peers dial; required when bound to `0.0.0.0` |
| `-grpc-addr` | `127.0.0.1:50051` | gRPC bind address |
| `-grpc-advertise` | `-grpc-addr` | gRPC address peers dial for write forwarding |
| `-join` | | Comma-separated gRPC addresses of existing nodes |
| `-bootstrap` | `false` | Create a new single-node cluster; mutually exclusive with `-join` |
| `-data-dir` | `data` | Raft log and snapshots |
| `-log-level` | `info` | Application log level |
| `-raft-log-level` | `warn` | Raft internals log level |

A node started with existing state in `-data-dir` neither bootstraps nor re-joins: it recovers its
log and rejoins the cluster it already belongs to.

## Failover demo

```sh
scripts/killnode.sh          # stops the current leader
scripts/killnode.sh node2    # or stop a specific node
```

The script finds the leader, writes a key, stops that container, waits for the surviving two to
elect a new leader, then verifies the pre-failure key is still readable on both and that fresh
writes still commit through them. It exits non-zero if any of that fails.

One subtlety worth knowing: for roughly a heartbeat timeout after a leader dies, its peers still
report it as leader, so writes in that window fail with `UNAVAILABLE`. The script waits for a leader
that is actually one of the survivors rather than trusting the first answer. With timeouts tuned to
500ms heartbeat / 500ms election, a new leader is in place about a second after the kill.

Bring the node back and watch it catch up:

```sh
docker compose start node1
docker compose exec node1 raftkvctl -addr 127.0.0.1:50051 status
```

## Snapshots

`FSM.Snapshot` serialises the map and the peer table; `FSM.Restore` replaces state from a snapshot
stream. Raft snapshots automatically every 30s once 2048 log entries have accumulated. To force one:

```sh
docker compose exec node1 raftkvctl -addr 127.0.0.1:50051 snapshot
```

## Throughput check

```sh
scripts/bench.sh             # 2000 ops, 16 workers
scripts/bench.sh 10000 32
```

Reports three numbers: writes against the leader, writes against a follower (an extra proxy hop per
op), and local reads. The gap between the first two is the cost of forwarding; the gap between
writes and reads is the cost of consensus.

## Known limitations

- **Stale reads.** As described above. No staleness bound, no read-your-writes guarantee across
  nodes.
- **No client-side leader caching.** Clients pick a node arbitrarily, so every write to a follower
  pays an extra network hop. A real client would cache the leader from `Status` or a
  `not leader, leader is at X` error and re-resolve on failure.
- **No internal retry across elections.** Writes issued during the election window return
  `UNAVAILABLE` and the caller must retry. The server does not queue or replay them.
- **No transactions.** Single-key `Set`/`Delete` only — no CAS, no atomic multi-key batches, no
  watches, no TTLs, no range scans or key iteration.
- **State must fit in memory,** and each snapshot is one JSON blob written in a single pass. Fine at
  demo scale; large state would need a streaming or chunked encoding.
- **No security.** gRPC is plaintext with no auth, and any caller can invoke `Join` and add a voter.
  Do not expose this outside a trusted network.
- **Membership is manual.** A permanently dead node stays a voter until someone removes it; there is
  no `Leave` RPC or automatic reaping, and losing two of three nodes halts writes until a node
  returns.
- **One raft group.** No sharding, so total write throughput is bounded by a single leader's disk
  and network.

## Development

Requires Go 1.26 — `raft-boltdb/v2` and `golang.org/x/sys` set that floor, which `go mod tidy`
enforces.

Regenerating the gRPC stubs after editing `internal/api/kv.proto`:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
buf generate        # or: protoc --go_out=. --go-grpc_out=. internal/api/kv.proto
```

```sh
go build ./... && go vet ./...
```
