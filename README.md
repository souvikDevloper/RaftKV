# RaftKV — Distributed Key-Value Store

RaftKV is a Go implementation of a replicated key-value store built around a custom Raft consensus engine. It is intentionally small enough to explain in an interview, but complete enough to demonstrate leader election, log replication, quorum commits, durable storage, snapshots, and fault-injection testing.

## Why this project matters

This repo targets the same core systems ideas behind replicated metadata stores, distributed locks, and coordination layers:

- leader election
- majority quorum replication
- durable write-ahead log
- ordered state-machine application
- snapshot-based log compaction
- failover after leader crash
- stale-read detection through a Jepsen-style history checker

## Tech stack

- **Go** for the server and CLI
- **gRPC** peer/client transport using an explicit JSON codec
- **BoltDB / bbolt** for durable Raft metadata, WAL entries, and snapshots
- **Docker Compose** for a 5-node cluster
- **GitHub Actions** for tests and a smoke fault suite
- **Python** for the history checker

## Architecture

```text
client CLI
   |
   | Put/Get/Status over gRPC
   v
leader node
   |
   | AppendEntries / RequestVote / InstallSnapshot over gRPC
   v
followers
   |
   v
BoltDB WAL + snapshot + in-memory KV state machine
```

## Features

### Raft consensus

- RequestVote RPC
- AppendEntries RPC
- InstallSnapshot RPC
- randomized election timeout
- leader heartbeats
- term and votedFor persistence
- leader-only reads/writes
- majority quorum commits
- follower log conflict repair

### Storage

- durable metadata: `currentTerm`, `votedFor`
- durable replicated log entries
- snapshot persistence
- log compaction after configurable commit threshold

### Fault testing

- 5-node local cluster
- leader crash script
- chaos script that writes before/after leader death
- history checker that detects stale reads after committed writes
- CI smoke test for fault scenario

## Run locally

```bash
./scripts/start_cluster.sh
./scripts/demo.sh  # start_cluster waits until a leader is elected
```

Stop the cluster:

```bash
./scripts/stop_cluster.sh
```

## Manual commands

```bash
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"

./run/raftkv status --nodes "$NODES"
./run/raftkv put --nodes "$NODES" --key user:1 --value active
./run/raftkv get --nodes "$NODES" --key user:1
```

## Fault injection demo

```bash
./scripts/start_cluster.sh
sleep 2
./scripts/kill_leader.sh
sleep 1
./scripts/demo.sh
./scripts/stop_cluster.sh
```

## Jepsen-style history check

```bash
./scripts/chaos.sh
```

For a longer local suite:

```bash
RUNS=50 ./scripts/run_fault_suite.sh
```

## Docker Compose

```bash
docker compose up --build
```

In another terminal:

```bash
go build -o run/raftkv ./cmd/raftkv
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
./run/raftkv put --nodes "$NODES" --key x --value 42
./run/raftkv get --nodes "$NODES" --key x
```

## Benchmark

```bash
./scripts/start_cluster.sh
sleep 2
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
./run/raftkv bench --nodes "$NODES" --n 1000
./scripts/stop_cluster.sh
```

Benchmark numbers depend heavily on laptop, OS, and background processes. Keep the output in `docs/benchmark.md` if you want to quote it in your resume.

## Interview talking points

1. **Why Raft instead of primary-backup?** Raft provides a clear replicated-log model, term-based leader authority, and quorum-based safety.
2. **How are writes committed?** The leader appends a log entry locally, replicates it to followers, and advances commitIndex after a majority acknowledges.
3. **Why leader-only reads?** It keeps the demo simple and avoids stale follower reads without implementing lease reads or ReadIndex.
4. **What does BoltDB store?** Current term, votedFor, log entries, snapshot index/term, and the compacted KV snapshot.
5. **What does the history checker prove?** It checks that successful reads do not go backward after successful writes in recorded order.

## Current limitation

This is a learning/interview project, not etcd. The most important missing production features are membership changes, real network partitions through a proxy, disk fsync tuning, linearizable read-index optimization, and full Elle/Knossos-style verification.

## Offline dependency note

This ZIP is designed to compile in offline environments. It includes small local compatibility layers under `third_party/` and uses `replace` directives in `go.mod` for:

- `google.golang.org/grpc`
- `go.etcd.io/bbolt`

That keeps the project runnable without internet access. For a production-grade GitHub version, replace these local shims with the official packages and generated protobuf definitions.
