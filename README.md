# RaftKV

**RaftKV** is a fault-tolerant replicated key-value store built in Go with a custom Raft consensus engine. It supports leader election, replicated logs, quorum-based writes, durable state, snapshotting, and local fault-injection testing across a 5-node cluster.

The project is designed as a compact distributed systems implementation that demonstrates the core mechanics behind replicated storage and coordination services.

---

## Highlights

- Custom Raft consensus implementation in Go
- 5-node replicated key-value cluster
- Leader election with randomized election timeouts
- Log replication using `AppendEntries`
- Majority-quorum commit flow
- Leader-routed reads and writes
- Persistent Raft metadata and replicated log storage
- Snapshot-based log compaction
- Fault-injection scripts for leader failure
- History checker for stale-read detection
- Docker Compose cluster harness
- GitHub Actions CI workflow

---

## Architecture

```text
                +-------------+
                |   Client    |
                +------+------+
                       |
                       | Put / Get / Status
                       v
                +-------------+
                |   Leader    |
                +------+------+
                       |
        +--------------+--------------+
        |              |              |
        v              v              v
   +---------+    +---------+    +---------+
   |Follower |    |Follower |    |Follower |
   +----+----+    +----+----+    +----+----+
        |              |              |
        v              v              v
  Durable Log    Durable Log    Durable Log
  Snapshot KV    Snapshot KV    Snapshot KV

Each write is accepted by the current leader, appended to the replicated log, sent to followers, and committed only after majority acknowledgement. Committed entries are then applied to the key-value state machine in log order.

Repository Structure
cmd/raftkv/          CLI and server entry point
internal/raft/      Raft consensus implementation
internal/rpc/       RPC transport and request handling
internal/store/     durable metadata, log, and snapshot storage
scripts/            cluster startup, demo, chaos, and benchmark scripts
tools/              history checker utilities
docs/               design and benchmark notes
.github/workflows/  CI configuration
Tech Stack
Area	Technology
Language	Go
Consensus	Custom Raft implementation
Storage	Embedded durable log and snapshot store
Cluster Harness	Bash scripts, Docker Compose
Validation	Go tests, fault scripts, history checker
CI/CD	GitHub Actions
Getting Started
1. Run tests
go test ./...
2. Start a 5-node cluster
./scripts/start_cluster.sh

The script builds the server binary, starts five local nodes, waits for leader election, and prints cluster status.

3. Run the demo
./scripts/demo.sh

The demo performs:

cluster status check
write operation
read operation
multiple writes to trigger snapshotting
final replicated state check
4. Stop the cluster
./scripts/stop_cluster.sh
CLI Usage
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
Cluster status
./run/raftkv status --nodes "$NODES"
Write a key
./run/raftkv put --nodes "$NODES" --key user:1 --value active
Read a key
./run/raftkv get --nodes "$NODES" --key user:1
Fault Injection

RaftKV includes a local chaos workflow that starts a 5-node cluster, writes data, kills the active leader, continues operations through the remaining quorum, and validates the recorded history.

./scripts/chaos.sh

Sample output:

killing leader n5
PASS: checked 9 events; no stale reads after successful writes
cluster stopped

The history checker verifies that successful reads do not observe stale values after successful writes in the recorded execution order.

Snapshotting

RaftKV supports snapshot-based log compaction after a configurable commit threshold. Once the threshold is reached, the state machine snapshot is persisted and old log entries are compacted.

Example demo output:

snapshot_index: 30

This prevents unbounded replicated log growth during long-running workloads.

Benchmarking

Run a local write benchmark:

./scripts/start_cluster.sh

NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
./run/raftkv bench --nodes "$NODES" --n 50

./scripts/stop_cluster.sh

Sample WSL result:

writes=50 throughput=34.5_ops/sec p50=29.875ms p99=51.356ms

Performance depends on hardware, OS, filesystem, cluster mode, and background workload.

Docker Compose

Start the cluster with Docker Compose:

docker compose up --build

Then use the CLI from another terminal:

go build -o run/raftkv ./cmd/raftkv

NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"

./run/raftkv put --nodes "$NODES" --key x --value 42
./run/raftkv get --nodes "$NODES" --key x
Reliability Checks

RaftKV validates correctness through:

unit tests for consensus and storage components
5-node local cluster startup checks
leader election verification
quorum write validation
leader crash workflow
stale-read history checking
CI smoke test for fault scenarios
Current Scope

RaftKV focuses on core consensus and replication mechanics. It does not yet include:

dynamic membership changes
production-grade network partition simulation
lease reads or ReadIndex optimization
advanced compaction tuning
full formal linearizability verification
production deployment hardening
Roadmap
Add proxy-based network partition testing
Add stronger linearizability checker
Add read-index based linearizable reads
Improve benchmark throughput through batching
Add metrics endpoint for cluster health
Add web dashboard for node status and log replication
License

MIT License