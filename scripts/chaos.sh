#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
SNAPSHOT_EVERY=1000 ./scripts/start_cluster.sh
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
H="run/history.jsonl"

# Calls overlap the leader crash. Failed calls remain pending in Porcupine's
# event history, so a timed-out write is allowed to have committed; ignoring
# those calls would produce false correctness failures and false confidence.
./run/chaosload --nodes "$NODES" --workers 4 --ops-per-worker 12 --history "$H" &
load_pid=$!
sleep 0.15
./scripts/kill_leader.sh
wait "$load_pid"
./run/linearizability --history "$H" --timeout 30s
./scripts/stop_cluster.sh
