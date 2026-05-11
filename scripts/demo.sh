#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
echo "--- cluster status"
./run/raftkv status --nodes "$NODES" || true
echo "--- put x=42"
./run/raftkv put --nodes "$NODES" --key x --value 42
echo "--- get x"
./run/raftkv get --nodes "$NODES" --key x
echo "--- write 30 keys to trigger snapshotting"
for i in $(seq 1 30); do ./run/raftkv put --nodes "$NODES" --key "k$i" --value "v$i" >/dev/null; done
./run/raftkv status --nodes "$NODES" | head -5
