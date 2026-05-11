#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p run data

# Stop any old local cluster first. Without this, a second run may leave ports
# occupied and the new nodes will exit immediately.
if compgen -G "run/*.pid" > /dev/null; then
  for f in run/*.pid; do
    [[ -f "$f" ]] || continue
    kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  sleep 0.3
fi
rm -f run/*.log

# Clean old data for repeatable demos; comment this line to test persistence.
rm -rf data/n{1,2,3,4,5}

SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-10}"
go build -o run/raftkv ./cmd/raftkv

nodes=(127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005)
for i in 1 2 3 4 5; do
  peers=""
  for j in 1 2 3 4 5; do
    if [[ "$i" != "$j" ]]; then
      [[ -n "$peers" ]] && peers+=","
      peers+="n$j=127.0.0.1:700$j"
    fi
  done
  ./run/raftkv server --id "n$i" --listen "127.0.0.1:700$i" --peers "$peers" --data "data/n$i" --snapshot-every "$SNAPSHOT_EVERY" > "run/n$i.log" 2>&1 &
  echo $! > "run/n$i.pid"
done

echo "started 5-node RaftKV cluster"
echo "logs: run/n*.log"

NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
for attempt in $(seq 1 60); do
  out="$(./run/raftkv status --nodes "$NODES" 2>/dev/null || true)"
  if echo "$out" | grep -q '"role":"leader"'; then
    echo "cluster ready"
    echo "$out"
    exit 0
  fi
  sleep 0.2
done

echo "cluster did not become ready in time; showing logs:" >&2
for f in run/n*.log; do
  echo "--- $f" >&2
  tail -20 "$f" >&2 || true
done
exit 1
