#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
SNAPSHOT_EVERY=1000 ./scripts/start_cluster.sh
sleep 2
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
H="run/history.jsonl"
: > "$H"
record_put() {
  local i="$1"
  if timeout 1 ./run/raftkv put --nodes "$NODES" --key account --value "$i" --json > run/tmp.json 2>/dev/null; then
    python3 -c 'import json,sys; i=sys.argv[1]; o=json.load(open("run/tmp.json")); print(json.dumps({"op":"put","key":"account","value":i,"ok":o.get("Ok",False)}))' "$i" >> "$H"
  else
    echo '{"op":"put","key":"account","value":"'$i'","ok":false}' >> "$H"
  fi
}
record_get() {
  if timeout 1 ./run/raftkv get --nodes "$NODES" --key account --json > run/tmp.json 2>/dev/null; then
    python3 -c 'import json; o=json.load(open("run/tmp.json")); print(json.dumps({"op":"get","key":"account","value":o.get("Value",""),"ok":o.get("Ok",False)}))' >> "$H"
  fi
}
for i in $(seq 1 5); do record_put "$i"; done
./scripts/kill_leader.sh || true
sleep 1
for i in $(seq 6 7); do record_put "$i"; record_get; done
./tools/check_history.py "$H"
./scripts/stop_cluster.sh
