#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
leader=$(./run/raftkv status --nodes "$NODES" | python3 -c 'import sys,json
for line in sys.stdin:
    try:
        o=json.loads(line)
        if o.get("role")=="leader":
            print(o["id"])
            break
    except Exception:
        pass')
if [[ -z "${leader:-}" ]]; then echo "no leader found"; exit 1; fi
pidfile="run/$leader.pid"
echo "killing leader $leader pid=$(cat "$pidfile")"
kill "$(cat "$pidfile")"
rm -f "$pidfile"
