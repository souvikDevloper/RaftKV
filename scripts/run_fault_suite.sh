#!/usr/bin/env bash
set -euo pipefail
RUNS="${RUNS:-50}"
for i in $(seq 1 "$RUNS"); do
  echo "fault scenario $i/$RUNS"
  ./scripts/chaos.sh
  sleep 0.2
done
