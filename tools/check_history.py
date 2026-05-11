#!/usr/bin/env python3
"""Small Jepsen-style history checker for RaftKV demos.

It reads JSONL records with fields:
  {"op":"put", "key":"k", "value":"v", "ok":true}
  {"op":"get", "key":"k", "value":"v", "ok":true}
The checker validates monotonic reads after successful writes in recorded order.
This is not a replacement for Knossos/Elle, but it catches stale read regressions
and is intentionally small enough to explain in an interview.
"""
import argparse, json, sys

def check(path: str) -> int:
    latest = {}
    errors = []
    total = 0
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line=line.strip()
            if not line: continue
            total += 1
            ev = json.loads(line)
            if not ev.get("ok", False):
                continue
            op = ev.get("op")
            k = ev.get("key")
            if op == "put":
                latest[k] = ev.get("value")
            elif op == "get":
                expected = latest.get(k)
                if expected is not None and ev.get("value") != expected:
                    errors.append((i, k, expected, ev.get("value")))
    if errors:
        print(f"FAIL: {len(errors)} stale reads found across {total} events")
        for e in errors[:10]:
            print(f"line={e[0]} key={e[1]} expected={e[2]!r} got={e[3]!r}")
        return 1
    print(f"PASS: checked {total} events; no stale reads after successful writes")
    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("history")
    args = ap.parse_args()
    sys.exit(check(args.history))
