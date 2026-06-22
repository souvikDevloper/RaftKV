#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

LINE = re.compile(r"^\[([^]]+)\],\s*([^,]+),\s*(.+?)\s*$")


def parse(path: Path) -> dict[str, dict[str, float | str]]:
    metrics: dict[str, dict[str, float | str]] = {}
    data = path.read_bytes()
    encoding = "utf-16" if data.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8"
    for raw in data.decode(encoding, errors="replace").splitlines():
        match = LINE.match(raw)
        if not match:
            continue
        group, name, raw_value = match.groups()
        try:
            value: float | str = float(raw_value)
        except ValueError:
            value = raw_value
        metrics.setdefault(group, {})[name] = value
    return metrics


def main() -> int:
    results = Path(sys.argv[1]).resolve()
    workloads: dict[str, object] = {}
    for path in sorted(results.glob("workload?-run.txt")):
        name = path.name.split("-")[0]
        workloads[name] = {
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "metrics": parse(path),
        }
    summary = {"schema_version": 1, "results_directory": str(results), "workloads": workloads}
    (results / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    checks = []
    c = workloads.get("workloadc", {})
    throughput = c.get("metrics", {}).get("OVERALL", {}).get("Throughput(ops/sec)") if isinstance(c, dict) else None
    p99 = c.get("metrics", {}).get("READ", {}).get("99thPercentileLatency(us)") if isinstance(c, dict) else None
    checks.append({"claim": "Workload C throughput >= 18,000 ops/s", "value": throughput, "pass": isinstance(throughput, float) and throughput >= 18000})
    checks.append({"claim": "Workload C READ p99 < 3,000 us", "value": p99, "pass": isinstance(p99, float) and p99 < 3000})
    evidence = {"checks": checks, "all_pass": all(check["pass"] for check in checks)}
    (results / "claim-verification.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    for check in checks:
        print(("PASS" if check["pass"] else "FAIL"), check["claim"], "observed=", check["value"])
    return 0 if evidence["all_pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
