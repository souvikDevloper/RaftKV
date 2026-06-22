# Benchmark methodology

RaftKV performance claims use the official YCSB 0.17.0 Redis binding. The repository does not use a custom request generator for headline numbers.

## Claim profile

```text
workload:          C (100% reads)
records:           1,000
operations:        100,000
clients:           16
record shape:      1 field x 100 bytes
measurement:       HdrHistogram
topology:          five local Raft processes
benchmark API:     direct RESP endpoint on the leader
durability:        bbolt-backed replicas
```

The gate passes only when throughput is at least 18,000 operations/second and READ p99 is below 3,000 microseconds. The record shape and concurrency are part of the claim and must be reported with the number.

## Reproduce

```powershell
.\scripts\start_cluster.ps1
.\bench\ycsb\run.ps1 -Workload c -RecordCount 1000 -OperationCount 100000 -Threads 16 -FieldCount 1 -FieldLength 100
.\scripts\stop_cluster.ps1
```

Use `-Workload all` to exercise A-F. Workload E is supported through the Redis binding's sorted-set scan path.

## Evidence rules

- Do not compare results with different record sizes or client counts without labeling the difference.
- Do not average p99 values from multiple runs. Read the percentile from the merged HdrHistogram distribution for each run.
- Keep raw output and `.hdr` files with any published number.
- A dirty-tree run is useful during development but should be rerun from the final committed revision before publishing.
- Benchmark the comparison system on the same hardware and configuration before claiming relative performance.

The 2026-06-21 development run produced 67,430.88 ops/s and 2,789 us READ p99 on an i5-12450H Windows host. It validates the gate for that working tree; it should be regenerated after the changes are committed.
