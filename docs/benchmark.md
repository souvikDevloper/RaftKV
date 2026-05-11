# Benchmarks

Local smoke benchmark from the generated workspace:

```text
writes=50 throughput=281.3_ops_sec p50=2.751488ms p99=10.232868ms
```

Run your own benchmark before quoting numbers in your resume:

```bash
./scripts/start_cluster.sh
sleep 2
NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
./run/raftkv bench --nodes "$NODES" --n 1000
./scripts/stop_cluster.sh
```

Benchmark numbers depend on laptop, OS, background processes, and whether the cluster is local or Dockerized.
