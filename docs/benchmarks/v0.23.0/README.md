# v0.23.0 Per-Partition Admission Benchmark

This focused benchmark validates the isolation property introduced by
per-partition runtime handles. It is diagnostic evidence, not a service-level
objective or production capacity claim.

## Baseline identity

| Item | Value |
|---|---|
| Parent revision | `b836e90` (`v0.22.1`) |
| Benchmark code | v0.23.0 milestone worktree |
| Recorded | 2026-09-02 |
| JMH | 1.37 |
| JVM | Oracle HotSpot 21.0.1+12-LTS-29 |
| OS | Linux 5.15.0-190-generic, x86_64 |
| CPU | Intel Core i7-8700, 1 socket, 6 cores, 12 hardware threads |
| Memory | 15 GiB |

The environment matches the developer machine used for the v0.22.1 baseline,
but it was not isolated and CPU frequency was not pinned.

## Experimental design

`readyQueueLookup` includes concurrent-map lookup, handle-local permit
acquisition and release, registration validation, and a no-op callback. It does
not include HTTP, JSON, or queue/WAL work.

At one active queue, every worker targets the same handle. At 1,000 active
queues, workers select different queue IDs using their JMH thread index. The
eight-thread comparison therefore tests same-partition contention against
cross-partition independence.

This routing method is intentionally stronger than the v0.22.1 benchmark,
where all workers targeted one queue for both topology sizes. Do not treat the
two version directories as a strict before/after score comparison. The v0.23
result should be read as an internal control: same implementation and run,
different contention topology.

Each result uses one fork, two one-second warm-up iterations, and three
one-second measurement iterations.

## Results

| Workers | Active queues | Work distribution | Throughput |
|---:|---:|---|---:|
| 1 | 1 | one handle | 25,510,148.169 ops/s |
| 1 | 1,000 | one selected handle | 26,202,003.496 ops/s |
| 8 | 1 | all workers share one handle | 16,051,255.183 ops/s |
| 8 | 1,000 | workers use independent handles | 162,293,548.452 ops/s |

The one-thread results remain effectively flat as active queue count grows,
preserving constant-time lookup. With eight workers, independent handles
produce approximately 10.1 times the throughput of one shared handle. This is
consistent with the intended removal of node-wide admission serialization.
The deterministic concurrency test remains the correctness evidence; these
numbers only characterize this machine and workload.

## Reproduce

```bash
mvn -pl queue-benchmarks -am clean package -DskipTests

java -jar queue-benchmarks/target/benchmarks.jar \
  RuntimeAdmissionBenchmark.readyQueueLookup \
  -bm thrpt -tu s -t 1 -rf json \
  -rff docs/benchmarks/v0.23.0/admission-throughput-1-thread.json

java -jar queue-benchmarks/target/benchmarks.jar \
  RuntimeAdmissionBenchmark.readyQueueLookup \
  -bm thrpt -tu s -t 8 -rf json \
  -rff docs/benchmarks/v0.23.0/admission-throughput-8-threads.json
```

## Raw evidence

- [`admission-throughput-1-thread.json`](admission-throughput-1-thread.json)
- [`admission-throughput-8-threads.json`](admission-throughput-8-threads.json)
