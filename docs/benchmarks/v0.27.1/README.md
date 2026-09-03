# v0.27.1 Replication Performance Baseline

This directory records a decision baseline, not a production capacity claim.
The runs are intentionally short enough to be repeatable during development;
release or hardware qualification requires longer runs on dedicated storage.

## Environment

- Commit under test: `c4d1633fe2469de05e910f3c2c8ced7b525fded4`
- OS: Ubuntu Linux, kernel `5.15.0-190-generic`
- CPU: Intel Core i7-8700, 6 cores / 12 hardware threads
- JDK: Oracle HotSpot 21.0.1
- Filesystem: ext4 on `/dev/sda3`
- JMH: 1.37, one fork, one 500 ms warmup, two 500 ms measurements

The filesystem reports a local block device, but the exact device cache and
power-loss guarantees were not independently verified. Absolute latency must
therefore not be generalized to other machines.

## Main result

The current follower batch is a transport batch, not a durability batch. Its
latency grows with record count because `OrderedFollowerReplicaLog` invokes the
production WAL append once per entry and each append calls `force(true)`.

| Batch | Current follower mean | Production force/record | One-force prototype |
|---:|---:|---:|---:|
| 1 | 6.94 ms | 6.22 ms | 6.14 ms |
| 8 | 53.86 ms | 51.96 ms | 8.66 ms |
| 32 | 209.67 ms | 189.81 ms | 7.17 ms |
| 128 | 860.88 ms | 846.20 ms | 10.18 ms |
| 256 | 1,689.26 ms | 1,594.88 ms | 10.27 ms |

The prototype exists only in the benchmark module. It writes a complete frame
set before one force and does not alter production code or claim crash-safe
batch semantics. The result is strong evidence that v0.28 should expose an
atomic durable batch append boundary, with failure poisoning and torn-tail
tests defined before implementation.

At batch size 256, measured record throughput was approximately 153 records/s
for the current follower and 26,854 records/s for the one-force prototype.
These short-run values are directional; the important result is that current
record throughput remains roughly flat as transport batches grow because force
count is unchanged.

## Other observations

- A forced 1 KiB WAL append averaged 6.57 ms. Increasing payload to 256 KiB
  raised the mean to 13.94 ms; force latency dominates small records.
- With a deliberately tiny 4 KiB segment target, median append remained 6.49
  ms but p95 rose to 20.38 ms. Rotation is visible in tail latency.
- During snapshot creation, foreground publish measured 6.53 ms p50, 7.09 ms
  p95, 13.40 ms p99, and 14.83 ms maximum. Snapshot samples are sparse and
  must be rerun with realistic state sizes before drawing a capacity limit.
- Opening 128 idle partition WALs took 1,133 ms; opening and forcing one record
  in each took 1,784 ms. Startup/reconciliation must remain bounded and staged.
- A shared durable queue with four producer threads had a 6.02 ms p50 but a
  142.70 ms p95 and noisy multi-second outliers. The run demonstrates lock plus
  force queueing risk, not a stable tail-latency number.
- Encoding a 256-record HTTP request measured 0.41 ms p50; decoding measured
  0.52 ms p50. On this host, JSON work is orders of magnitude below the current
  1.69-second durable follower batch and is not the first optimization target.

## Matrix boundaries

Snapshot installation is not implemented in v0.27, so it cannot be measured
without inventing the next protocol. End-to-end network-plus-disk measurement
also needs replica membership and a representative long-running topology;
localhost JMH would conflate Spring startup, scheduler, socket, and disk costs.
Both become mandatory integration profiles when those capabilities land.
These dependency-bound omissions are explicit and do not weaken the evidence
selecting the v0.28 storage boundary.

## Raw results

- `storage-baseline.json`: forced append, current follower batching, and the
  benchmark-only one-force comparison.
- `batch-throughput.json`: batch operations and derived record-throughput input.
- `http-serialization.json`: replica request encoding and decoding without I/O.
- `lifecycle-density.json`: forced rotation, snapshot contention, and physical
  partition startup density.
- `producers-1-thread.json` and `producers-4-threads.json`: durable publish
  contention comparison.

## Reproduce

```bash
JAVA_HOME=/usr/lib/jvm/jdk-21-oracle-x64 \
  mvn -pl queue-benchmarks -am clean package -DskipTests

JAVA_HOME=/usr/lib/jvm/jdk-21-oracle-x64 \
  java -jar queue-benchmarks/target/benchmarks.jar \
  '.*(WalDurabilityBenchmark|BatchForcePrototypeBenchmark).*' \
  -t 1 -wi 1 -w 500ms -i 2 -r 500ms -f 1 -rf json \
  -rff docs/benchmarks/v0.27.1/storage-baseline.json
```

Use the annotation defaults (longer warmup and measurement) for a validation
run, and record a new environment section rather than overwriting this result.
