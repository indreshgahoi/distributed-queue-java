# v0.22.1 Performance Baseline

This is the first repeatable performance baseline for the local queue engine
and queue-node runtime-admission boundary. It is diagnostic evidence, not a
capacity promise or service-level objective.

## Baseline identity

| Item | Value |
|---|---|
| Source revision | `7f44bd4eee89bb0dc4fbf7bbe94e16fa44edd470` (`v0.22.0`) |
| Benchmark code version | `0.22.1-SNAPSHOT` |
| Recorded | 2026-09-02 |
| JMH | 1.37 |
| JVM | Oracle HotSpot 21.0.1+12-LTS-29 |
| OS | Linux 5.15.0-190-generic, x86_64 |
| CPU | Intel Core i7-8700, 1 socket, 6 cores, 12 hardware threads |
| Memory | 15 GiB |
| Repository filesystem | ext4 on `/dev/sda3` |

The machine was not isolated, CPU frequency was not pinned, and other desktop
workloads were present. Compare future results only when the environment and
commands are recorded. Treat small differences as noise until a controlled
experiment establishes otherwise.

## Scenarios

- `publishWithoutDurability` uses a no-op WAL. It isolates queue state-machine,
  locking, identifier, and allocation cost; it does **not** represent a durable
  queue guarantee.
- `publishWithForcedWal` publishes through `SegmentedFileWriteAheadLog`, whose
  append path forces the frame before success.
- `durableReceiveAckCycle` performs receive, ACK, and one replacement publish,
  preserving a steady queue depth of one. One operation represents three
  durable transitions. In the eight-thread run, each worker owns an independent
  queue and WAL directory, so the result measures concurrent filesystem
  pressure rather than contention on one queue.
- `readyQueueLookup` executes `RuntimePartitionManager.withReadyQueue` with a
  no-op callback and either 1 or 1,000 active queues. It measures lookup and
  guarded admission, not HTTP, serialization, or a real queue mutation.

Each result uses one fork, two one-second warm-up iterations, and three
one-second measurement iterations. The short run intentionally keeps this
developer baseline inexpensive; the wide JMH confidence intervals show why it
must not be presented as production sizing data.

## Results

### One-thread latency

| Scenario | Mean | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Durable receive/ACK/publish cycle | 5,672.364 us | 5,398.528 us | 6,868.992 us | 8,901.427 us |
| Publish with forced WAL | 1,881.752 us | 1,810.432 us | 2,262.221 us | 2,609.889 us |
| Publish without durability | 0.519 us | 0.386 us | 0.482 us | 1.680 us |
| READY lookup, 1 active queue | 50.418 ns | 40 ns | 44 ns | 73 ns |
| READY lookup, 1,000 active queues | 50.737 ns | 41 ns | 50 ns | 64 ns |

### Throughput

| Scenario | 1 thread | 8 threads |
|---|---:|---:|
| Durable receive/ACK/publish cycle | 162.437 ops/s | 727.145 ops/s |
| Publish with forced WAL | 489.671 ops/s | Not captured; see limitation below |
| Publish without durability | 2,016,251.752 ops/s | 1,795,715.005 ops/s |
| READY lookup, 1 active queue | 51,217,649.808 ops/s | 15,534,717.552 ops/s |
| READY lookup, 1,000 active queues | 50,887,420.769 ops/s | 19,459,787.825 ops/s |

The 1-versus-1,000 queue lookup comparison is effectively flat in the
single-thread latency run. This supports the intended constant-time serving
index and shows that lookup no longer scans all active queues. The eight-thread
admission result does not scale with thread count because the current manager
uses one node-wide lifecycle monitor. The durable/non-durable gap also shows
that forced storage, not queue bookkeeping, dominates durable publish latency
on this machine.

The eight-thread forced-WAL publish case was deliberately excluded. During the
first combined run, a measurement iteration did not finish in a reasonable
time while workers were blocked in synchronous filesystem flushes. The run was
interrupted and its partial output was not retained. A future storage-focused
suite should add an external timeout, filesystem telemetry, and a controlled
device before drawing a concurrency conclusion.

## Reproduce

Build the executable benchmark artifact from the repository root:

```bash
mvn -pl queue-benchmarks -am clean package -DskipTests
```

Run the same suites:

```bash
java -jar queue-benchmarks/target/benchmarks.jar \
  -t 1 -rf json \
  -rff docs/benchmarks/v0.22.1/sample-1-thread.json

java -jar queue-benchmarks/target/benchmarks.jar \
  -bm thrpt -tu s -t 1 -rf json \
  -rff docs/benchmarks/v0.22.1/throughput-1-thread.json

java -jar queue-benchmarks/target/benchmarks.jar \
  '.*(durableReceiveAckCycle|publishWithoutDurability|readyQueueLookup).*' \
  -bm thrpt -tu s -t 8 -rf json \
  -rff docs/benchmarks/v0.22.1/throughput-8-threads.json
```

JMH overwrites the named result file. Preserve a released baseline before
rerunning it, or write to a new versioned directory.

## Raw evidence

- [`sample-1-thread.json`](sample-1-thread.json)
- [`throughput-1-thread.json`](throughput-1-thread.json)
- [`throughput-8-threads.json`](throughput-8-threads.json)

Raw JMH data is checked in so later optimizations can be compared against the
original distributions and iteration scores, not only a copied summary table.
