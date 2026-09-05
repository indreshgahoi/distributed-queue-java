# v0.28.0 Production WAL Batch Benchmark

This benchmark compares the production segmented WAL when a caller appends
each record durably on its own with the v0.28 batch path, which writes every
frame in the batch and performs one force before reporting success.

## Environment

- JMH 1.37
- Java 21.0.1
- one benchmark thread and one fork
- one 500 ms warm-up iteration
- two 500 ms measurement iterations
- sample-time mode

The run is intentionally short enough for routine milestone verification. The
numbers establish direction and order of magnitude; they are not hardware
capacity claims or an SLA.

## Results

Lower mean latency is better.

| Batch size | Force per record | One force per batch | Approximate improvement |
|---:|---:|---:|---:|
| 1 | 6.57 ms | 6.72 ms | no material change |
| 8 | 53.46 ms | 7.07 ms | 7.6x |
| 32 | 206.83 ms | 11.36 ms | 18.2x |
| 128 | 860.88 ms | 10.01 ms | 86.0x |
| 256 | 1,655.70 ms | 13.39 ms | 123.7x |

## Interpretation

The comparison confirms that the previous cost scaled primarily with the
number of filesystem force operations. The v0.28 path amortizes that cost
across the batch while retaining the required durability boundary: a batch is
not reported durable until its single force succeeds.

The test does not model producer wait time, adaptive batch formation,
concurrent producers, replication network latency, or quorum acknowledgement.
Those belong to later end-to-end benchmarks after the corresponding runtime
features exist.

## Reproduction

Build the benchmark module, then run:

```bash
java -jar queue-benchmarks/target/benchmarks.jar \
  '.*BatchForcePrototypeBenchmark.production.*' \
  -t 1 -wi 1 -w 500ms -i 2 -r 500ms -f 1 \
  -rf json \
  -rff docs/benchmarks/v0.28.0/production-batch.json
```

The complete machine-readable output is in
[`production-batch.json`](production-batch.json).
