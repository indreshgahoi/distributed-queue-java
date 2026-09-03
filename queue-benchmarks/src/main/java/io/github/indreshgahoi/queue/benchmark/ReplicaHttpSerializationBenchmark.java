package io.github.indreshgahoi.queue.benchmark;

import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ReplicaHttpSerializationBenchmark {

    @Benchmark
    public void serializeRequest(
            SerializationState state,
            Blackhole blackhole
    ) throws Exception {
        blackhole.consume(state.mapper.writeValueAsBytes(state.request));
    }

    @Benchmark
    public void deserializeRequest(
            SerializationState state,
            Blackhole blackhole
    ) throws Exception {
        blackhole.consume(state.mapper.readValue(
                state.encodedRequest,
                ReplicaRequest.class
        ));
    }

    @State(Scope.Thread)
    public static class SerializationState {
        @Param({"1", "8", "32", "128", "256"})
        private int batchSize;

        private ObjectMapper mapper;
        private ReplicaRequest request;
        private byte[] encodedRequest;

        @Setup
        public void setUp() throws Exception {
            mapper = JsonMapper.builder()
                    .findAndAddModules()
                    .build();
            WalRecord record = new WalRecord(
                    WalRecordType.PUBLISH,
                    "serialization-message",
                    "x".repeat(1024),
                    null,
                    1,
                    Instant.parse("2026-09-03T00:00:00Z")
            );
            request = new ReplicaRequest(
                    1,
                    1,
                    java.util.Collections.nCopies(batchSize, record)
            );
            encodedRequest = mapper.writeValueAsBytes(request);
        }
    }

    /** Mirrors the wire shape owned by the follower HTTP endpoint. */
    public record ReplicaRequest(
            long leaderEpoch,
            long firstSequence,
            List<WalRecord> records
    ) {
    }
}
