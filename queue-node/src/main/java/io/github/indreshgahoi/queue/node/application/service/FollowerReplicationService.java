package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.in.FollowerReplicationUseCase;
import io.github.indreshgahoi.queue.node.application.port.out.FollowerReplicaLogProvider;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatch;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;
import io.github.indreshgahoi.queue.storage.replication.ReplicaBatchAppendResult;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
public final class FollowerReplicationService
        implements FollowerReplicationUseCase {

    public static final int MAX_BATCH_ENTRIES = 256;
    public static final int MAX_BATCH_PAYLOAD_BYTES = 1024 * 1024;

    private final FollowerReplicaLogProvider logs;

    public FollowerReplicationService(
            FollowerReplicaLogProvider logs
    ) {
        this.logs = Objects.requireNonNull(logs, "logs");
    }

    @Override
    public ReplicaWalBatchResult replicate(
            ReplicaWalBatch batch
    ) {
        Objects.requireNonNull(batch, "batch");
        validateBounded(batch);

        ReplicaBatchAppendResult result = logs.open(batch.lineage())
                .appendBatch(batch.entries());
        log.debug(
                "event=follower_replica_batch_applied queueId={} "
                        + "generationId={} partitionId={} leaderEpoch={} "
                        + "firstSequence={} acceptedThroughSequence={} "
                        + "appendedEntries={} alreadyPresentEntries={}",
                batch.lineage().queueId(),
                batch.lineage().generationId(),
                batch.lineage().partitionId(),
                batch.leaderEpoch(),
                batch.firstSequence(),
                result.acceptedThroughSequence(),
                result.appendedEntries(),
                result.alreadyPresentEntries()
        );
        return new ReplicaWalBatchResult(
                result.acceptedThroughSequence(),
                result.appendedEntries(),
                result.alreadyPresentEntries()
        );
    }

    private void validateBounded(ReplicaWalBatch batch) {
        if (batch.records().size() > MAX_BATCH_ENTRIES) {
            throw new IllegalArgumentException(
                    "replica batch exceeds " + MAX_BATCH_ENTRIES
                            + " entries"
            );
        }

        batch.records().forEach(record -> {
            if (record == null
                    || record.type() == null
                    || record.messageId() == null
                    || record.timestamp() == null) {
                throw new IllegalArgumentException(
                        "replica WAL record is missing a required field"
                );
            }
            if (record.attempt() < 0) {
                throw new IllegalArgumentException(
                        "record.attempt must not be negative"
                );
            }
        });

        long payloadBytes = batch.records().stream()
                .mapToLong(record -> utf8Length(record.payload()))
                .sum();
        if (payloadBytes > MAX_BATCH_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "replica batch payload exceeds "
                            + MAX_BATCH_PAYLOAD_BYTES + " bytes"
            );
        }
    }

    private long utf8Length(String value) {
        return value == null
                ? 0
                : value.getBytes(StandardCharsets.UTF_8).length;
    }
}
