package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatch;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.replication.FollowerReplicaLog;
import io.github.indreshgahoi.queue.storage.replication.OrderedFollowerReplicaLog;
import io.github.indreshgahoi.queue.storage.wal.InMemoryWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FollowerReplicationServiceTest {

    private final StorageLineage lineage = new StorageLineage(
            UUID.randomUUID(),
            UUID.randomUUID(),
            0
    );

    @TempDir
    Path tempDirectory;

    @Test
    void ambiguousBatchRetryDoesNotDuplicateWalRecords() {
        InMemoryWriteAheadLog wal =
                new InMemoryWriteAheadLog(lineage);
        FollowerReplicaLog replica = new OrderedFollowerReplicaLog(
                wal,
                tempDirectory.resolve("epoch.bin")
        );
        FollowerReplicationService service =
                new FollowerReplicationService(ignored -> replica);
        ReplicaWalBatch batch = new ReplicaWalBatch(
                lineage,
                7,
                1,
                List.of(record("one"), record("two"))
        );

        ReplicaWalBatchResult first = service.replicate(batch);
        ReplicaWalBatchResult retry = service.replicate(batch);

        assertEquals(2, first.appendedEntries());
        assertEquals(0, first.alreadyPresentEntries());
        assertEquals(0, retry.appendedEntries());
        assertEquals(2, retry.alreadyPresentEntries());
        assertEquals(2, wal.readAll().size());
    }

    @Test
    void rejectsUnboundedBatchBeforeOpeningStorage() {
        boolean[] opened = {false};
        FollowerReplicationService service =
                new FollowerReplicationService(ignored -> {
                    opened[0] = true;
                    throw new AssertionError("must not open storage");
                });
        List<WalRecord> records = java.util.stream.IntStream.rangeClosed(
                        1,
                        FollowerReplicationService.MAX_BATCH_ENTRIES + 1
                )
                .mapToObj(index -> record("record-" + index))
                .toList();

        assertThrows(
                IllegalArgumentException.class,
                () -> service.replicate(new ReplicaWalBatch(
                        lineage,
                        7,
                        1,
                        records
                ))
        );
        assertEquals(false, opened[0]);
    }

    private WalRecord record(String payload) {
        return new WalRecord(
                WalRecordType.PUBLISH,
                "message-" + payload,
                payload,
                null,
                0,
                Instant.parse("2026-09-02T00:00:00Z")
        );
    }
}
