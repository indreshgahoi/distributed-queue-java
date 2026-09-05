package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.replication.AppendBatchResult;
import io.github.indreshgahoi.queue.storage.replication.HistoryReclaimedException;
import io.github.indreshgahoi.queue.storage.replication.LogConflictException;
import io.github.indreshgahoi.queue.storage.replication.LogEntry;
import io.github.indreshgahoi.queue.storage.replication.LogGapException;
import io.github.indreshgahoi.queue.storage.replication.LogPoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SegmentedReplicatedLogTest {

    @TempDir
    Path tempDir;

    @Test
    void localBatchAssignsStableConsecutiveIndexesAndSurvivesRestart() {
        StorageLineage lineage = StorageLineage.create();

        try (SegmentedFileWriteAheadLog wal = wal(lineage, 4_096)) {
            AppendBatchResult result = wal.appendLocal(
                    7,
                    List.of(record("one"), record("two"), record("three"))
            );

            assertEquals(1, result.firstIndex());
            assertEquals(3, result.durableThroughIndex());
            assertEquals(3, result.appendedEntries());
            assertEquals(
                    List.of(1L, 2L, 3L),
                    wal.readFrom(1, 10).stream()
                            .map(LogEntry::logIndex)
                            .toList()
            );
            assertEquals(new LogPoint(3, 7), wal.lastLogPoint());
        }

        try (SegmentedFileWriteAheadLog recovered = wal(lineage, 4_096)) {
            assertEquals(new LogPoint(3, 7), recovered.lastLogPoint());
            assertEquals(3, recovered.localDurableIndex());
            assertEquals("two", recovered.entry(2).orElseThrow().record().messageId());
        }
    }

    @Test
    void successfulBatchWritesEveryFrameThenForcesExactlyOnce() {
        StorageLineage lineage = StorageLineage.create();
        int[] writes = {0};
        int[] forces = {0};

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             4_096,
                             (channel, frame) -> {
                                 writes[0]++;
                                 SegmentedFileWriteAheadLog.writeFrame(
                                         channel,
                                         frame
                                 );
                             },
                             (channel, metadata) -> {
                                 forces[0]++;
                                 channel.force(metadata);
                             },
                             lineage
                     )) {
            wal.appendLocal(
                    3,
                    List.of(record("one"), record("two"), record("three"))
            );
        }

        assertEquals(3, writes[0]);
        assertEquals(1, forces[0]);
    }

    @Test
    void failedGroupForcePoisonsWriterWithoutPublishingProgress() {
        StorageLineage lineage = StorageLineage.create();

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             4_096,
                             SegmentedFileWriteAheadLog::writeFrame,
                             (channel, metadata) -> {
                                 throw new IOException("forced failure");
                             },
                             lineage
                     )) {
            assertThrows(
                    WalException.class,
                    () -> wal.appendLocal(3, List.of(record("one")))
            );
            assertEquals(0, wal.localDurableIndex());
            assertThrows(
                    WalException.class,
                    () -> wal.appendLocal(3, List.of(record("two")))
            );
        }
    }

    @Test
    void replicatedBatchTreatsIdenticalPrefixAsRetryAndRejectsConflict() {
        StorageLineage lineage = StorageLineage.create();
        List<LogEntry> initial = List.of(
                new LogEntry(1, 4, record("one")),
                new LogEntry(2, 4, record("two"))
        );

        try (SegmentedFileWriteAheadLog wal = wal(lineage, 4_096)) {
            AppendBatchResult first = wal.appendReplicated(LogPoint.EMPTY, initial);
            AppendBatchResult retry = wal.appendReplicated(LogPoint.EMPTY, initial);
            AppendBatchResult extended = wal.appendReplicated(
                    new LogPoint(1, 4),
                    List.of(
                            initial.get(1),
                            new LogEntry(3, 4, record("three"))
                    )
            );

            assertEquals(2, first.appendedEntries());
            assertEquals(0, retry.appendedEntries());
            assertEquals(2, retry.alreadyPresentEntries());
            assertEquals(1, extended.appendedEntries());
            assertEquals(1, extended.alreadyPresentEntries());

            assertThrows(
                    LogConflictException.class,
                    () -> wal.appendReplicated(
                            new LogPoint(1, 4),
                            List.of(new LogEntry(2, 5, record("different")))
                    )
            );
        }
    }

    @Test
    void replicatedBatchMustStartImmediatelyAfterPreviousPoint() {
        StorageLineage lineage = StorageLineage.create();

        try (SegmentedFileWriteAheadLog wal = wal(lineage, 4_096)) {
            wal.appendLocal(2, List.of(record("one"), record("two")));

            assertThrows(
                    LogGapException.class,
                    () -> wal.appendReplicated(
                            new LogPoint(1, 2),
                            List.of(new LogEntry(3, 2, record("three")))
                    )
            );
            assertEquals(2, wal.localDurableIndex());
        }
    }

    @Test
    void snapshotBoundaryRestoresLogicalIdentityAfterSegmentReclamation()
            throws Exception {
        StorageLineage lineage = StorageLineage.create();

        try (SegmentedFileWriteAheadLog wal = wal(lineage, 80)) {
            wal.appendLocal(3, List.of(record("first-with-large-payload-value")));
            wal.appendLocal(3, List.of(record("second-with-large-payload-value")));
        }

        Files.delete(tempDir.resolve("segment-000000.wal"));

        try (SegmentedFileWriteAheadLog recovered = wal(lineage, 80)) {
            recovered.restoreSnapshotBoundary(new LogPoint(1, 3));

            assertEquals(2, recovered.localDurableIndex());
            assertThrows(
                    HistoryReclaimedException.class,
                    () -> recovered.readFrom(1, 10)
            );

            AppendBatchResult result = recovered.appendReplicated(
                    new LogPoint(2, 3),
                    List.of(new LogEntry(3, 4, record("third")))
            );
            assertEquals(3, result.durableThroughIndex());
        }
    }

    private SegmentedFileWriteAheadLog wal(
            StorageLineage lineage,
            long segmentBytes
    ) {
        return new SegmentedFileWriteAheadLog(tempDir, segmentBytes, lineage);
    }

    private static WalRecord record(String id) {
        return new WalRecord(
                WalRecordType.PUBLISH,
                id,
                "payload-" + id,
                null,
                0,
                Instant.parse("2026-01-01T00:00:00Z")
        );
    }
}
