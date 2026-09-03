package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.wal.InMemoryWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalException;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import io.github.indreshgahoi.queue.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderedFollowerReplicaLogTest {

    private static final StorageLineage LINEAGE =
            new StorageLineage(
                    UUID.fromString(
                            "10000000-0000-0000-0000-000000000001"
                    ),
                    UUID.fromString(
                            "20000000-0000-0000-0000-000000000002"
                    ),
                    0
            );

    @TempDir
    Path tempDirectory;

    @Test
    void appendsOnlyTheNextSequence() {
        InMemoryWriteAheadLog wal =
                new InMemoryWriteAheadLog(LINEAGE);
        OrderedFollowerReplicaLog replica = replica(wal);

        assertEquals(
                ReplicaAppendResult.APPENDED,
                replica.append(entry(4, 1, record("one")))
        );
        assertEquals(1, replica.lastSequence());
        assertEquals(4, replica.highestLeaderEpoch());
        assertEquals(List.of(record("one")), wal.readAll());
    }

    @Test
    void rejectsGapWithoutMutatingWal() {
        InMemoryWriteAheadLog wal =
                new InMemoryWriteAheadLog(LINEAGE);
        OrderedFollowerReplicaLog replica = replica(wal);

        assertThrows(
                ReplicaSequenceException.class,
                () -> replica.append(
                        entry(4, 2, record("two"))
                )
        );
        assertEquals(List.of(), wal.readAll());
        assertEquals(4, replica.highestLeaderEpoch());
    }

    @Test
    void treatsExactRetryAsIdempotent() {
        InMemoryWriteAheadLog wal =
                new InMemoryWriteAheadLog(LINEAGE);
        OrderedFollowerReplicaLog replica = replica(wal);
        WalRecord record = record("one");

        replica.append(entry(4, 1, record));

        assertEquals(
                ReplicaAppendResult.ALREADY_PRESENT,
                replica.append(entry(4, 1, record))
        );
        assertEquals(List.of(record), wal.readAll());
    }

    @Test
    void rejectsDifferentRecordAtCommittedSequence() {
        InMemoryWriteAheadLog wal =
                new InMemoryWriteAheadLog(LINEAGE);
        OrderedFollowerReplicaLog replica = replica(wal);

        replica.append(entry(4, 1, record("one")));

        assertThrows(
                ReplicaConflictException.class,
                () -> replica.append(
                        entry(4, 1, record("different"))
                )
        );
        assertEquals(List.of(record("one")), wal.readAll());
    }

    @Test
    void rejectsWrongLineageAndStaleLeader() {
        OrderedFollowerReplicaLog replica = replica(
                new InMemoryWriteAheadLog(LINEAGE)
        );
        replica.append(entry(8, 1, record("one")));

        assertThrows(
                StaleLeaderEpochException.class,
                () -> replica.append(
                        entry(7, 2, record("two"))
                )
        );

        StorageLineage other = new StorageLineage(
                LINEAGE.queueId(),
                UUID.randomUUID(),
                0
        );
        assertThrows(
                ReplicaLineageMismatchException.class,
                () -> replica.append(
                        new ReplicatedWalEntry(
                                other,
                                8,
                                2,
                                record("two")
                        )
                )
        );
    }

    @Test
    void persistsLeaderEpochAndSequenceAcrossRestart() {
        Path walDirectory = tempDirectory.resolve("wal");
        Path epochPath = tempDirectory.resolve("leader.epoch");

        try (OrderedFollowerReplicaLog first =
                     fileReplica(walDirectory, epochPath)) {
            first.append(entry(8, 1, record("one")));
        }

        try (OrderedFollowerReplicaLog recovered =
                     fileReplica(walDirectory, epochPath)) {
            assertEquals(1, recovered.lastSequence());
            assertEquals(8, recovered.highestLeaderEpoch());
            assertEquals(
                    ReplicaAppendResult.ALREADY_PRESENT,
                    recovered.append(
                            entry(8, 1, record("one"))
                    )
            );
            assertThrows(
                    StaleLeaderEpochException.class,
                    () -> recovered.append(
                            entry(7, 2, record("two"))
                    )
            );
        }
    }

    @Test
    void failsClosedWhenEpochStateIsCorrupt()
            throws Exception {
        Path walDirectory = tempDirectory.resolve("wal");
        Path epochPath = tempDirectory.resolve("leader.epoch");

        try (OrderedFollowerReplicaLog first =
                     fileReplica(walDirectory, epochPath)) {
            first.append(entry(8, 1, record("one")));
        }
        Files.write(epochPath, new byte[]{1, 2, 3});

        SegmentedFileWriteAheadLog wal =
                new SegmentedFileWriteAheadLog(
                        walDirectory,
                        1024,
                        LINEAGE
                );
        assertThrows(
                ReplicaException.class,
                () -> new OrderedFollowerReplicaLog(
                        wal,
                        epochPath
                )
        );
        wal.close();
    }

    @Test
    void storageFailurePoisonsFollower() {
        FailingWriteAheadLog wal =
                new FailingWriteAheadLog(LINEAGE);
        OrderedFollowerReplicaLog replica = new OrderedFollowerReplicaLog(
                wal,
                new MemoryEpochStore()
        );

        assertThrows(
                WalException.class,
                () -> replica.append(
                        entry(3, 1, record("one"))
                )
        );
        assertThrows(
                ReplicaException.class,
                () -> replica.append(
                        entry(3, 1, record("one"))
                )
        );
    }

    @Test
    void epochPersistenceFailurePoisonsFollowerBeforeWalAppend() {
        InMemoryWriteAheadLog wal =
                new InMemoryWriteAheadLog(LINEAGE);
        OrderedFollowerReplicaLog replica = new OrderedFollowerReplicaLog(
                wal,
                new FailingEpochStore()
        );

        assertThrows(
                ReplicaException.class,
                () -> replica.append(
                        entry(3, 1, record("one"))
                )
        );
        assertEquals(List.of(), wal.readAll());
        assertThrows(
                ReplicaException.class,
                () -> replica.append(
                        entry(3, 1, record("one"))
                )
        );
    }

    @Test
    void batchReportsNewAndAlreadyPresentEntries() {
        InMemoryWriteAheadLog wal =
                new InMemoryWriteAheadLog(LINEAGE);
        OrderedFollowerReplicaLog replica = replica(wal);
        replica.append(entry(3, 1, record("one")));

        ReplicaBatchAppendResult result = replica.appendBatch(List.of(
                entry(3, 1, record("one")),
                entry(3, 2, record("two")),
                entry(3, 3, record("three"))
        ));

        assertEquals(3, result.acceptedThroughSequence());
        assertEquals(2, result.appendedEntries());
        assertEquals(1, result.alreadyPresentEntries());
        assertEquals(3, wal.readAll().size());
    }

    private OrderedFollowerReplicaLog replica(
            WriteAheadLog wal
    ) {
        return new OrderedFollowerReplicaLog(
                wal,
                new MemoryEpochStore()
        );
    }

    private OrderedFollowerReplicaLog fileReplica(
            Path walDirectory,
            Path epochPath
    ) {
        return new OrderedFollowerReplicaLog(
                new SegmentedFileWriteAheadLog(
                        walDirectory,
                        1024,
                        LINEAGE
                ),
                epochPath
        );
    }

    private static ReplicatedWalEntry entry(
            long epoch,
            long sequence,
            WalRecord record
    ) {
        return new ReplicatedWalEntry(
                LINEAGE,
                epoch,
                sequence,
                record
        );
    }

    private static WalRecord record(String payload) {
        return new WalRecord(
                WalRecordType.PUBLISH,
                "message-" + payload,
                payload,
                null,
                0,
                Instant.parse("2026-09-02T00:00:00Z")
        );
    }

    private static final class MemoryEpochStore
            implements LeaderEpochStore {

        private long epoch;

        @Override
        public long load() {
            return epoch;
        }

        @Override
        public void save(long leaderEpoch) {
            epoch = leaderEpoch;
        }
    }

    private static final class FailingWriteAheadLog
            implements WriteAheadLog {

        private final StorageLineage lineage;
        private final List<WalRecord> records =
                new ArrayList<>();

        private FailingWriteAheadLog(
                StorageLineage lineage
        ) {
            this.lineage = lineage;
        }

        @Override
        public void append(WalRecord record) {
            throw new WalException("injected failure");
        }

        @Override
        public List<WalRecord> readAll() {
            return List.copyOf(records);
        }

        @Override
        public WalPosition currentDurablePosition() {
            return new WalPosition(0, 0);
        }

        @Override
        public List<WalRecord> readFrom(
                WalPosition position
        ) {
            return List.of();
        }

        @Override
        public StorageLineage storageLineage() {
            return lineage;
        }

        @Override
        public void close() {
        }
    }

    private static final class FailingEpochStore
            implements LeaderEpochStore {

        @Override
        public long load() {
            return 0;
        }

        @Override
        public void save(long leaderEpoch) {
            throw new ReplicaException("injected failure");
        }
    }
}
