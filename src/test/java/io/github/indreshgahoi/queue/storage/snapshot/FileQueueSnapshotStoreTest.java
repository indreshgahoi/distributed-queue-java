package io.github.indreshgahoi.queue.storage.snapshot;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FileQueueSnapshotStoreTest {

    private static final StorageLineage LINEAGE =
            StorageLineage.create();

    @TempDir
    Path tempDir;

    @Test
    void saveForcesParentDirectoryAfterSnapshotPromotion() {
        Path snapshotPath = tempDir.resolve("queue.snapshot");
        boolean[] promoted = {false};
        boolean[] directoryForced = {false};

        FileQueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath,
                        (candidate, data) -> { },
                        candidate -> { },
                        (candidate, destination) ->
                                promoted[0] = true,
                        publishedPath -> {
                            assertTrue(promoted[0]);
                            assertEquals(snapshotPath, publishedPath);
                            directoryForced[0] = true;
                        }
                );

        store.save(snapshot(new WalPosition(0, 8), "A"));

        assertTrue(directoryForced[0]);
    }

    @Test
    void directoryForceFailureDoesNotReportSnapshotCommitSuccess() {
        Path snapshotPath = tempDir.resolve("queue.snapshot");

        FileQueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath,
                        FileQueueSnapshotStore::writeCandidate,
                        FileQueueSnapshotStore::forceCandidate,
                        FileQueueSnapshotStore::promoteCandidate,
                        publishedPath -> {
                            throw new IOException(
                                    "simulated directory force failure"
                            );
                        }
                );

        assertThrows(
                SnapshotException.class,
                () -> store.save(
                        snapshot(new WalPosition(0, 8), "A")
                )
        );

        assertTrue(
                Files.exists(snapshotPath),
                "promotion happened before directory durability failed"
        );
    }

    @Test
    void saveThenLoadReturnsEquivalentSnapshot() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot expected =
                completeSnapshot(
                        new WalPosition(0, 12_480)
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(expected);

        QueueSnapshot actual =
                store.loadLatest()
                        .orElseThrow();

        assertEquals(
                expected,
                actual
        );
    }

    @Test
    void snapshotSurvivesStoreReopen() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot expected =
                completeSnapshot(
                        new WalPosition(0, 500)
                );

        /*
         * First store instance.
         */
        QueueSnapshotStore firstStore =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        firstStore.save(expected);

        /*
         * New object simulates another process lifetime.
         */
        QueueSnapshotStore reopened =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        assertEquals(
                expected,
                reopened.loadLatest()
                        .orElseThrow()
        );
    }

    @Test
    void loadLatestReturnsEmptyWhenNoSnapshotExists() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        assertTrue(
                store.loadLatest().isEmpty()
        );
    }

    @Test
    void snapshotPreservesWalPosition() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        WalPosition position =
                new WalPosition(
                        7,
                        98_765
                );

        QueueSnapshot snapshot =
                completeSnapshot(position);

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(snapshot);

        QueueSnapshot recovered =
                store.loadLatest()
                        .orElseThrow();

        assertEquals(
                position,
                recovered.walPosition()
        );
    }

    @Test
    void snapshotPreservesReadyEntries() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot snapshot =
                new QueueSnapshot(
                        LINEAGE,
                        new WalPosition(0, 100),
                        List.of(
                                new ReadySnapshotEntry(
                                        "m1",
                                        "A",
                                        1
                                ),
                                new ReadySnapshotEntry(
                                        "m2",
                                        "B",
                                        3
                                )
                        ),
                        List.of(),
                        List.of(),
                        List.of()
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(snapshot);

        QueueSnapshot recovered =
                store.loadLatest()
                        .orElseThrow();

        assertEquals(
                snapshot.ready(),
                recovered.ready()
        );
    }

    @Test
    void snapshotPreservesInFlightLeaseMetadata() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        Instant leaseUntil =
                Instant.parse(
                        "2026-08-23T10:30:00Z"
                );

        QueueSnapshot snapshot =
                new QueueSnapshot(
                        LINEAGE,
                        new WalPosition(0, 200),
                        List.of(),
                        List.of(
                                new InFlightSnapshotEntry(
                                        "m1",
                                        "A",
                                        "receipt-123",
                                        2,
                                        leaseUntil
                                )
                        ),
                        List.of(),
                        List.of()
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(snapshot);

        InFlightSnapshotEntry recovered =
                store.loadLatest()
                        .orElseThrow()
                        .inFlight()
                        .getFirst();

        assertEquals(
                "m1",
                recovered.messageId()
        );

        assertEquals(
                "receipt-123",
                recovered.receiptHandle()
        );

        assertEquals(
                2,
                recovered.attempt()
        );

        assertEquals(
                leaseUntil,
                recovered.leaseUntil()
        );
    }

    @Test
    void snapshotPreservesDelayedRetryTime() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        Instant retryAt =
                Instant.parse(
                        "2026-08-23T11:00:00Z"
                );

        QueueSnapshot snapshot =
                new QueueSnapshot(
                        LINEAGE,
                        new WalPosition(0, 300),
                        List.of(),
                        List.of(),
                        List.of(
                                new DelayedSnapshotEntry(
                                        "m1",
                                        "A",
                                        3,
                                        retryAt
                                )
                        ),
                        List.of()
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(snapshot);

        DelayedSnapshotEntry recovered =
                store.loadLatest()
                        .orElseThrow()
                        .delayed()
                        .getFirst();

        assertEquals(
                3,
                recovered.nextAttempt()
        );

        assertEquals(
                retryAt,
                recovered.retryAt()
        );
    }

    @Test
    void snapshotPreservesDeadLetterEntries() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot snapshot =
                new QueueSnapshot(
                        LINEAGE,
                        new WalPosition(0, 400),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(
                                new DeadLetterSnapshotEntry(
                                        "m1",
                                        "poison"
                                )
                        )
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(snapshot);

        assertEquals(
                snapshot.deadLetters(),
                store.loadLatest()
                        .orElseThrow()
                        .deadLetters()
        );
    }

    @Test
    void newerSnapshotReplacesOlderSnapshot() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot first =
                new QueueSnapshot(
                        LINEAGE,
                        new WalPosition(0, 100),
                        List.of(
                                new ReadySnapshotEntry(
                                        "m1",
                                        "A",
                                        1
                                )
                        ),
                        List.of(),
                        List.of(),
                        List.of()
                );

        QueueSnapshot second =
                new QueueSnapshot(
                        LINEAGE,
                        new WalPosition(0, 200),
                        List.of(
                                new ReadySnapshotEntry(
                                        "m2",
                                        "B",
                                        1
                                )
                        ),
                        List.of(),
                        List.of(),
                        List.of()
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(first);
        store.save(second);

        QueueSnapshot recovered =
                store.loadLatest()
                        .orElseThrow();

        assertEquals(
                second,
                recovered
        );

        assertNotEquals(
                first,
                recovered
        );
    }

    @Test
    void corruptedSnapshotPayloadIsRejected()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot snapshot =
                completeSnapshot(
                        new WalPosition(0, 500)
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(snapshot);

        /*
         * Layout:
         *
         * magic         4
         * version       4
         * payloadLength 4
         * payload       N
         * checksum      4
         *
         * Change one payload byte without updating CRC.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             snapshotPath,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE
                     )) {

            channel.position(
                    Integer.BYTES * 3L
            );

            ByteBuffer oneByte =
                    ByteBuffer.allocate(1);

            assertEquals(
                    1,
                    channel.read(oneByte)
            );

            oneByte.flip();

            byte original =
                    oneByte.get();

            channel.position(
                    Integer.BYTES * 3L
            );

            ByteBuffer corrupted =
                    ByteBuffer.wrap(
                            new byte[]{
                                    (byte) (original ^ 0x01)
                            }
                    );

            while (corrupted.hasRemaining()) {
                channel.write(corrupted);
            }
        }

        assertThrows(
                SnapshotException.class,
                store::loadLatest
        );
    }

    @Test
    void corruptedSnapshotChecksumIsRejected()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(
                completeSnapshot(
                        new WalPosition(0, 600)
                )
        );

        try (FileChannel channel =
                     FileChannel.open(
                             snapshotPath,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer prefix =
                    ByteBuffer.allocate(
                            Integer.BYTES * 3
                    );

            readFully(
                    channel,
                    prefix
            );

            prefix.flip();

            prefix.getInt(); // magic
            prefix.getInt(); // version

            int payloadLength =
                    prefix.getInt();

            long checksumPosition =
                    Integer.BYTES * 3L
                            + payloadLength;

            channel.position(
                    checksumPosition
            );

            ByteBuffer checksum =
                    ByteBuffer.allocate(
                            Integer.BYTES
                    );

            readFully(
                    channel,
                    checksum
            );

            checksum.flip();

            int originalChecksum =
                    checksum.getInt();

            channel.position(
                    checksumPosition
            );

            ByteBuffer replacement =
                    ByteBuffer.allocate(
                            Integer.BYTES
                    );

            replacement.putInt(
                    originalChecksum ^ 0x01
            );

            replacement.flip();

            while (replacement.hasRemaining()) {
                channel.write(replacement);
            }
        }

        assertThrows(
                SnapshotException.class,
                store::loadLatest
        );
    }

    @Test
    void invalidSnapshotMagicIsRejected()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(
                completeSnapshot(
                        new WalPosition(0, 700)
                )
        );

        /*
         * Overwrite magic.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             snapshotPath,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer invalidMagic =
                    ByteBuffer.allocate(
                            Integer.BYTES
                    );

            invalidMagic.putInt(
                    0x12345678
            );

            invalidMagic.flip();

            while (invalidMagic.hasRemaining()) {
                channel.write(
                        invalidMagic
                );
            }
        }

        assertThrows(
                SnapshotException.class,
                store::loadLatest
        );
    }

    @Test
    void unsupportedSnapshotVersionIsRejected()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(
                completeSnapshot(
                        new WalPosition(0, 800)
                )
        );

        /*
         * version begins immediately after magic.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             snapshotPath,
                             StandardOpenOption.WRITE
                     )) {

            channel.position(
                    Integer.BYTES
            );

            ByteBuffer unsupported =
                    ByteBuffer.allocate(
                            Integer.BYTES
                    );

            unsupported.putInt(999);
            unsupported.flip();

            while (unsupported.hasRemaining()) {
                channel.write(unsupported);
            }
        }

        assertThrows(
                SnapshotException.class,
                store::loadLatest
        );
    }

    @Test
    void truncatedSnapshotIsRejected()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(
                completeSnapshot(
                        new WalPosition(0, 900)
                )
        );

        long originalSize =
                Files.size(snapshotPath);

        /*
         * Remove some bytes from the end,
         * leaving checksum or payload incomplete.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             snapshotPath,
                             StandardOpenOption.WRITE
                     )) {

            channel.truncate(
                    originalSize - 2
            );
        }

        assertThrows(
                SnapshotException.class,
                store::loadLatest
        );
    }

    @Test
    void snapshotWithUnexpectedTrailingBytesIsRejected()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(
                completeSnapshot(
                        new WalPosition(0, 1_000)
                )
        );

        try (FileChannel channel =
                     FileChannel.open(
                             snapshotPath,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.APPEND
                     )) {

            channel.write(
                    ByteBuffer.wrap(
                            new byte[]{
                                    1, 2, 3
                            }
                    )
            );
        }

        assertThrows(
                SnapshotException.class,
                store::loadLatest
        );
    }

    @Test
    void failedCandidateSnapshotDoesNotDestroyPreviousValidSnapshot()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot original =
                completeSnapshot(
                        new WalPosition(0, 1_100)
                );

        QueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(original);

        byte[] validSnapshotBytes =
                Files.readAllBytes(
                        snapshotPath
                );

        /*
         * Simulate an incomplete candidate file.
         *
         * The important part is that snapshotPath
         * remains untouched.
         */
        Path tempPath =
                snapshotPath.resolveSibling(
                        snapshotPath.getFileName()
                                + ".tmp"
                );

        Files.write(
                tempPath,
                new byte[]{
                        1, 2, 3, 4
                }
        );

        /*
         * Existing snapshot must remain valid.
         */
        assertEquals(
                original,
                store.loadLatest()
                        .orElseThrow()
        );

        assertArrayEquals(
                validSnapshotBytes,
                Files.readAllBytes(
                        snapshotPath
                )
        );
    }

    //---------------------------------------------------------------------
    //            Test cases for crash safe snapshot replacement
    //------------------------------------------------------------------------
    @Test
    void candidateWriteFailurePreservesPreviousSnapshot() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot first =
                snapshot(
                        new WalPosition(0, 100),
                        "first"
                );

        QueueSnapshot second =
                snapshot(
                        new WalPosition(0, 200),
                        "second"
                );

        /*
         * First create S1 successfully using normal store.
         */
        new FileQueueSnapshotStore(
                snapshotPath
        ).save(first);

        /*
         * New store simulates failure while writing S2 candidate.
         */
        FileQueueSnapshotStore failingStore =
                new FileQueueSnapshotStore(
                        snapshotPath,
                        (candidate, data) -> {
                            /*
                             * Write some bytes so this really models
                             * partial candidate creation.
                             */
                            try (FileChannel channel =
                                         FileChannel.open(
                                                 candidate,
                                                 StandardOpenOption.CREATE,
                                                 StandardOpenOption.WRITE,
                                                 StandardOpenOption.TRUNCATE_EXISTING
                                         )) {

                                int bytesToWrite =
                                        Math.min(
                                                8,
                                                data.remaining()
                                        );

                                int originalLimit =
                                        data.limit();

                                data.limit(
                                        data.position()
                                                + bytesToWrite
                                );

                                while (data.hasRemaining()) {
                                    channel.write(data);
                                }

                                data.limit(originalLimit);
                            }

                            throw new IOException(
                                    "Simulated candidate write failure"
                            );
                        },
                        FileQueueSnapshotStore::forceCandidate,
                        FileQueueSnapshotStore::promoteCandidate
                );

        assertThrows(
                SnapshotException.class,
                () -> failingStore.save(second)
        );

        /*
         * S1 must still be authoritative.
         */
        QueueSnapshot recovered =
                new FileQueueSnapshotStore(
                        snapshotPath
                )
                        .loadLatest()
                        .orElseThrow();

        assertEquals(
                first,
                recovered
        );
    }


    @Test
    void candidateForceFailurePreservesPreviousSnapshot() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot first =
                snapshot(
                        new WalPosition(0, 100),
                        "first"
                );

        QueueSnapshot second =
                snapshot(
                        new WalPosition(0, 200),
                        "second"
                );

        new FileQueueSnapshotStore(
                snapshotPath
        ).save(first);

        FileQueueSnapshotStore failingStore =
                new FileQueueSnapshotStore(
                        snapshotPath,

                        /*
                         * Candidate bytes are written successfully.
                         */
                        FileQueueSnapshotStore::writeCandidate,

                        /*
                         * But durability establishment fails.
                         */
                        candidate -> {
                            throw new IOException(
                                    "Simulated force failure"
                            );
                        },

                        FileQueueSnapshotStore::promoteCandidate
                );

        assertThrows(
                SnapshotException.class,
                () -> failingStore.save(second)
        );

        QueueSnapshot recovered =
                new FileQueueSnapshotStore(
                        snapshotPath
                )
                        .loadLatest()
                        .orElseThrow();

        assertEquals(
                first,
                recovered
        );
    }

    @Test
    void promotionFailurePreservesPreviousSnapshot() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot first =
                snapshot(
                        new WalPosition(0, 100),
                        "first"
                );

        QueueSnapshot second =
                snapshot(
                        new WalPosition(0, 200),
                        "second"
                );

        new FileQueueSnapshotStore(
                snapshotPath
        ).save(first);

        FileQueueSnapshotStore failingStore =
                new FileQueueSnapshotStore(
                        snapshotPath,
                        FileQueueSnapshotStore::writeCandidate,
                        FileQueueSnapshotStore::forceCandidate,

                        /*
                         * Candidate is fully written and forced,
                         * but publication fails.
                         */
                        (candidate, destination) -> {
                            throw new IOException(
                                    "Simulated promotion failure"
                            );
                        }
                );

        assertThrows(
                SnapshotException.class,
                () -> failingStore.save(second)
        );

        QueueSnapshot recovered =
                new FileQueueSnapshotStore(
                        snapshotPath
                )
                        .loadLatest()
                        .orElseThrow();

        assertEquals(
                first,
                recovered
        );
    }

    @Test
    void failedSaveDoesNotReportNewSnapshotAsLatest() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot first =
                snapshot(
                        new WalPosition(0, 100),
                        "first"
                );

        QueueSnapshot second =
                snapshot(
                        new WalPosition(0, 200),
                        "second"
                );

        FileQueueSnapshotStore normalStore =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        normalStore.save(first);

        FileQueueSnapshotStore failingStore =
                new FileQueueSnapshotStore(
                        snapshotPath,
                        FileQueueSnapshotStore::writeCandidate,
                        FileQueueSnapshotStore::forceCandidate,
                        (candidate, destination) -> {
                            throw new IOException(
                                    "Simulated promotion failure"
                            );
                        }
                );

        assertThrows(
                SnapshotException.class,
                () -> failingStore.save(second)
        );

        Optional<QueueSnapshot> latest =
                normalStore.loadLatest();

        assertTrue(
                latest.isPresent()
        );

        assertEquals(
                first,
                latest.orElseThrow()
        );

        assertNotEquals(
                second,
                latest.orElseThrow()
        );
    }

    @Test
    void successfulReplacementMakesNewSnapshotAuthoritative() {
        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot first =
                snapshot(
                        new WalPosition(0, 100),
                        "first"
                );

        QueueSnapshot second =
                snapshot(
                        new WalPosition(0, 200),
                        "second"
                );

        FileQueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(first);

        assertEquals(
                first,
                store.loadLatest()
                        .orElseThrow()
        );

        store.save(second);

        QueueSnapshot recovered =
                store.loadLatest()
                        .orElseThrow();

        assertEquals(
                second,
                recovered
        );

        assertNotEquals(
                first,
                recovered
        );
    }

    @Test
    void leftoverTempFileDoesNotOverrideCommittedSnapshot()
            throws IOException {

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        QueueSnapshot committed =
                snapshot(
                        new WalPosition(0, 100),
                        "committed"
                );

        FileQueueSnapshotStore store =
                new FileQueueSnapshotStore(
                        snapshotPath
                );

        store.save(committed);

        /*
         * Simulate garbage left behind by a crashed
         * candidate save.
         */
        Path tempPath =
                snapshotPath.resolveSibling(
                        snapshotPath.getFileName()
                                + ".tmp"
                );

        Files.write(
                tempPath,
                new byte[]{
                        1, 2, 3, 4, 5, 6
                }
        );

        /*
         * loadLatest() must only trust the committed path.
         *
         * A .tmp file is never authoritative merely because
         * it happens to exist.
         */
        QueueSnapshot recovered =
                store.loadLatest()
                        .orElseThrow();

        assertEquals(
                committed,
                recovered
        );

        assertTrue(
                Files.exists(tempPath)
        );
    }


    private QueueSnapshot completeSnapshot(
            WalPosition position
    ) {
        return new QueueSnapshot(
                LINEAGE,
                position,
                List.of(
                        new ReadySnapshotEntry(
                                "ready-1",
                                "ready-payload",
                                1
                        )
                ),
                List.of(
                        new InFlightSnapshotEntry(
                                "inflight-1",
                                "inflight-payload",
                                "receipt-123",
                                2,
                                Instant.parse(
                                        "2026-08-23T10:30:00Z"
                                )
                        )
                ),
                List.of(
                        new DelayedSnapshotEntry(
                                "delayed-1",
                                "delayed-payload",
                                3,
                                Instant.parse(
                                        "2026-08-23T11:00:00Z"
                                )
                        )
                ),
                List.of(
                        new DeadLetterSnapshotEntry(
                                "dead-1",
                                "dead-payload"
                        )
                )
        );
    }

    private QueueSnapshot snapshot(
            WalPosition position,
            String payload
    ) {
        return new QueueSnapshot(
                LINEAGE,
                position,
                List.of(
                        new ReadySnapshotEntry(
                                "message-" + payload,
                                payload,
                                1
                        )
                ),
                List.of(),
                List.of(),
                List.of()
        );
    }

    private static void readFully(
            FileChannel channel,
            ByteBuffer buffer
    ) throws IOException {

        while (buffer.hasRemaining()) {
            int read =
                    channel.read(buffer);

            if (read == -1) {
                throw new AssertionError(
                        "Unexpected EOF in test setup"
                );
            }
        }
    }
}
