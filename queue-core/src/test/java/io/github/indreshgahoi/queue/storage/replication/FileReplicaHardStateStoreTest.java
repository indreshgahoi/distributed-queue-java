package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileReplicaHardStateStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void roundTripsLineageBoundTermVoteAndCommitIndex() {
        StorageLineage lineage = StorageLineage.create();
        Path path = tempDir.resolve("replica-hard-state.bin");
        FileReplicaHardStateStore store =
                new FileReplicaHardStateStore(path, lineage);
        ReplicaHardState expected =
                new ReplicaHardState(8, Optional.of("node-b"), 12);

        store.save(expected, 12);

        assertEquals(
                expected,
                new FileReplicaHardStateStore(path, lineage).load(12)
        );
    }

    @Test
    void rejectsTermCommitAndVoteRegressionWithoutReplacingAuthority() {
        StorageLineage lineage = StorageLineage.create();
        FileReplicaHardStateStore store = new FileReplicaHardStateStore(
                tempDir.resolve("replica-hard-state.bin"),
                lineage
        );
        ReplicaHardState authority =
                new ReplicaHardState(5, Optional.of("node-a"), 7);
        store.save(authority, 10);

        assertThrows(
                ReplicaException.class,
                () -> store.save(
                        new ReplicaHardState(4, Optional.empty(), 7),
                        10
                )
        );
        assertThrows(
                ReplicaException.class,
                () -> store.save(
                        new ReplicaHardState(5, Optional.of("node-b"), 7),
                        10
                )
        );
        assertThrows(
                ReplicaException.class,
                () -> store.save(
                        new ReplicaHardState(6, Optional.empty(), 11),
                        10
                )
        );
        assertEquals(authority, store.load(10));
    }

    @Test
    void rejectsCorruptOrForeignAuthority() throws Exception {
        StorageLineage lineage = StorageLineage.create();
        Path path = tempDir.resolve("replica-hard-state.bin");
        FileReplicaHardStateStore store =
                new FileReplicaHardStateStore(path, lineage);
        store.save(new ReplicaHardState(2, Optional.empty(), 0), 0);

        byte[] corrupt = Files.readAllBytes(path);
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(path, corrupt);
        assertThrows(ReplicaException.class, () -> store.load(0));

        Path foreignPath = tempDir.resolve("foreign.bin");
        new FileReplicaHardStateStore(
                foreignPath,
                StorageLineage.create()
        ).save(new ReplicaHardState(2, Optional.empty(), 0), 0);
        assertThrows(
                ReplicaLineageMismatchException.class,
                () -> new FileReplicaHardStateStore(
                        foreignPath,
                        lineage
                ).load(0)
        );
    }
}
