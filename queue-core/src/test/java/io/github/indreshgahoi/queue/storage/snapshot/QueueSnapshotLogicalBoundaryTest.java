package io.github.indreshgahoi.queue.storage.snapshot;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.WalPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueueSnapshotLogicalBoundaryTest {

    @TempDir
    Path tempDir;

    @Test
    void versionThreeRoundTripsLogicalAndPhysicalBoundary() throws Exception {
        Path path = tempDir.resolve("snapshot.bin");
        QueueSnapshot expected = new QueueSnapshot(
                StorageLineage.create(),
                new WalPosition(4, 900),
                42,
                7,
                List.of(),
                List.of(),
                List.of(),
                List.of()
        );
        FileQueueSnapshotStore store = new FileQueueSnapshotStore(path);

        store.save(expected);

        assertEquals(expected, store.loadLatest().orElseThrow());
        assertEquals(
                3,
                ByteBuffer.wrap(Files.readAllBytes(path)).getInt(Integer.BYTES)
        );
    }

    @Test
    void logicalBoundaryRequiresIndexAndTermTogether() {
        StorageLineage lineage = StorageLineage.create();
        WalPosition position = new WalPosition(0, 44);

        assertThrows(
                IllegalArgumentException.class,
                () -> new QueueSnapshot(
                        lineage,
                        position,
                        5,
                        0,
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of()
                )
        );
    }
}
