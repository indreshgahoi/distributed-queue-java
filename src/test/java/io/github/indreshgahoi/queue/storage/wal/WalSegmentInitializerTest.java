package io.github.indreshgahoi.queue.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class WalSegmentInitializerTest {

    @TempDir
    Path tempDir;

    @Test
    void newlyInitializedSegmentContainsValidHeader()
            throws IOException {

        Path path =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(path);

        assertTrue(
                Files.exists(path)
        );

        assertEquals(
                WalSegmentInitializer.WAL_HEADER_SIZE,
                Files.size(path)
        );

        /*
         * Validation itself should succeed.
         */
        assertDoesNotThrow(
                () -> initializer.validate(path)
        );
    }

    @Test
    void initializedSegmentContainsExpectedMagicAndVersion()
            throws IOException {

        Path path =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(path);

        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.READ
                     )) {

            ByteBuffer header =
                    ByteBuffer.allocate(
                            Integer.BYTES * 2
                    );

            while (header.hasRemaining()) {
                channel.read(header);
            }

            header.flip();

            assertEquals(
                    0x4451574C,
                    header.getInt()
            );

            assertEquals(
                    1,
                    header.getInt()
            );
        }
    }

    @Test
    void incompleteSegmentHeaderIsRejected()
            throws IOException {

        Path path =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        /*
         * Only magic, version is missing.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.CREATE_NEW,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer partial =
                    ByteBuffer.allocate(
                            Integer.BYTES
                    );

            partial.putInt(
                    0x4451574C
            );

            partial.flip();

            while (partial.hasRemaining()) {
                channel.write(partial);
            }
        }

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        assertThrows(
                WalException.class,
                () -> initializer.validate(path)
        );
    }

    @Test
    void invalidSegmentMagicIsRejected()
            throws IOException {

        Path path =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.CREATE_NEW,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer header =
                    ByteBuffer.allocate(
                            Integer.BYTES * 2
                    );

            header.putInt(
                    0x12345678
            );

            header.putInt(
                    1
            );

            header.flip();

            while (header.hasRemaining()) {
                channel.write(header);
            }
        }

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        assertThrows(
                WalException.class,
                () -> initializer.validate(path)
        );
    }

    @Test
    void unsupportedSegmentVersionIsRejected()
            throws IOException {

        Path path =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.CREATE_NEW,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer header =
                    ByteBuffer.allocate(
                            Integer.BYTES * 2
                    );

            header.putInt(
                    0x4451574C
            );

            header.putInt(
                    999
            );

            header.flip();

            while (header.hasRemaining()) {
                channel.write(header);
            }
        }

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        assertThrows(
                WalException.class,
                () -> initializer.validate(path)
        );
    }

    @Test
    void initializeDoesNotOverwriteExistingSegment()
            throws IOException {

        Path path =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(path);

        long originalSize =
                Files.size(path);

        assertThrows(
                WalException.class,
                () -> initializer.initialize(path)
        );

        assertEquals(
                originalSize,
                Files.size(path)
        );
    }
}