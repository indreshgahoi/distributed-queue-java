package io.github.indreshgahoi.queue.storage.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class WalSegmentInitializer {

    private static final int WAL_MAGIC =
            0x4451574C; // DQWL

    private static final int WAL_VERSION =
            1;

    static final int WAL_HEADER_SIZE =
            Integer.BYTES
                    + Integer.BYTES;

    void initialize(
            Path path
    ) {
        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.CREATE_NEW,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer header =
                    ByteBuffer.allocate(
                            WAL_HEADER_SIZE
                    );

            header.putInt(WAL_MAGIC);
            header.putInt(WAL_VERSION);
            header.flip();

            while (header.hasRemaining()) {
                channel.write(header);
            }

            /*
             * New segment must be durable before it can
             * eventually become authoritative.
             */
            channel.force(true);

        } catch (IOException e) {
            throw new WalException(
                    "Failed to initialize WAL segment: "
                            + path,
                    e
            );
        }
    }

    void validate(
            Path path
    ) {
        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.READ
                     )) {

            if (channel.size()
                    < WAL_HEADER_SIZE) {

                throw new WalException(
                        "Incomplete WAL segment header: "
                                + path
                );
            }

            ByteBuffer header =
                    ByteBuffer.allocate(
                            WAL_HEADER_SIZE
                    );

            while (header.hasRemaining()) {
                int read =
                        channel.read(header);

                if (read == -1) {
                    throw new WalException(
                            "Incomplete WAL segment header: "
                                    + path
                    );
                }
            }

            header.flip();

            int magic =
                    header.getInt();

            int version =
                    header.getInt();

            if (magic != WAL_MAGIC) {
                throw new WalException(
                        "Invalid WAL segment magic: "
                                + path
                );
            }

            if (version != WAL_VERSION) {
                throw new WalException(
                        "Unsupported WAL segment version "
                                + version
                                + ": "
                                + path
                );
            }

        } catch (IOException e) {
            throw new WalException(
                    "Failed to validate WAL segment: "
                            + path,
                    e
            );
        }
    }
}