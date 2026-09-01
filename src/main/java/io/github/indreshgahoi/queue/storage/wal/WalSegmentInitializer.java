package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class WalSegmentInitializer {

    static final int WAL_HEADER_SIZE =
            WalHeaderCodec.HEADER_SIZE;

    private final StorageLineage storageLineage;
    private final WalHeaderCodec headerCodec;

    WalSegmentInitializer() {
        this(StorageLineage.create());
    }

    WalSegmentInitializer(
            StorageLineage storageLineage
    ) {
        this.storageLineage = storageLineage;
        this.headerCodec = new WalHeaderCodec();
    }

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
                    headerCodec.encode(
                            storageLineage
                    );

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

            StorageLineage actual =
                    headerCodec.read(
                            channel,
                            "WAL segment"
                    );

            if (!storageLineage.equals(actual)) {
                throw new WalException(
                        "WAL segment lineage mismatch. Expected "
                                + storageLineage
                                + " but found "
                                + actual
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

    static StorageLineage readLineage(
            Path path
    ) {
        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.READ
                     )) {
            return new WalHeaderCodec()
                    .read(channel, "WAL segment");

        } catch (IOException e) {
            throw new WalException(
                    "Failed to read WAL segment lineage: "
                            + path,
                    e
            );
        }
    }
}
