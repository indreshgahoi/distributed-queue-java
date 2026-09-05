package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

final class WalHeaderCodec {

    private static final int WAL_MAGIC =
            0x4451574C;

    private static final int WAL_VERSION = 3;

    static final int HEADER_SIZE =
            Integer.BYTES
                    + Integer.BYTES
                    + Long.BYTES * 4
                    + Integer.BYTES;

    ByteBuffer encode(
            StorageLineage lineage
    ) {
        ByteBuffer header =
                ByteBuffer.allocate(HEADER_SIZE);

        header.putInt(WAL_MAGIC);
        header.putInt(WAL_VERSION);
        putUuid(header, lineage.queueId());
        putUuid(header, lineage.generationId());
        header.putInt(lineage.partitionId());
        header.flip();

        return header;
    }

    StorageLineage read(
            FileChannel channel,
            String description
    ) throws IOException {
        ByteBuffer prefix =
                ByteBuffer.allocate(
                        Integer.BYTES * 2
                );

        readFully(channel, prefix, "Incomplete " + description + " header");
        prefix.flip();

        int magic = prefix.getInt();
        int version = prefix.getInt();

        if (magic != WAL_MAGIC) {
            throw new WalException(
                    "Invalid " + description + " magic"
            );
        }

        if (version != WAL_VERSION) {
            throw new WalException(
                    "Unsupported " + description + " version: "
                            + version
                            + "; expected: "
                            + WAL_VERSION
            );
        }

        ByteBuffer identity =
                ByteBuffer.allocate(
                        HEADER_SIZE - prefix.capacity()
                );

        readFully(channel, identity, "Incomplete " + description + " header");
        identity.flip();

        return new StorageLineage(
                readUuid(identity),
                readUuid(identity),
                identity.getInt()
        );
    }

    private static void putUuid(
            ByteBuffer buffer,
            UUID value
    ) {
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
    }

    private static UUID readUuid(
            ByteBuffer buffer
    ) {
        return new UUID(
                buffer.getLong(),
                buffer.getLong()
        );
    }

    private static void readFully(
            FileChannel channel,
            ByteBuffer buffer,
            String message
    ) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) == -1) {
                throw new WalException(message);
            }
        }
    }
}
