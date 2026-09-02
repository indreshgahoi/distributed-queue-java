package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.DirectoryDurability;
import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

final class FileLeaderEpochStore
        implements LeaderEpochStore {

    private static final int MAGIC = 0x44514550; // "DQEP"
    private static final int VERSION = 1;
    private static final int PAYLOAD_BYTES =
            Integer.BYTES * 3
                    + Long.BYTES * 5;
    private static final int FILE_BYTES =
            PAYLOAD_BYTES + Integer.BYTES;

    private final Path statePath;
    private final Path candidatePath;
    private final StorageLineage lineage;

    FileLeaderEpochStore(
            Path statePath,
            StorageLineage lineage
    ) {
        this.statePath = Objects.requireNonNull(
                statePath,
                "statePath"
        );
        this.candidatePath = statePath.resolveSibling(
                statePath.getFileName() + ".tmp"
        );
        this.lineage = Objects.requireNonNull(
                lineage,
                "lineage"
        );
    }

    @Override
    public long load() {
        if (!Files.exists(statePath)) {
            return 0;
        }

        try {
            byte[] bytes = Files.readAllBytes(statePath);
            if (bytes.length != FILE_BYTES) {
                throw new ReplicaException(
                        "Invalid leader epoch state length"
                );
            }

            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int magic = buffer.getInt();
            int version = buffer.getInt();
            StorageLineage storedLineage = new StorageLineage(
                    readUuid(buffer),
                    readUuid(buffer),
                    buffer.getInt()
            );
            long epoch = buffer.getLong();
            int storedChecksum = buffer.getInt();

            if (magic != MAGIC || version != VERSION) {
                throw new ReplicaException(
                        "Unsupported leader epoch state header"
                );
            }
            if (!lineage.equals(storedLineage)) {
                throw new ReplicaLineageMismatchException(
                        storedLineage,
                        lineage
                );
            }
            if (epoch <= 0) {
                throw new ReplicaException(
                        "Stored leader epoch must be positive"
                );
            }
            if (checksum(bytes, PAYLOAD_BYTES)
                    != storedChecksum) {
                throw new ReplicaException(
                        "Leader epoch state checksum mismatch"
                );
            }
            return epoch;
        } catch (IOException e) {
            throw new ReplicaException(
                    "Failed to load leader epoch state",
                    e
            );
        }
    }

    @Override
    public void save(long leaderEpoch) {
        ByteBuffer buffer = ByteBuffer.allocate(FILE_BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(VERSION);
        writeUuid(buffer, lineage.queueId());
        writeUuid(buffer, lineage.generationId());
        buffer.putInt(lineage.partitionId());
        buffer.putLong(leaderEpoch);
        buffer.putInt(checksum(buffer.array(), PAYLOAD_BYTES));
        buffer.flip();

        try {
            Path parent = statePath.toAbsolutePath().getParent();
            if (parent == null) {
                throw new IOException(
                        "Leader epoch state has no parent directory"
                );
            }
            Files.createDirectories(parent);

            try (FileChannel channel = FileChannel.open(
                    candidatePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            )) {
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }

            Files.move(
                    candidatePath,
                    statePath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
            DirectoryDurability.forceParent(statePath);
        } catch (IOException e) {
            deleteCandidateBestEffort();
            throw new ReplicaException(
                    "Failed to persist leader epoch " + leaderEpoch,
                    e
            );
        }
    }

    private static UUID readUuid(ByteBuffer buffer) {
        return new UUID(
                buffer.getLong(),
                buffer.getLong()
        );
    }

    private static void writeUuid(
            ByteBuffer buffer,
            UUID value
    ) {
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
    }

    private static int checksum(
            byte[] bytes,
            int length
    ) {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, length);
        return (int) checksum.getValue();
    }

    private void deleteCandidateBestEffort() {
        try {
            Files.deleteIfExists(candidatePath);
        } catch (IOException ignored) {
            // The candidate is never authoritative and is ignored on load.
        }
    }
}
