package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.DirectoryDurability;
import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Atomically publishes the term, vote, and committed logical boundary for one
 * replica. A candidate is forced before promotion and the directory is forced
 * after promotion so a successful save survives restart.
 */
public final class FileReplicaHardStateStore
        implements ReplicaHardStateStore {

    private static final int MAGIC = 0x44514853; // DQHS
    private static final int VERSION = 1;
    private static final int MAX_VOTE_BYTES = 4 * 1024;

    private final Path statePath;
    private final Path candidatePath;
    private final StorageLineage lineage;

    public FileReplicaHardStateStore(
            Path statePath,
            StorageLineage lineage
    ) {
        this.statePath = Objects.requireNonNull(statePath, "statePath");
        this.candidatePath = statePath.resolveSibling(
                statePath.getFileName() + ".tmp"
        );
        this.lineage = Objects.requireNonNull(lineage, "lineage");
    }

    @Override
    public synchronized ReplicaHardState load(long localDurableIndex) {
        if (localDurableIndex < 0) {
            throw new IllegalArgumentException(
                    "localDurableIndex must not be negative"
            );
        }
        if (!Files.exists(statePath)) {
            return ReplicaHardState.EMPTY;
        }

        try {
            byte[] bytes = Files.readAllBytes(statePath);
            if (bytes.length < minimumFileBytes()) {
                throw new ReplicaException("Invalid replica hard-state length");
            }

            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int magic = buffer.getInt();
            int version = buffer.getInt();
            StorageLineage storedLineage = new StorageLineage(
                    readUuid(buffer),
                    readUuid(buffer),
                    buffer.getInt()
            );
            long currentTerm = buffer.getLong();
            byte votePresent = buffer.get();
            int voteLength = buffer.getInt();
            validateVoteEncoding(votePresent, voteLength, buffer.remaining());

            Optional<String> votedFor;
            if (votePresent == 1) {
                byte[] voteBytes = new byte[voteLength];
                buffer.get(voteBytes);
                votedFor = Optional.of(
                        new String(voteBytes, StandardCharsets.UTF_8)
                );
            } else {
                votedFor = Optional.empty();
            }

            long commitIndex = buffer.getLong();
            int storedChecksum = buffer.getInt();

            if (buffer.hasRemaining()) {
                throw new ReplicaException("Trailing replica hard-state bytes");
            }
            if (magic != MAGIC || version != VERSION) {
                throw new ReplicaException("Unsupported replica hard-state header");
            }
            if (!lineage.equals(storedLineage)) {
                throw new ReplicaLineageMismatchException(storedLineage, lineage);
            }
            if (checksum(bytes, bytes.length - Integer.BYTES) != storedChecksum) {
                throw new ReplicaException("Replica hard-state checksum mismatch");
            }

            ReplicaHardState state =
                    new ReplicaHardState(currentTerm, votedFor, commitIndex);
            validateCommit(state, localDurableIndex);
            return state;
        } catch (IOException e) {
            throw new ReplicaException("Failed to load replica hard state", e);
        }
    }

    @Override
    public synchronized void save(
            ReplicaHardState state,
            long localDurableIndex
    ) {
        Objects.requireNonNull(state, "state");
        validateCommit(state, localDurableIndex);

        ReplicaHardState previous = load(localDurableIndex);
        validateMonotonic(previous, state);

        byte[] vote = state.votedFor()
                .map(value -> value.getBytes(StandardCharsets.UTF_8))
                .orElseGet(() -> new byte[0]);
        if (vote.length > MAX_VOTE_BYTES) {
            throw new ReplicaException("Replica vote exceeds maximum size");
        }

        int payloadBytes = minimumPayloadBytes() + vote.length;
        ByteBuffer buffer = ByteBuffer.allocate(payloadBytes + Integer.BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(VERSION);
        writeUuid(buffer, lineage.queueId());
        writeUuid(buffer, lineage.generationId());
        buffer.putInt(lineage.partitionId());
        buffer.putLong(state.currentTerm());
        buffer.put((byte) (state.votedFor().isPresent() ? 1 : 0));
        buffer.putInt(vote.length);
        buffer.put(vote);
        buffer.putLong(state.commitIndex());
        buffer.putInt(checksum(buffer.array(), payloadBytes));
        buffer.flip();

        try {
            Path parent = statePath.toAbsolutePath().getParent();
            if (parent == null) {
                throw new IOException("Hard-state path has no parent");
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
            throw new ReplicaException("Failed to persist replica hard state", e);
        }
    }

    private static void validateCommit(
            ReplicaHardState state,
            long localDurableIndex
    ) {
        if (state.commitIndex() > localDurableIndex) {
            throw new ReplicaException(
                    "Commit index " + state.commitIndex()
                            + " exceeds local durable index " + localDurableIndex
            );
        }
    }

    private static void validateMonotonic(
            ReplicaHardState previous,
            ReplicaHardState next
    ) {
        if (next.currentTerm() < previous.currentTerm()) {
            throw new ReplicaException("Replica term must not move backwards");
        }
        if (next.commitIndex() < previous.commitIndex()) {
            throw new ReplicaException("Commit index must not move backwards");
        }
        if (next.currentTerm() == previous.currentTerm()
                && previous.votedFor().isPresent()
                && !previous.votedFor().equals(next.votedFor())) {
            throw new ReplicaException("Vote must not change within one term");
        }
    }

    private static void validateVoteEncoding(
            byte votePresent,
            int voteLength,
            int remaining
    ) {
        if (votePresent != 0 && votePresent != 1) {
            throw new ReplicaException("Invalid vote-present marker");
        }
        if (voteLength < 0 || voteLength > MAX_VOTE_BYTES) {
            throw new ReplicaException("Invalid replica vote length");
        }
        if ((votePresent == 0 && voteLength != 0)
                || (votePresent == 1 && voteLength == 0)
                || remaining != voteLength + Long.BYTES + Integer.BYTES) {
            throw new ReplicaException("Invalid replica vote encoding");
        }
    }

    private static int minimumPayloadBytes() {
        return Integer.BYTES * 3
                + Long.BYTES * 6
                + Byte.BYTES
                + Integer.BYTES;
    }

    private static int minimumFileBytes() {
        return minimumPayloadBytes() + Integer.BYTES;
    }

    private static UUID readUuid(ByteBuffer buffer) {
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static void writeUuid(ByteBuffer buffer, UUID value) {
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
    }

    private static int checksum(byte[] bytes, int length) {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, length);
        return (int) checksum.getValue();
    }

    private void deleteCandidateBestEffort() {
        try {
            Files.deleteIfExists(candidatePath);
        } catch (IOException ignored) {
            // Candidate files are never authoritative.
        }
    }
}
