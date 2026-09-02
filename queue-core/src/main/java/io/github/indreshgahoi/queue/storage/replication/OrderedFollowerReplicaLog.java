package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WriteAheadLog;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Applies leader WAL records to a follower in lineage-scoped sequence order.
 *
 * <p>The leader epoch is durably advanced before a record from that epoch is
 * appended. A crash can therefore leave a newer epoch with no new record, but
 * can never make the follower accept an older leader after restart.</p>
 */
public final class OrderedFollowerReplicaLog
        implements FollowerReplicaLog {

    private final WriteAheadLog wal;
    private final StorageLineage lineage;
    private final LeaderEpochStore epochStore;
    private final List<WalRecord> records;

    private long highestLeaderEpoch;
    private boolean closed;
    private boolean poisoned;

    public OrderedFollowerReplicaLog(
            WriteAheadLog wal,
            Path leaderEpochStatePath
    ) {
        this(
                wal,
                new FileLeaderEpochStore(
                        leaderEpochStatePath,
                        wal.storageLineage()
                )
        );
    }

    OrderedFollowerReplicaLog(
            WriteAheadLog wal,
            LeaderEpochStore epochStore
    ) {
        this.wal = Objects.requireNonNull(wal, "wal");
        this.lineage = Objects.requireNonNull(
                wal.storageLineage(),
                "wal.storageLineage()"
        );
        this.epochStore = Objects.requireNonNull(
                epochStore,
                "epochStore"
        );

        if (!wal.hasCompleteHistory()) {
            throw new ReplicaException(
                    "Follower replication requires complete WAL history"
            );
        }

        this.records = new ArrayList<>(wal.readAll());
        this.highestLeaderEpoch = epochStore.load();

        if (!records.isEmpty() && highestLeaderEpoch == 0) {
            throw new ReplicaException(
                    "Follower WAL has records but no durable leader epoch"
            );
        }
    }

    @Override
    public synchronized ReplicaAppendResult append(
            ReplicatedWalEntry entry
    ) {
        ensureWritable();
        Objects.requireNonNull(entry, "entry");

        if (!lineage.equals(entry.lineage())) {
            throw new ReplicaLineageMismatchException(
                    entry.lineage(),
                    lineage
            );
        }
        if (entry.leaderEpoch() < highestLeaderEpoch) {
            throw new StaleLeaderEpochException(
                    entry.leaderEpoch(),
                    highestLeaderEpoch
            );
        }

        /*
         * Epoch is authority, while sequence is log consistency. Once a
         * newer legitimate authority is observed, it must fence the older
         * leader even when this particular entry has a gap or conflict.
         */
        advanceEpochIfRequired(entry.leaderEpoch());

        long expectedSequence = records.size() + 1L;
        if (entry.sequence() > expectedSequence) {
            throw new ReplicaSequenceException(
                    entry.sequence(),
                    expectedSequence
            );
        }

        if (entry.sequence() < expectedSequence) {
            WalRecord existing = records.get(
                    Math.toIntExact(entry.sequence() - 1)
            );
            if (!existing.equals(entry.record())) {
                throw new ReplicaConflictException(entry.sequence());
            }
            return ReplicaAppendResult.ALREADY_PRESENT;
        }

        try {
            wal.append(entry.record());
            records.add(entry.record());
            return ReplicaAppendResult.APPENDED;
        } catch (RuntimeException e) {
            poisoned = true;
            throw e;
        }
    }

    @Override
    public StorageLineage lineage() {
        return lineage;
    }

    @Override
    public synchronized long lastSequence() {
        ensureOpen();
        return records.size();
    }

    @Override
    public synchronized long highestLeaderEpoch() {
        ensureOpen();
        return highestLeaderEpoch;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        wal.close();
        closed = true;
    }

    private void advanceEpochIfRequired(long leaderEpoch) {
        if (leaderEpoch == highestLeaderEpoch) {
            return;
        }

        try {
            epochStore.save(leaderEpoch);
            highestLeaderEpoch = leaderEpoch;
        } catch (RuntimeException e) {
            poisoned = true;
            throw e;
        }
    }

    private void ensureWritable() {
        ensureOpen();
        if (poisoned) {
            throw new ReplicaException(
                    "Follower replica log is poisoned after a storage failure"
            );
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new ReplicaException(
                    "Follower replica log is closed"
            );
        }
    }
}
