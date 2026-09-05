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
    private final ReplicatedLog replicatedLog;
    private final StorageLineage lineage;
    private final LeaderEpochStore epochStore;
    private final ReplicaHardStateStore hardStateStore;
    private final List<WalRecord> records;

    private long highestLeaderEpoch;
    private boolean closed;
    private boolean poisoned;

    public OrderedFollowerReplicaLog(
            WriteAheadLog wal,
            Path hardStatePath
    ) {
        this.wal = Objects.requireNonNull(wal, "wal");
        this.lineage = Objects.requireNonNull(
                wal.storageLineage(),
                "wal.storageLineage()"
        );
        this.replicatedLog = wal instanceof ReplicatedLog logical
                ? logical
                : null;
        this.hardStateStore = replicatedLog == null
                ? null
                : new FileReplicaHardStateStore(hardStatePath, lineage);
        this.epochStore = replicatedLog == null
                ? new FileLeaderEpochStore(hardStatePath, lineage)
                : null;

        if (!wal.hasCompleteHistory() && replicatedLog == null) {
            throw new ReplicaException(
                    "Follower replication requires complete WAL history"
            );
        }

        this.records = new ArrayList<>(wal.readAll());
        this.highestLeaderEpoch = replicatedLog == null
                ? epochStore.load()
                : hardStateStore.load(
                        replicatedLog.localDurableIndex()
                ).currentTerm();

        if (!records.isEmpty() && highestLeaderEpoch == 0) {
            throw new ReplicaException(
                    "Follower WAL has records but no durable leader term"
            );
        }
    }

    OrderedFollowerReplicaLog(
            WriteAheadLog wal,
            LeaderEpochStore epochStore
    ) {
        this.wal = Objects.requireNonNull(wal, "wal");
        this.replicatedLog = null;
        this.lineage = Objects.requireNonNull(
                wal.storageLineage(),
                "wal.storageLineage()"
        );
        this.epochStore = Objects.requireNonNull(
                epochStore,
                "epochStore"
        );
        this.hardStateStore = null;

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
        ReplicaBatchAppendResult result = appendBatch(List.of(entry));
        return result.appendedEntries() == 1
                ? ReplicaAppendResult.APPENDED
                : ReplicaAppendResult.ALREADY_PRESENT;
    }

    @Override
    public synchronized ReplicaBatchAppendResult appendBatch(
            List<ReplicatedWalEntry> entries
    ) {
        ensureWritable();
        Objects.requireNonNull(entries, "entries");

        List<ReplicatedWalEntry> batch = List.copyOf(entries);
        if (batch.isEmpty()) {
            return new ReplicaBatchAppendResult(
                    lastSequence(),
                    0,
                    0
            );
        }

        if (replicatedLog != null) {
            return appendLogicalBatch(batch);
        }

        int appended = 0;
        int alreadyPresent = 0;
        for (ReplicatedWalEntry entry : batch) {
            ReplicaAppendResult result = appendLegacy(entry);
            if (result == ReplicaAppendResult.APPENDED) {
                appended++;
            } else {
                alreadyPresent++;
            }
        }
        return new ReplicaBatchAppendResult(
                records.size(),
                appended,
                alreadyPresent
        );
    }

    @Override
    public StorageLineage lineage() {
        return lineage;
    }

    @Override
    public synchronized long lastSequence() {
        ensureOpen();
        return replicatedLog == null
                ? records.size()
                : replicatedLog.localDurableIndex();
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
            if (hardStateStore == null) {
                epochStore.save(leaderEpoch);
            } else {
                ReplicaHardState current = hardStateStore.load(
                        replicatedLog.localDurableIndex()
                );
                hardStateStore.save(
                        new ReplicaHardState(
                                leaderEpoch,
                                java.util.Optional.empty(),
                                current.commitIndex()
                        ),
                        replicatedLog.localDurableIndex()
                );
            }
            highestLeaderEpoch = leaderEpoch;
        } catch (RuntimeException e) {
            poisoned = true;
            throw e;
        }
    }

    private ReplicaBatchAppendResult appendLogicalBatch(
            List<ReplicatedWalEntry> entries
    ) {
        ReplicatedWalEntry first = entries.getFirst();
        validateAuthority(first);
        for (ReplicatedWalEntry entry : entries) {
            validateAuthority(entry);
            if (entry.leaderEpoch() != first.leaderEpoch()) {
                throw new ReplicaException(
                        "One follower batch must contain one leader term"
                );
            }
        }

        advanceEpochIfRequired(first.leaderEpoch());

        long previousIndex = first.sequence() - 1;
        LogPoint previous = previousIndex == 0
                ? LogPoint.EMPTY
                : replicatedLog.entry(previousIndex)
                        .map(LogEntry::point)
                        .orElseGet(() -> {
                            LogPoint compacted = replicatedLog.snapshotBoundary();
                            if (compacted.logIndex() == previousIndex) {
                                return compacted;
                            }
                            throw new ReplicaSequenceException(
                                    first.sequence(),
                                    replicatedLog.localDurableIndex() + 1
                            );
                        });

        List<LogEntry> logicalEntries = entries.stream()
                .map(entry -> new LogEntry(
                        entry.sequence(),
                        entry.leaderEpoch(),
                        entry.record()
                ))
                .toList();

        try {
            AppendBatchResult result = replicatedLog.appendReplicated(
                    previous,
                    logicalEntries
            );
            records.clear();
            records.addAll(wal.readAll());
            return new ReplicaBatchAppendResult(
                    result.durableThroughIndex(),
                    result.appendedEntries(),
                    result.alreadyPresentEntries()
            );
        } catch (LogConflictException failure) {
            throw new ReplicaConflictException(first.sequence());
        } catch (LogGapException failure) {
            throw new ReplicaSequenceException(
                    first.sequence(),
                    replicatedLog.localDurableIndex() + 1
            );
        } catch (RuntimeException failure) {
            if (!(failure instanceof ReplicaSequenceException)
                    && !(failure instanceof ReplicaConflictException)) {
                poisoned = true;
            }
            throw failure;
        }
    }

    private ReplicaAppendResult appendLegacy(ReplicatedWalEntry entry) {
        validateAuthority(entry);
        advanceEpochIfRequired(entry.leaderEpoch());

        long expectedSequence = records.size() + 1L;
        if (entry.sequence() > expectedSequence) {
            throw new ReplicaSequenceException(entry.sequence(), expectedSequence);
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
        } catch (RuntimeException failure) {
            poisoned = true;
            throw failure;
        }
    }

    private void validateAuthority(ReplicatedWalEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if (!lineage.equals(entry.lineage())) {
            throw new ReplicaLineageMismatchException(entry.lineage(), lineage);
        }
        if (entry.leaderEpoch() < highestLeaderEpoch) {
            throw new StaleLeaderEpochException(
                    entry.leaderEpoch(),
                    highestLeaderEpoch
            );
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
