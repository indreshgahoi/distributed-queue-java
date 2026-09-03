package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;

import java.util.List;

/**
 * Supplies logical leader entries. Implementations must retain the requested
 * sequence or fail explicitly; returning a later suffix would create a gap.
 */
public interface LeaderReplicationSource {

    List<WalRecord> read(
            StorageLineage lineage,
            long firstSequence,
            int maximumEntries
    );
}
