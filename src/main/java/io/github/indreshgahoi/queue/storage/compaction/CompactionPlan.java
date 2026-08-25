package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.List;

public record CompactionPlan(
        List<Long> deletableSegmentIds,
        WalPosition retainedFrom
) {
}