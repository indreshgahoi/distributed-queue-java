package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

public final class NoOpWalCompactor
        implements WalCompactor {

    @Override
    public void compactThrough(
            WalPosition position
    ) {
        /*
         * v0.12.4:
         *
         * Boundary is understood and validated,
         * but physical prefix reclamation is deliberately
         * deferred until WAL segmentation.
         */
    }
}