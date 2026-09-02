package io.github.indreshgahoi.queue.node.adapter.in.scheduling;

import io.github.indreshgahoi.queue.node.application.port.in.ReconcileRuntimePartitionsUseCase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
final class RuntimePartitionScheduler {
    private final ReconcileRuntimePartitionsUseCase reconciler;

    RuntimePartitionScheduler(
            ReconcileRuntimePartitionsUseCase reconciler
    ) {
        this.reconciler = reconciler;
    }

    @Scheduled(fixedDelayString = "${queue.node.runtime-poll-delay:PT1S}")
    void reconcile() {
        try {
            reconciler.runOnce();
        } catch (RuntimeException failure) {
            // A failed poll must not kill Spring's scheduling thread. Existing
            // runtimes remain governed by their finite registration lease.
            log.warn("event=runtime_partition_reconciliation_failed", failure);
        }
    }
}
