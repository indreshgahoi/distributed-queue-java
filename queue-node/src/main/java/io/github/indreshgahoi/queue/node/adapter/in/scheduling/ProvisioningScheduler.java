package io.github.indreshgahoi.queue.node.adapter.in.scheduling;

import io.github.indreshgahoi.queue.node.application.port.in.ReconcileProvisioningUseCase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
final class ProvisioningScheduler {
    private static final int REPEATED_FAILURE_LOG_INTERVAL = 60;

    private final ReconcileProvisioningUseCase reconciler;
    private int consecutiveFailures;

    ProvisioningScheduler(
            ReconcileProvisioningUseCase reconciler
    ) {
        this.reconciler = reconciler;
    }

    @Scheduled(
            fixedDelayString = "${queue.node.poll-delay:PT1S}"
    )
    void reconcile() {
        try {
            reconciler.runOnce();
            if (consecutiveFailures > 0) {
                log.info(
                        "event=provisioning_reconciliation_recovered "
                                + "previousConsecutiveFailures={}",
                        consecutiveFailures
                );
                consecutiveFailures = 0;
            }
        } catch (RuntimeException failure) {
            consecutiveFailures++;
            if (consecutiveFailures == 1) {
                log.warn(
                        "event=provisioning_reconciliation_failed "
                                + "consecutiveFailures={}",
                        consecutiveFailures,
                        failure
                );
            } else if (consecutiveFailures
                    % REPEATED_FAILURE_LOG_INTERVAL == 0) {
                log.warn(
                        "event=provisioning_reconciliation_still_failing "
                                + "consecutiveFailures={} errorType={} "
                                + "errorMessage={}",
                        consecutiveFailures,
                        failure.getClass().getSimpleName(),
                        failure.getMessage()
                );
            } else {
                log.debug(
                        "event=provisioning_reconciliation_retry_failed "
                                + "consecutiveFailures={}",
                        consecutiveFailures,
                        failure
                );
            }
        }
    }
}
