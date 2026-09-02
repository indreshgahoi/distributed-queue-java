package io.github.indreshgahoi.queue.node.adapter.in.scheduling;

import io.github.indreshgahoi.queue.node.application.service.NodeRegistrationManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
final class NodeRegistrationScheduler {
    private final NodeRegistrationManager registrationManager;

    NodeRegistrationScheduler(NodeRegistrationManager registrationManager) {
        this.registrationManager = registrationManager;
    }

    @Scheduled(
            initialDelayString = "${queue.node.registration-initial-delay:PT0S}",
            fixedDelayString = "${queue.node.heartbeat-delay:PT10S}"
    )
    void maintainRegistration() {
        try {
            registrationManager.maintainLease();
        } catch (RuntimeException failure) {
            log.warn(
                    "event=queue_node_registration_maintenance_failed",
                    failure
            );
        }
    }
}
