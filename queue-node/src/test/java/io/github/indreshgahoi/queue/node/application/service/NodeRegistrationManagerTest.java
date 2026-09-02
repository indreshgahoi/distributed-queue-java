package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.NodeTopologyClient;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeRegistrationManagerTest {
    private static final URI ENDPOINT = URI.create(
            "http://node-a:8081"
    );

    @Test
    void registersBeforeExposingCurrentAuthority() {
        FakeTopology topology = new FakeTopology();
        NodeRegistrationManager manager = manager(topology);

        assertTrue(manager.currentRegistration().isEmpty());
        manager.maintainLease();

        assertEquals(1, topology.registrations);
        assertEquals(
                1,
                manager.currentRegistration()
                        .orElseThrow()
                        .registrationEpoch()
        );
    }

    @Test
    void heartbeatsExistingRegistrationWithoutNewEpoch() {
        FakeTopology topology = new FakeTopology();
        NodeRegistrationManager manager = manager(topology);

        manager.maintainLease();
        manager.maintainLease();

        assertEquals(1, topology.registrations);
        assertEquals(1, topology.heartbeats);
        assertEquals(
                1,
                manager.currentRegistration()
                        .orElseThrow()
                        .registrationEpoch()
        );
    }

    @Test
    void ambiguousHeartbeatDropsAuthorityAndReregisters() {
        FakeTopology topology = new FakeTopology();
        NodeRegistrationManager manager = manager(topology);
        manager.maintainLease();
        topology.failHeartbeat = true;

        assertThrows(RuntimeException.class, manager::maintainLease);
        assertTrue(manager.currentRegistration().isEmpty());

        topology.failHeartbeat = false;
        manager.maintainLease();
        assertEquals(
                2,
                manager.currentRegistration()
                        .orElseThrow()
                        .registrationEpoch()
        );
    }

    private NodeRegistrationManager manager(FakeTopology topology) {
        return new NodeRegistrationManager(
                "node-a",
                ENDPOINT,
                Duration.ofSeconds(30),
                topology
        );
    }

    private static final class FakeTopology
            implements NodeTopologyClient {
        private int registrations;
        private int heartbeats;
        private boolean failHeartbeat;

        @Override
        public NodeRegistration register(
                String nodeId,
                URI endpoint,
                Duration leaseDuration
        ) {
            registrations++;
            return registration(registrations);
        }

        @Override
        public NodeRegistration heartbeat(
                NodeRegistration registration,
                Duration leaseDuration
        ) {
            heartbeats++;
            if (failHeartbeat) {
                throw new IllegalStateException("response lost");
            }
            return registration(registration.registrationEpoch());
        }

        private NodeRegistration registration(long epoch) {
            return new NodeRegistration(
                    "node-a",
                    epoch,
                    Instant.parse("2026-09-01T12:00:30Z")
            );
        }
    }
}
