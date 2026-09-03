package io.github.indreshgahoi.queue.node.config;

import io.github.indreshgahoi.queue.node.application.port.out.ProvisioningMetadataClient;
import io.github.indreshgahoi.queue.node.application.port.out.NodeTopologyClient;
import io.github.indreshgahoi.queue.node.application.port.out.QueueStorageProvisioner;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueueFactory;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeTopologyClient;
import io.github.indreshgahoi.queue.node.application.service.NodeRegistrationManager;
import io.github.indreshgahoi.queue.node.application.service.ProvisioningReconciler;
import io.github.indreshgahoi.queue.node.application.service.QueueDataPlaneService;
import io.github.indreshgahoi.queue.node.application.service.RuntimePartitionManager;
import io.github.indreshgahoi.queue.node.application.service.FollowerReplicationService;
import io.github.indreshgahoi.queue.node.application.port.out.FollowerReplicaLogProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;

import java.net.http.HttpClient;
import java.time.Clock;

@Configuration(proxyBeanMethods = false)
class QueueNodeConfiguration {

    @Bean
    RestClient.Builder restClientBuilder(
            ClientHttpRequestFactory requestFactory
    ) {
        return RestClient.builder().requestFactory(requestFactory);
    }

    @Bean
    ClientHttpRequestFactory queueNodeRequestFactory(
            QueueNodeProperties properties
    ) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(properties.httpConnectTimeout())
                .build();
        JdkClientHttpRequestFactory factory =
                new JdkClientHttpRequestFactory(client);
        factory.setReadTimeout(properties.httpRequestTimeout());
        return factory;
    }

    @Bean
    Clock queueNodeClock() {
        return Clock.systemUTC();
    }

    @Bean
    NodeRegistrationManager nodeRegistrationManager(
            QueueNodeProperties properties,
            NodeTopologyClient topology
    ) {
        return new NodeRegistrationManager(
                properties.id(),
                properties.endpoint(),
                properties.registrationLeaseDuration(),
                topology
        );
    }

    @Bean
    ProvisioningReconciler provisioningReconciler(
            QueueNodeProperties properties,
            NodeRegistrationManager registration,
            ProvisioningMetadataClient metadata,
            QueueStorageProvisioner storage
    ) {
        return new ProvisioningReconciler(
                properties.id(),
                properties.leaseDuration(),
                registration,
                metadata,
                storage
        );
    }

    @Bean
    RuntimePartitionManager runtimePartitionManager(
            QueueNodeProperties properties,
            Clock clock,
            NodeRegistrationManager registration,
            RuntimeTopologyClient topology,
            RuntimeQueueFactory queues
    ) {
        return new RuntimePartitionManager(
                properties.id(),
                clock,
                registration,
                topology,
                queues
        );
    }

    @Bean
    QueueDataPlaneService queueDataPlaneService(
            RuntimePartitionManager partitions
    ) {
        return new QueueDataPlaneService(partitions);
    }

    @Bean
    FollowerReplicationService followerReplicationService(
            FollowerReplicaLogProvider logs
    ) {
        return new FollowerReplicationService(logs);
    }
}
