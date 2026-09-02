package io.github.indreshgahoi.queue.node.config;

import io.github.indreshgahoi.queue.node.application.port.out.ProvisioningMetadataClient;
import io.github.indreshgahoi.queue.node.application.port.out.NodeTopologyClient;
import io.github.indreshgahoi.queue.node.application.port.out.QueueStorageProvisioner;
import io.github.indreshgahoi.queue.node.application.service.NodeRegistrationManager;
import io.github.indreshgahoi.queue.node.application.service.ProvisioningReconciler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
class QueueNodeConfiguration {

    @Bean
    RestClient.Builder restClientBuilder() {
        return RestClient.builder();
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
}
