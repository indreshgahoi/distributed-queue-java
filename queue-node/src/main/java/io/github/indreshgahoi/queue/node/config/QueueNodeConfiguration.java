package io.github.indreshgahoi.queue.node.config;

import io.github.indreshgahoi.queue.node.application.port.out.ProvisioningMetadataClient;
import io.github.indreshgahoi.queue.node.application.port.out.QueueStorageProvisioner;
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
    ProvisioningReconciler provisioningReconciler(
            QueueNodeProperties properties,
            ProvisioningMetadataClient metadata,
            QueueStorageProvisioner storage
    ) {
        return new ProvisioningReconciler(
                properties.id(),
                properties.leaseDuration(),
                metadata,
                storage
        );
    }
}
