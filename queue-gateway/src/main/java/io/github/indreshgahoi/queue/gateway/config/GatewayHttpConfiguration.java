package io.github.indreshgahoi.queue.gateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;

import java.net.http.HttpClient;

@Configuration
class GatewayHttpConfiguration {
    @Bean
    ClientHttpRequestFactory gatewayRequestFactory(
            QueueGatewayProperties properties
    ) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(properties.connectTimeout())
                .build();
        JdkClientHttpRequestFactory factory =
                new JdkClientHttpRequestFactory(client);
        factory.setReadTimeout(properties.requestTimeout());
        return factory;
    }
}
