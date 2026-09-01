package io.github.indreshgahoi.queue.metadata.adapter.in.web;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class OpenApiConfiguration {

    @Bean
    OpenAPI metadataServiceOpenApi() {
        return new OpenAPI().info(
                new Info()
                        .title("Queue Metadata Service API")
                        .version("v0.19.0")
                        .description(
                                "Tenant-scoped queue identity and lifecycle "
                                        + "control-plane API"
                        )
        );
    }
}
