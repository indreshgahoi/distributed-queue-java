package io.github.indreshgahoi.queue.gateway.adapter.in.web;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class OpenApiConfiguration {
    @Bean
    OpenAPI gatewayOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Distributed Queue Gateway API")
                .version("v0.25.0")
                .description(
                        "Stable customer data plane backed by fenced "
                                + "metadata route resolution"
                ));
    }
}
