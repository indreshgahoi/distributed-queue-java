package io.github.indreshgahoi.queue.node.adapter.in.web;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class OpenApiConfiguration {
    @Bean
    OpenAPI queueNodeOpenApi() {
        return new OpenAPI().info(
                new Info()
                        .title("Queue Node Data Plane API")
                        .version("v0.22.0")
                        .description(
                                "Authority-guarded message operations for "
                                        + "local READY queue runtimes"
                        )
        );
    }
}
