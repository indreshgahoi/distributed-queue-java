package io.github.indreshgahoi.queue.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan
@SpringBootApplication
public class QueueGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(QueueGatewayApplication.class, args);
    }
}
