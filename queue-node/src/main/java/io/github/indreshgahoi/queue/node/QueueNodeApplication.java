package io.github.indreshgahoi.queue.node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@ConfigurationPropertiesScan
@SpringBootApplication
public class QueueNodeApplication {
    public static void main(String[] args) {
        SpringApplication.run(QueueNodeApplication.class, args);
    }
}
