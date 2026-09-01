package io.github.indreshgahoi.queue.metadata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

@SpringBootApplication
public class MetadataServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(MetadataServiceApplication.class, args);
    }

    @Bean
    Clock metadataClock() {
        return Clock.systemUTC();
    }
}
