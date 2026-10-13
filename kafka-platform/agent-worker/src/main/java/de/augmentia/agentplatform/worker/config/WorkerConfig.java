package de.augmentia.agentplatform.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class WorkerConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
