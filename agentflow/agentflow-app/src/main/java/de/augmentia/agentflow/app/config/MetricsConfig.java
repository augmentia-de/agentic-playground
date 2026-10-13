package de.augmentia.agentflow.app.config;

import de.augmentia.quad.core.observability.AgentMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Micrometer-backed AgentMetrics. Counters/timers are exposed on the Spring Boot actuator
 * endpoint {@code /actuator/prometheus}.
 */
@Configuration
public class MetricsConfig {

    @Bean
    public AgentMetrics agentMetrics(MeterRegistry registry) {
        return new AgentMetrics(registry);
    }
}
