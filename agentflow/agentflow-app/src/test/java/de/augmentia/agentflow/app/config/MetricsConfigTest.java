package de.augmentia.agentflow.app.config;

import de.augmentia.quad.core.observability.AgentMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsConfigTest {

    @Test
    void providesMicrometerBackedAgentMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AgentMetrics metrics = new MetricsConfig().agentMetrics(registry);

        metrics.getLlmCalls().increment();
        metrics.getToolExecutions().increment(2);
        metrics.getErrors().increment();

        assertThat(metrics.getLlmCalls().count()).isEqualTo(1.0);
        assertThat(metrics.getToolExecutions().count()).isEqualTo(2.0);
        assertThat(metrics.getErrors().count()).isEqualTo(1.0);
    }
}
