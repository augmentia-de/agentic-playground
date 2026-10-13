package de.augmentia.quad.worker.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.ClassPathResource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Worker-Properties gegen die ausgelieferte application.yml")
class WorkerPropertiesBindingTest {

    /**
     * Binds {@code worker.quad} from the shipped {@code application.yml} without starting the
     * application. A full {@code @SpringBootTest} would need a broker, because the Kafka
     * listener is not conditional on {@code worker.quad.enabled}.
     */
    private static WorkerProperties shippedDefaults() throws java.io.IOException {
        var loaded = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        var sources = new MutablePropertySources();
        loaded.forEach(sources::addLast);
        var bound = new Binder(ConfigurationPropertySources.from(sources))
                .bind("worker.quad", WorkerProperties.class)
                .orElseGet(() -> new WorkerProperties(false, "quad-workflow-worker",
                        "agent\\.tasks\\..*", "agent.execution.results", "agent.execution.dlq",
                        1, 1, 0L, "/tmp", Map.of(), null));
        return bound;
    }

    @Test
    @DisplayName("worker.quad.kafka ist belegt, obwohl die YAML keinen Abschnitt dafür hat")
    void kafkaSectionIsNeverNull() throws java.io.IOException {
        // taskExecutor() reads props.kafka().maxTaskTimeoutMs() while building the bean. With a
        // null section the worker refuses to start with an NPE — and only a real start-up
        // exposed it, because hand-built WorkerProperties in the unit tests always passed one.
        assertThat(shippedDefaults().kafka()).isNotNull();
    }

    @Test
    @DisplayName("der Task-Timeout bleibt unter max.poll.interval.ms")
    void taskTimeoutStaysBelowPollInterval() throws java.io.IOException {
        var kafka = shippedDefaults().kafka();

        assertThat(kafka.maxTaskTimeoutMs()).isPositive();
        assertThat(kafka.maxTaskTimeoutMs())
                .as("the listener blocks until the whole batch is done; a task longer than that "
                        + "interval evicts the consumer and replays the whole partition")
                .isLessThan((long) kafka.maxPollIntervalMs());
    }

    @Test
    @DisplayName("agents ist nie null")
    void agentsSectionIsNeverNull() throws java.io.IOException {
        assertThat(shippedDefaults().agents()).isNotNull();
    }

    @Test
    @DisplayName("der Consumer startet bei earliest, nicht bei latest")
    void autoOffsetResetIsEarliest() {
        // A task produced to a freshly created partition is only assigned afterwards, so a
        // reset to "latest" starts behind it and the task is never executed. Measured live: the
        // workflow stalled until the sweep reported a timeout although the worker was healthy,
        // and the identical run succeeded once the assignment had settled. The consequences
        // are asymmetric - a replayed task is served from the result cache, a skipped one fails
        // the workflow.
        assertThat(new WorkerProperties.KafkaConfig().autoOffsetReset()).isEqualTo("earliest");
    }
}