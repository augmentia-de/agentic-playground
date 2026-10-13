package de.augmentia.agentplatform.orchestrator.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for a bug that was only visible at runtime.
 *
 * <p>{@code AutoScalingService.computeTotalLag} read {@code bootstrap.servers} by casting
 * it to {@code String}. The Kafka client however delivers the value as
 * {@code List<String>}. Consequence: every 30 seconds a
 * {@code ClassCastException}, the evaluation never ran — auto-scaling had failed completely
 * without a single test noticing.
 */
@DisplayName("AutoScalingService — bootstrap server from consumer properties")
class AutoScalingServiceBootstrapTest {

    @Test
    @DisplayName("The Kafka client returns a list — no cast to String")
    void listIsHandled() {
        assertThat(AutoScalingService.firstBootstrapServer(
                Map.of("bootstrap.servers", List.of("localhost:9093"))))
                .isEqualTo("localhost:9093");
    }

    @Test
    @DisplayName("Mehrere Broker: der erste zaehlt")
    void firstOfSeveralBrokers() {
        assertThat(AutoScalingService.firstBootstrapServer(
                Map.of("bootstrap.servers", List.of("a:9092", "b:9092", "c:9092"))))
                .isEqualTo("a:9092");
    }

    @Test
    @DisplayName("A plain String is still accepted")
    void plainStringStillWorks() {
        assertThat(AutoScalingService.firstBootstrapServer(
                Map.of("bootstrap.servers", "localhost:9092")))
                .isEqualTo("localhost:9092");
    }

    @Test
    @DisplayName("If the value is missing, the default applies instead of an NPE")
    void missingValueFallsBack() {
        assertThat(AutoScalingService.firstBootstrapServer(Map.of()))
                .isEqualTo("localhost:9092");
        assertThat(AutoScalingService.firstBootstrapServer(
                Map.of("bootstrap.servers", List.of())))
                .isEqualTo("localhost:9092");
    }

    @Test
    @DisplayName("Real ConsumerFactory properties do not throw")
    void realConsumerFactoryPropertiesDoNotThrow() {
        // Reproduces the production case: the properties come from an
        // actual DefaultKafkaConsumerFactory, not from a hand-built map.
        //
        // Deliberately WITHOUT group.id — that is exactly how it looks in production. Spring Boot
        // sets no global spring.kafka.consumer.group-id, and the groups of the
        // listeners sit on the @KafkaListener annotation. Such a group.id
        // as it stood here previously masked the real bug:
        // computeTotalLag needed a group and got none.
        var factory = new DefaultKafkaConsumerFactory<>(
                Map.of("bootstrap.servers", List.of("localhost:9093"),
                       "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                       "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"));

        Map<String, Object> props = new HashMap<>(factory.getConfigurationProperties());

        assertThatCode(() -> AutoScalingService.firstBootstrapServer(props))
                .doesNotThrowAnyException();
        assertThat(AutoScalingService.firstBootstrapServer(props))
                .isEqualTo("localhost:9093");

        // And the admin config comes out without a group — that was previously the reason
        // for the InvalidGroupIdException.
        assertThat(props).as("Production props carry no group").doesNotContainKey("group.id");
        assertThat(AutoScalingService.adminProperties(props))
                .containsOnlyKeys("bootstrap.servers")
                .containsEntry("bootstrap.servers", "localhost:9093");
    }
}
