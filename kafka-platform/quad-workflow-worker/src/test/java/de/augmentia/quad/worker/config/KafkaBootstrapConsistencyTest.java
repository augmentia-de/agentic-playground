package de.augmentia.quad.worker.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP03/B39 — producer and consumer must target the same cluster and agree on the
 * (non-transactional) isolation reality.
 */
class KafkaBootstrapConsistencyTest {

    @Test
    void consumerUsesSpringBootstrapWhenSet() {
        var env = new MockEnvironment().withProperty("spring.kafka.bootstrap-servers", "broker:9092");
        var props = new KafkaConfig(env).quadConsumerFactory().getConfigurationProperties();

        assertThat(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("broker:9092");
    }

    @Test
    void consumerAndProducerUseTheSameBootstrap() {
        var env = new MockEnvironment().withProperty("spring.kafka.bootstrap-servers", "shared:9092");

        var consumer = new KafkaConfig(env).quadConsumerFactory().getConfigurationProperties()
            .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        var producer = new QuadWorkerConfig().producerConfig(env)
            .get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        assertThat(producer).isEqualTo("shared:9092");
        assertThat(consumer).isEqualTo(producer);
    }

    @Test
    void bootstrapDefaultsAreConsistent() {
        var env = new MockEnvironment();

        var consumer = new KafkaConfig(env).quadConsumerFactory().getConfigurationProperties()
            .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        var producer = new QuadWorkerConfig().producerConfig(env)
            .get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        // the old defaults diverged: consumer "kafka:9092" vs. producer "localhost:9092"
        assertThat(consumer).isEqualTo("localhost:9092");
        assertThat(producer).isEqualTo(consumer);
    }

    @Test
    void consumerDoesNotClaimReadCommittedWithoutATransactionalProducer() {
        var env = new MockEnvironment();
        var props = new KafkaConfig(env).quadConsumerFactory().getConfigurationProperties();

        assertThat(props.get("isolation.level")).isNotEqualTo("read_committed");
    }
}
