package de.augmentia.quad.worker.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    /**
     * Single source of truth for the bootstrap servers, shared by the consumer (here) and
     * the manual producer ({@code QuadWorkerConfig.producerConfig}).
     *
     * <p>Precedence: {@code spring.kafka.bootstrap-servers} (which {@code application.yml}
     * maps from {@code KAFKA_BOOTSTRAP_SERVERS}), then the raw {@code KAFKA_BOOTSTRAP_SERVERS}
     * environment variable, then {@code localhost:9092}. Previously the consumer defaulted to
     * {@code kafka:9092} while the producer defaulted to {@code localhost:9092}, so a worker
     * without explicit configuration consumed from a different cluster than it produced to.
     */
    public static String resolveBootstrapServers(org.springframework.core.env.Environment env) {
        String value = env.getProperty("spring.kafka.bootstrap-servers");
        if (value == null || value.isBlank()) {
            value = env.getProperty("KAFKA_BOOTSTRAP_SERVERS");
        }
        return (value == null || value.isBlank()) ? "localhost:9092" : value;
    }

    private String bootstrapServers;
    private String groupId;
    private int maxPollRecords;
    private int maxPollIntervalMs;
    private String autoOffsetReset;

    public KafkaConfig(org.springframework.core.env.Environment env) {
        this.bootstrapServers = resolveBootstrapServers(env);
        this.groupId = env.getProperty("worker.quad.group-id", "quad-workflow-worker");
        this.maxPollRecords = Integer.parseInt(env.getProperty("worker.quad.kafka.max-poll-records", "8"));
        this.maxPollIntervalMs = Integer.parseInt(env.getProperty("worker.quad.kafka.max-poll-interval-ms", "900000"));
        // "earliest", not "latest". Measured: a task produced to a freshly created partition
        // was never executed. The partition only gets assigned after the produce, so the reset
        // starts behind the task and the workflow stalls until the orchestrator's sweep times
        // it out - reported as "Task timeout detected by sweep" although the worker was healthy.
        // This happens for every agent type introduced through /agents/convert, because its
        // task topic is created at import time.
        //
        // The cost of "earliest" is a replay of existing tasks when a worker joins a group for
        // the first time. That is the cheaper direction: the in-process result cache serves a
        // redelivered task without a second LLM call, and the orchestrator discards duplicate
        // results (orchestrator.stale_result_total).
        this.autoOffsetReset = env.getProperty("worker.quad.kafka.auto-offset-reset", "earliest");
    }

    /**
 * Consumer factory for the quad worker.
     * WICHTIG: enable.auto.commit=false + MANUAL AckMode!
     */
    @Bean
    public ConsumerFactory<String, String> quadConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // No "isolation.level=read_committed" here: the worker's producer is NOT transactional
        // (see QuadWorkerConfig.producerConfig), so read_committed would buy nothing while
        // suggesting transactional guarantees that do not exist. The consumer stays on the
        // default (read_uncommitted).

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Batch Listener Factory mit MANUAL Acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> quadBatchListenerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}