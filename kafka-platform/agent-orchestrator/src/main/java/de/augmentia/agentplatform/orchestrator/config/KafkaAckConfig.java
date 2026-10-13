package de.augmentia.agentplatform.orchestrator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Container factory for listeners that control their acknowledgement themselves.
 *
 * <p>{@code IntakeListener.handleIntake} and
 * {@code ConfigUpdateConsumer.handleConfigUpdate} deliberately acknowledge <em>even in
 * the error case</em>, so that a record that cannot be processed again does not cause
 * a reprocessing loop. That is only possible if the container
 * runs in {@code MANUAL} mode and the
 * {@link org.springframework.kafka.support.Acknowledgment} is populated at all.
 *
 * <p>Under the standard factory {@code AckMode.RECORD} applies
 * ({@code spring.kafka.listener.ack-mode} in {@code application.yml}, resp.
 * explicitly in {@link KafkaTlsSecurityConfig}). There the Acknowledgment parameter
 * is not available, and {@code acknowledge()} throws
 * {@code IllegalStateException: No Acknowledgment available as an argument …}.
 * The record then falls into the container error handling and is
 * redelivered — ten times in operation, then onto the error path.
 *
 * <p>This class is deliberately <em>unconditional</em>: in the configuration for
 * {@code profile: tls} the standard factory is overridden by
 * {@link KafkaTlsSecurityConfig}, and a factory that is not replaced there
 * does not apply, it is missing in all other profiles. The listener would then
 * fail already at startup.
 */
@Configuration
public class KafkaAckConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaAckConfig.class);

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualAckListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(1);
        log.debug("manualAckListenerContainerFactory: AckMode.MANUAL");
        return factory;
    }
}
