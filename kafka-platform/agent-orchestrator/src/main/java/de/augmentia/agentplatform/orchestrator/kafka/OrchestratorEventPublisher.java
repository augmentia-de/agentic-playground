package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.orchestrator.tracing.TraceHeaders;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes orchestrator events to Kafka.
 *
 * <p><b>Transaction behaviour:</b> {@link #publishAfterCommit(String, String, String)}
 * holds the message back until the calling transaction has committed.
 * Without an active transaction it publishes immediately. That is important because
 * {@code WorkflowEngine} writes checkpoints and dispatches in one transaction
 * — a Kafka publish before the commit could announce tasks whose
 * checkpoint is then rolled back.
 *
 * <p><b>Tracing:</b> the W3C traceparent is set here, not via a
 * Kafka client interceptor. The latter is created by Kafka reflectively with a
 * no-arg constructor and cannot obtain a Spring bean with an injected tracer
 * at all. Without a tracer or an active span no header is set —
 * tracing is optional.
 */
@Component
public class OrchestratorEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorEventPublisher.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Tracer tracer;

    public OrchestratorEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                      ObjectProvider<Tracer> tracerProvider) {
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracerProvider.getIfAvailable();
        if (this.tracer == null) {
            log.info("No Micrometer tracer configured - traceparent headers will not be set.");
        }
    }

    public void publish(String topic, String key, String payload) {
        try {
            kafkaTemplate.send(record(topic, key, payload))
                    .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing to " + topic, ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Failed publishing to " + topic, ex);
        }
    }

    public void publishAfterCommit(String topic, String key, String payload) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            publish(topic, key, payload);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    publish(topic, key, payload);
                } catch (RuntimeException ex) {
                    log.error("Post-commit Kafka publish failed topic={} key={}", topic, key, ex);
                }
            }
        });
    }

    private ProducerRecord<String, String> record(String topic, String key, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        if (tracer == null) {
            return record;
        }
        var existing = new java.util.ArrayList<org.apache.kafka.common.header.Header>();
        for (org.apache.kafka.common.header.Header h : record.headers()) {
            existing.add(h);
        }
        var enriched = TraceHeaders.enrich(tracer.currentSpan(), key, existing);
        return new ProducerRecord<>(topic, record.partition(), record.timestamp(), key, payload,
                new org.apache.kafka.common.header.internals.RecordHeaders(enriched));
    }
}
