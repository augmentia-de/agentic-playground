package de.augmentia.quad.worker.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import de.augmentia.quad.worker.contract.WorkerJson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes records to the DLQ topic on parsing or validation errors.
 *
 * <p><b>Delivery guarantee:</b> like {@link WorkflowResultPublisher}, {@link #send} waits for the
 * broker to confirm the DLQ write. Only a confirmed DLQ entry lets {@code TaskExecutor} report the
 * malformed record as handled (returning {@code true}, which leads to an ack). If the DLQ write is
 * lost, a fire-and-forget send would silently drop a message that the source record still claims
 * to be handled — a data-loss path that this blocking confirm closes.
 */
public class WorkflowDlqPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDlqPublisher.class);

    private final Producer<String, String> producer;
    private final String dlqTopic;
    private final long sendTimeoutMs;

    public WorkflowDlqPublisher(Producer<String, String> producer, String dlqTopic) {
        this(producer, dlqTopic, WorkflowResultPublisher.DEFAULT_SEND_TIMEOUT_MS);
    }

    public WorkflowDlqPublisher(Producer<String, String> producer, String dlqTopic, long sendTimeoutMs) {
        this.producer = producer;
        this.dlqTopic = dlqTopic;
        this.sendTimeoutMs = sendTimeoutMs > 0 ? sendTimeoutMs : WorkflowResultPublisher.DEFAULT_SEND_TIMEOUT_MS;
    }

    /**
     * Publishes an original record with error information to the DLQ and waits for confirmation.
     *
     * @throws KafkaPublishException if the broker does not confirm the DLQ write in time or rejects it
     */
    public void send(ConsumerRecord<String, String> record, Exception ex) {
        String key = record.key();

        JsonNode dlqEntry = WorkerJson.mapper().createObjectNode()
            .put("original_topic", record.topic())
            .put("original_partition", record.partition())
            .put("original_offset", record.offset())
            .put("original_key", key != null ? key : "")
            .put("original_value", record.value() != null ? record.value() : "")
            .put("error_type", ex.getClass().getName())
            .put("error_message", truncate(ex.getMessage(), 2000))
            .put("timestamp", System.currentTimeMillis());

        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, key,
            WorkerJson.write(dlqEntry));

        log.warn("Sending malformed record to DLQ: topic={}, offset={}", record.topic(), record.offset());
        try {
            producer.send(dlqRecord).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupt) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(
                "Interrupted while waiting for DLQ confirmation: topic=" + record.topic()
                    + ", offset=" + record.offset(), interrupt);
        } catch (TimeoutException timeout) {
            throw new KafkaPublishException(
                "Timed out after " + sendTimeoutMs + "ms waiting for DLQ confirmation: topic="
                    + record.topic() + ", offset=" + record.offset(), timeout);
        } catch (ExecutionException execution) {
            throw new KafkaPublishException(
                "Broker rejected DLQ record: topic=" + record.topic() + ", offset=" + record.offset()
                    + ": " + execution.getCause(), execution);
        }
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen);
    }
}
