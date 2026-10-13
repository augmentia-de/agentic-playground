package de.augmentia.quad.worker.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import de.augmentia.quad.worker.contract.WorkerJson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes records to the DLQ topic on parsing or validation errors.
 */
public class WorkflowDlqPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDlqPublisher.class);

    private final Producer<String, String> producer;
    private final String dlqTopic;

    public WorkflowDlqPublisher(Producer<String, String> producer, String dlqTopic) {
        this.producer = producer;
        this.dlqTopic = dlqTopic;
    }

    /**
     * Publishes an original record with error information to the DLQ.
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
        producer.send(dlqRecord);
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen);
    }
}