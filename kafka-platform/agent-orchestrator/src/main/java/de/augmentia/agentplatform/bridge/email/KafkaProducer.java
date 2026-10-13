package de.augmentia.agentplatform.bridge.email;

import de.augmentia.agentplatform.common.event.EventJson;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Simple async Kafka publisher with error handling and metadata tracking.
 *
 * <p>Deliberately serialises via {@link EventJson} and not via an own
 * {@code com.fasterxml.jackson.databind.ObjectMapper}. The project otherwise uses
 * consistently Jackson 3 via {@code EventJson}; a second, separately
 * configured mapper would have meant two serialisations with potentially
 * different settings — and it was the cause of the fact that the
 * application did not start because no Jackson 2 bean existed.
 */
@Component
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish a serializable event to a topic with the given key.
     */
    public void publish(String topic, String key, Object value) {
        final String payload;
        try {
            payload = EventJson.write(value);
        } catch (RuntimeException e) {
            log.error("Failed to serialize event for topic {}: {}", topic, e.getMessage());
            return;
        }
        kafkaTemplate.send(topic, key, payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish to {}: {}", topic, ex.getMessage());
                    } else {
                        RecordMetadata meta = result.getRecordMetadata();
                        log.debug("Published to {} partition={} offset={}", meta.topic(), meta.partition(), meta.offset());
                    }
                });
    }

    /**
     * Publish without a key (null key).
     */
    public void publish(String topic, Object value) {
        publish(topic, null, value);
    }
}
