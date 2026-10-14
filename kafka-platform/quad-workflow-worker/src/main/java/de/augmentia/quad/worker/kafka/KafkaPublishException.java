package de.augmentia.quad.worker.kafka;

/**
 * Thrown when a Kafka record could not be confirmed as written (broker error, timeout or
 * interruption). The worker treats this as "not delivered": the calling listener does not
 * acknowledge the source record, so it is redelivered instead of being lost.
 */
public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String message) {
        super(message);
    }

    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
