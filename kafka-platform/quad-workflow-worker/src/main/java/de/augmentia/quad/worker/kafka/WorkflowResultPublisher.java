package de.augmentia.quad.worker.kafka;

import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.quad.worker.contract.WorkerJson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes ResultEvents on the output topic of the worker.
 *
 * <p><b>Delivery guarantee:</b> {@link #send} blocks until the broker has confirmed the write
 * (or fails). Only then does the caller {@code TaskExecutor.safeExecute} report success and the
 * listener acknowledge the source record. A fire-and-forget {@code producer.send(record, callback)}
 * would acknowledge before the broker ever stored the result — a broker outage would then lose
 * the result of an already executed (and paid-for) task without any redelivery.
 */
public class WorkflowResultPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowResultPublisher.class);

    /** Upper bound for a single synchronous confirm; keeps the batch below max.poll.interval.ms. */
    public static final long DEFAULT_SEND_TIMEOUT_MS = 30_000L;

    private final Producer<String, String> producer;
    private final String resultTopic;
    private final long sendTimeoutMs;

    public WorkflowResultPublisher(Producer<String, String> producer, String resultTopic) {
        this(producer, resultTopic, DEFAULT_SEND_TIMEOUT_MS);
    }

    public WorkflowResultPublisher(Producer<String, String> producer, String resultTopic, long sendTimeoutMs) {
        this.producer = producer;
        this.resultTopic = resultTopic;
        this.sendTimeoutMs = sendTimeoutMs > 0 ? sendTimeoutMs : DEFAULT_SEND_TIMEOUT_MS;
    }

    /**
     * Publishes a result with workflowId as key and waits for broker confirmation.
     * The orchestrator also keys on workflowId for consistent partitioning.
     *
     * @throws KafkaPublishException if the broker does not confirm the write in time or rejects it
     */
    public void send(String workflowId, ResultEvent result) {
        String key = workflowId; // Consistent keying with orchestrator
        String value = WorkerJson.write(result);

        log.debug("Publishing result: workflow={}, taskId={}, step={}",
            key, result.taskId(), result.stepId());

        ProducerRecord<String, String> record = new ProducerRecord<>(resultTopic, key, value);
        try {
            producer.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
            log.debug("Result confirmed by broker: workflow={}, taskId={}", key, result.taskId());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(
                "Interrupted while waiting for result confirmation: workflow=" + key, ex);
        } catch (TimeoutException ex) {
            throw new KafkaPublishException(
                "Timed out after " + sendTimeoutMs + "ms waiting for result confirmation: workflow=" + key, ex);
        } catch (ExecutionException ex) {
            throw new KafkaPublishException(
                "Broker rejected result: workflow=" + key + ": " + ex.getCause(), ex);
        }
    }
}
