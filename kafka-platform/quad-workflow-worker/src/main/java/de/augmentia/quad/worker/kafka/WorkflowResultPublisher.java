package de.augmentia.quad.worker.kafka;

import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.quad.worker.contract.WorkerJson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes ResultEvents on the output topic of the worker.
 */
public class WorkflowResultPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowResultPublisher.class);

    private final Producer<String, String> producer;
    private final String resultTopic;

    public WorkflowResultPublisher(Producer<String, String> producer, String resultTopic) {
        this.producer = producer;
        this.resultTopic = resultTopic;
    }

    /**
     * Publishes a result with workflowId as key.
     * The orchestrator also keys on workflowId for consistent partitioning.
     */
    public void send(String workflowId, ResultEvent result) {
        String key = workflowId; // Consistent keying with orchestrator
        String value = WorkerJson.write(result);
        
        log.debug("Publishing result: workflow={}, taskId={}, step={}", 
            key, result.taskId(), result.stepId());
        
        ProducerRecord<String, String> record = new ProducerRecord<>(resultTopic, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to publish result for workflow {}: {}", key, exception.getMessage());
            }
        });
    }
}