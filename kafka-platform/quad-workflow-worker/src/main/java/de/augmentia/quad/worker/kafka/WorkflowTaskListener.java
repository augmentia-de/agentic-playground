package de.augmentia.quad.worker.kafka;

import de.augmentia.quad.worker.agent.TaskExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Kafka listener that consumes tasks from agent.tasks.* topics and executes them in
 * parallel on virtual threads.
 *
 * Flow:
 * 1. Poll the batch (max.poll.records = 8 by default)
 * 2. Execute all records in parallel on virtual threads
 * 3. Per record: publish the result, or the DLQ on parsing errors
 * 4. Only acknowledge the successful prefix of the batch
 *
 * <h3>Ack semantics</h3>
 * MANUAL acknowledgment for at-least-once. Only the
 * <em>contiguous success prefix</em> of the batch is acknowledged: as soon as a record
 * fails, nothing from that position onwards is acknowledged, so that Kafka redelivers
 * the failed record and all following ones. An
 * {@code ack.acknowledge()} without an index would acknowledge the whole batch and
 * lose a failed record permanently.
 *
 * <h3>Concurrency</h3>
 * Parallelism is controlled via partitions and instances, not via
 * manual concurrency. The executor is passed through to the {@link TaskExecutor}
 * so that the LLM call runs on the same virtual thread pool
 * and the {@code CurrentSession} bound there is visible.
 *
 * <p>Topic routing (item 2, O1): wildcard pattern {@code agent\.tasks\..*} — covers ALL
 * agent types that the orchestrator can dispatch dynamically per agentType
 * (including future ones, e.g. with a tenant suffix), without maintaining a static topic list.
 *
 * <h3>Poll interval</h3>
 * The calling thread blocks until the entire batch is done. So that the
 * resulting blocking time stays below {@code max.poll.interval.ms},
 * the task timeout must be smaller than that interval. The check
 * happens during dispatch in the orchestrator ({@code TaskDispatcher}).
 */
@Component
public class WorkflowTaskListener {

    private static final Logger log = LoggerFactory.getLogger(WorkflowTaskListener.class);

    private final TaskExecutor taskExecutor;
    private final ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();

    public WorkflowTaskListener(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * Batch listener for all task topics via a wildcard pattern.
     * Uses MANUAL acknowledgment for at-least-once semantics.
     */
    @KafkaListener(
        topicPattern = "${worker.quad.task-topic-pattern:agent\\.tasks\\..*}",
        groupId = "${worker.quad.group-id:quad-workflow-worker}",
        containerFactory = "quadBatchListenerFactory"
    )
    public void onTasks(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records.isEmpty()) {
            return;
        }

        log.debug("Received batch of {} tasks from topics {}",
            records.size(), records.stream().map(ConsumerRecord::topic).distinct().collect(Collectors.joining(", ")));

        int size = records.size();
        boolean[] delivered = new boolean[size];

        // execute all records in parallel on virtual threads
        CompletableFuture<?>[] futures = new CompletableFuture<?>[size];
        for (int i = 0; i < size; i++) {
            final int index = i;
            futures[i] = CompletableFuture.runAsync(() -> delivered[index] = safeExecute(records.get(index)), virtualThreads);
        }

        // wait until ALL records are finished (not just one!)
        try {
            CompletableFuture.allOf(futures).join();
        } catch (RuntimeException ex) {
            // allOf throws if a future completed with a throwable.
            // derive the delivery status from the array instead of losing the batch.
            log.error("Batch completed with exceptional future", ex);
        }

        ackDeliveredPrefix(ack, records, delivered);
    }

    /**
     * Acknowledges only the contiguous success prefix of the batch.
     * From the first failed record onwards the rest stays unacknowledged and is
     * redelivered on the next poll.
     */
    private void ackDeliveredPrefix(Acknowledgment ack, List<ConsumerRecord<String, String>> records, boolean[] delivered) {
        int firstFailure = -1;
        for (int i = 0; i < delivered.length; i++) {
            if (!delivered[i]) {
                firstFailure = i;
                break;
            }
        }

        if (firstFailure < 0) {
            ack.acknowledge();
            log.debug("Acknowledged batch of {} tasks", records.size());
            return;
        }

        log.warn("Batch not fully delivered: first failure at index {} of {} (partition {} offset {}). " +
                 "Records from that position will be redelivered.",
            firstFailure, records.size(),
            records.get(firstFailure).partition(), records.get(firstFailure).offset());

        if (firstFailure > 0) {
            // acknowledge(index) confirms up to and including the index -> the prefix before it
            ack.acknowledge(firstFailure - 1);
        }
        // firstFailure == 0: acknowledge nothing, the whole batch comes back.
    }

    /**
     * Executes a record and returns true if the result or the DLQ entry
     * was published successfully. Exceptions in themselves are not a failure — they
     * mean that the task is not complete, so the record has to be redelivered.
     */
    private boolean safeExecute(ConsumerRecord<String, String> record) {
        try {
            return taskExecutor.safeExecute(record);
        } catch (Throwable ex) {
            log.error("Critical error processing record from partition {} offset {}: {} — will be redelivered",
                record.partition(), record.offset(), ex.getMessage(), ex);
            return false;
        }
    }
}