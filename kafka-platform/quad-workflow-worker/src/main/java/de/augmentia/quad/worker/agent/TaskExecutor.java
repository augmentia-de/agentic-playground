package de.augmentia.quad.worker.agent;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.worker.contract.WorkerJson;
import de.augmentia.quad.worker.kafka.WorkflowDlqPublisher;
import de.augmentia.quad.worker.kafka.WorkflowResultPublisher;
import de.augmentia.quad.worker.metrics.WorkerMetrics;
import de.augmentia.quad.worker.result.ResultMapper;
import de.augmentia.quad.worker.session.TaskSessionContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/**
 * Central orchestration logic for executing a single task.
 *
 * Responsibilities:
 * - parsing &amp; validation of the TaskEvent
 * - agent creation and execution with a real LLM call
 * - session binding on the executing thread
 * - timeout handling per task (CompletableFuture.get with timeout)
 * - result mapping and idempotency cache
 * - error classification and DLQ publishing
 *
 * <h3>Threading</h3>
 * The LLM call runs on the supplied {@link Executor} (virtual threads),
 * <em>not</em> on the common ForkJoinPool. Reason: {@code CurrentSession} is a
 * pure {@code ThreadLocal} — the ReAct loop ({@code AgentRuntime}) and the
 * tool injection ({@code ReflectiveToolMethod}) read it directly. Therefore the
 * session is bound inside the asynchronous task, on the thread that
 * actually executes the agent, and released there again.
 *
 * <p>The permit semaphore is held by the asynchronous task, not by the
 * calling thread. A timeout therefore does not end the permit booking: the
 * HTTP call keeps running and holds its permit until it is really done.
 */
public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private final WorkerAgentFactory agentFactory;
    private final TaskPromptBuilder promptBuilder;
    private final TaskSessionContext sessionContext;
    private final ResultMapper resultMapper;
    private final WorkflowResultPublisher results;
    private final WorkflowDlqPublisher dlq;
    private final WorkerMetrics metrics;
    private final Executor agentExecutor;

    // limit for concurrent LLM calls (prevents overloading the provider)
    private final Semaphore taskPermits;

    /**
     * Upper bound for a single task, independent of what the workflow asked for.
     *
     * <p>The listener thread blocks until the whole batch is done, so the batch's wall time is
     * the slowest record. Once that exceeds {@code max.poll.interval.ms} (15 min by default)
     * the consumer is considered dead, Kafka rebalances the group and every task of the
     * partition is redelivered — so one task with an inflated timeout costs a rebalance plus
     * duplicate LLM calls for the whole partition.
     */
    private final long maxTaskTimeoutMs;

    // cache of already computed results: prevents a second LLM call,
    // if the record is redelivered after a rebalance.
    private final Cache<String, String> resultCache;

    public TaskExecutor(
        WorkerAgentFactory agentFactory,
        TaskPromptBuilder promptBuilder,
        TaskSessionContext sessionContext,
        ResultMapper resultMapper,
        WorkflowResultPublisher results,
        WorkflowDlqPublisher dlq,
        WorkerMetrics metrics,
        int maxConcurrentTasks,
        long maxTaskTimeoutMs,
        Executor agentExecutor
    ) {
        this.agentFactory = agentFactory;
        this.promptBuilder = promptBuilder;
        this.sessionContext = sessionContext;
        this.resultMapper = resultMapper;
        this.results = results;
        this.dlq = dlq;
        this.metrics = metrics;
        this.taskPermits = new Semaphore(Math.max(1, maxConcurrentTasks));
        this.maxTaskTimeoutMs = maxTaskTimeoutMs > 0 ? maxTaskTimeoutMs : 600_000L;
        this.agentExecutor = agentExecutor;

        this.resultCache = Caffeine.newBuilder()
            .maximumSize(50_000)
            .expireAfterWrite(Duration.ofHours(1))
            .build();
    }

    /**
     * Safe execution of a task from the Kafka consumer record.
     * Catches all exceptions and publishes to the result/DLQ topic.
     *
     * <p><b>Idempotency:</b> the result cache is checked <em>before</em> the LLM call
      * and filled in <em>before</em> publishing. If the process crashes
      * between the LLM answer and {@code results.send()}, a repeated
     * delivery gets the stored result without paying a second time.
     *
     * <p>The cache is process-local and not persistent — it protects against
     * rebalance/redelivery cases within one instance, not against a
     * failover across instances. That would require a shared store.
     *
     * @return true if the result was published successfully. Only then
     *         the calling listener may acknowledge the record.
     */
    public boolean safeExecute(ConsumerRecord<String, String> record) {
        TaskEvent task = null;
        try {
            // 1. Task parsen + Metrik inkrementieren
            task = WorkerJson.read(record.value(), TaskEvent.class);
            validate(task);
            metrics.recordTaskReceived();
            log.info("Processing task: workflow={}, taskId={}, agentType={}, step={}",
                task.header().workflowId(), task.header().taskId(), task.agentType(), task.stepId());

            // 2. check the result cache BEFORE the LLM call
            String cacheKey = buildCacheKey(task);
            String cached = resultCache.getIfPresent(cacheKey);
            if (cached != null) {
                ResultEvent replay = WorkerJson.read(cached, ResultEvent.class);
                results.send(task.header().workflowId(), replay);
                log.info("Replayed cached result for task={} (no LLM call)", task.header().taskId());
                return true;
            }

            // 3. Execute + Result publish
            ResultEvent result = execute(task);
            resultCache.put(cacheKey, WorkerJson.write(result));
            results.send(task.header().workflowId(), result);

            // 4. metrics for success/error
            String status = result.resultData() != null
                ? (String) result.resultData().get("status") : "unknown";
            if ("COMPLETED".equals(status)) {
                metrics.recordTaskCompleted(task.agentType(), "completed");
            } else {
                metrics.recordTaskFailed(task.agentType(), status);
            }
            return true;
        } catch (Exception ex) {
            log.error("Error executing task from partition {} offset {}: {}",
                record.partition(), record.offset(), ex.getMessage(), ex);
            if (task == null) {
                // Parsing/Validation error -> DLQ
                try {
                    dlq.send(record, ex);
                    metrics.recordDlqSent();
                    return true;
                } catch (Exception dlqEx) {
                    log.error("DLQ send failed for unparseable record from partition {} offset {} — record will be redelivered",
                        record.partition(), record.offset(), dlqEx);
                    return false;
                }
            }
            // Execution error -> Result mit Fehlerstatus
            try {
                publishErrorResult(task, ex, 0);
                metrics.recordTaskFailed(task.agentType(), "execution_error");
                return true;
            } catch (Exception e) {
                log.error("Failed to publish error-result for task {}: {}",
                    task.header().taskId(), e.getMessage());
                try {
                    dlq.send(record, e);
                    metrics.recordDlqSent();
                    return true;
                } catch (Exception dlqEx) {
                    log.error("DLQ send also failed for task {} — record will be redelivered",
                        task.header().taskId(), dlqEx);
                    return false;
                }
            }
        }
    }

    /**
     * Executes the actual task with a real LLM call via the quad-core agent.
     *
     * Architecture:
     * - per task a new agent is instantiated (no singleton agents!)
     * - per task a new QuadToolRegistry copy
     * - per task a new AgentSessionState
     * - session binding on the executing thread, not on the caller
     */
    private ResultEvent execute(TaskEvent task) throws Exception {
        long startedAt = System.currentTimeMillis();

        // timeout from the task, clamped to the worker's ceiling
        Long requested = task.timeoutMs();
        long timeoutMs = (requested == null || requested <= 0) ? 120_000L : requested;
        if (timeoutMs > maxTaskTimeoutMs) {
            // A workflow may ask for more than the poll interval allows. Honouring it would
            // stall the batch past max.poll.interval.ms and trigger a rebalance, so the
            // request is capped and the deviation is visible in the log.
            log.warn("Task timeout of {}ms for workflow={}, taskId={} exceeds the worker limit of "
                    + "{}ms and was reduced. The task will time out earlier than the workflow "
                    + "asked for; raise worker.quad.max-task-timeout-ms (and max.poll.interval.ms) "
                    + "if the workflow really needs that long.",
                    timeoutMs, task.header().workflowId(), task.header().taskId(), maxTaskTimeoutMs);
            timeoutMs = maxTaskTimeoutMs;
        }

        // Prompt bauen
        String prompt = promptBuilder.build(task, null);

        // Session erstellen
        AgentSessionState state = sessionContext.create(task);

        // Agent erstellen (neue Instanz pro Task!)
        var agent = agentFactory.create(task.agentType());

        // The permit is held by the asynchronous task: the caller gives it
        // back when it runs into the timeout. That keeps the number of
        // actually running LLM calls limited to maxConcurrentTasks, even with
        // an HTTP call surviving the task timeout.
        java.util.concurrent.CompletableFuture<AgentResult> future =
            java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                // Uninterruptible: a thread interrupt (shutdown) should not abort the
                // already paid-for LLM call, only let it pass the gate.
                taskPermits.acquireUninterruptibly();
                CurrentSession.setCurrent(state);
                try {
                    return agent.executeStructured(prompt, state);
                } finally {
                    CurrentSession.setCurrent(null);
                    taskPermits.release();
                }
            }, agentExecutor);

        try {
            AgentResult agentResult = future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            return resultMapper.mapSuccess(task, agentResult, state.getSessionId(),
                System.currentTimeMillis() - startedAt);
        } catch (java.util.concurrent.TimeoutException ex) {
            log.warn("Task timeout after {}ms: workflow={}, taskId={}, step={}. The running " +
                    "LLM call is not interrupted (HTTP is not interruptible) and gives up its " +
                    "permit only after actually terminating.",
                timeoutMs, task.header().workflowId(), task.header().taskId(), task.stepId());
            future.cancel(false);
            return resultMapper.mapFailure(task, ex,
                System.currentTimeMillis() - startedAt);
        } catch (java.util.concurrent.ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof Exception e) {
                return resultMapper.mapFailure(task, e, System.currentTimeMillis() - startedAt);
            }
            return resultMapper.mapFailure(task, new IllegalStateException("Agent execution failed", cause),
                System.currentTimeMillis() - startedAt);
        } catch (InterruptedException ex) {
            // thread was interrupted (shutdown, cancellation). Restore the interrupt flag
            // and report it as an error result — the calling
            // listener then does not acknowledge the record.
            Thread.currentThread().interrupt();
            log.warn("Task execution interrupted: workflow={}, taskId={}",
                task.header().workflowId(), task.header().taskId());
            return resultMapper.mapFailure(task, ex, System.currentTimeMillis() - startedAt);
        }
    }

    private void validate(TaskEvent task) {
        if (task == null) throw new IllegalArgumentException("Task must not be null");
        if (task.header() == null) throw new IllegalArgumentException("Task header must not be null");
        if (task.header().workflowId() == null || task.header().workflowId().isEmpty()) {
            throw new IllegalArgumentException("Workflow ID must not be empty");
        }
        if (task.header().taskId() == null || task.header().taskId().isEmpty()) {
            throw new IllegalArgumentException("Task ID must not be empty");
        }
        if (task.agentType() == null || task.agentType().isEmpty()) {
            throw new IllegalArgumentException("Agent type must not be empty");
        }
    }

    /**
     * Publishes an error result to the orchestrator.
     * If publish fails -> DLQ as the last resort.
     */
    private void publishErrorResult(TaskEvent task, Exception ex, long durationMs) {
        ResultEvent errResult = resultMapper.mapFailure(task, ex, durationMs);
        results.send(task.header().workflowId(), errResult);
    }

    private String buildCacheKey(TaskEvent task) {
        return String.join(":",
            task.header().workflowId(),
            task.header().taskId(),
            String.valueOf(task.header().attempt())
        );
    }
}