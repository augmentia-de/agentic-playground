package de.augmentia.quad.worker.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Worker metrics for observability and monitoring.
 *
 * <p>Task outcomes are reported on a <em>single</em> counter name,
 * {@code quad_worker_tasks_completed_total}, distinguished by the {@code status} tag
 * ({@code completed}, {@code failed}, …). There is deliberately no separate
 * {@code quad_worker_tasks_failed_total}: the class used to pre-register the failure series on
 * {@code …completed_total{status="failed"}} while the failure path incremented a differently named
 * counter, so dashboards watching the registered series saw a constant zero.
 *
 * Metrics:
 * - quad_worker_tasks_received_total
 * - quad_worker_tasks_completed_total{agentType,status}
 * - quad_worker_task_duration_seconds
 * - quad_worker_tool_errors_total{tool}
 * - quad_worker_dlq_total
 */
@Component
public class WorkerMetrics {

    /** Single outcome counter; failures and successes differ only by the {@code status} tag. */
    static final String TASKS_COMPLETED = "quad_worker_tasks_completed_total";

    private final MeterRegistry registry;

    // Counters (outcome counters are created per tag set on demand)
    public final Counter tasksReceived;
    public final Counter dlqSent;
    public final Counter toolErrors;

    // Timers
    public final Timer taskDuration;
    public final Timer llmCallDuration;

    public WorkerMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.tasksReceived = Counter.builder("quad_worker_tasks_received_total")
            .description("Total tasks received from Kafka")
            .register(registry);

        this.dlqSent = Counter.builder("quad_worker_dlq_total")
            .description("Total messages sent to DLQ")
            .register(registry);

        this.toolErrors = Counter.builder("quad_worker_tool_errors_total")
            .description("Total tool execution errors")
            .register(registry);

        this.taskDuration = Timer.builder("quad_worker_task_duration_seconds")
            .description("Task execution duration")
            .register(registry);

        this.llmCallDuration = Timer.builder("quad_worker_llm_duration_seconds")
            .description("LLM API call duration")
            .register(registry);
    }

    /**
     * Increments the counter for received tasks.
     */
    public void recordTaskReceived() {
        tasksReceived.increment();
    }

    /**
     * Increments the outcome counter for a successfully processed task.
     *
     * @param agentType the agent type (tag {@code agentType})
     * @param status    the outcome status (tag {@code status}), e.g. {@code completed}
     */
    public void recordTaskCompleted(String agentType, String status) {
        outcomeCounter(agentType, status != null ? status : "completed")
            .register(registry)
            .increment();
    }

    /**
     * Increments the outcome counter for a task that produced a failure result.
     *
     * <p>Uses the same series as {@link #recordTaskCompleted} with {@code status="failed"} so that
     * a single query over {@code quad_worker_tasks_completed_total} shows both outcomes. The
     * original failure reason is kept as the {@code errorClass} tag.
     *
     * @param agentType the agent type (tag {@code agentType})
     * @param errorClass the failure reason/status (tag {@code errorClass})
     */
    public void recordTaskFailed(String agentType, String errorClass) {
        outcomeCounter(agentType, "failed")
            .tag("errorClass", errorClass != null ? errorClass : "unknown")
            .register(registry)
            .increment();
    }

    /**
     * Increments the counter for DLQ entries.
     */
    public void recordDlqSent() {
        dlqSent.increment();
    }

    /**
     * Increments the counter for tool errors.
     */
    public void recordToolError(String toolName) {
        Counter.builder("quad_worker_tool_errors_total")
            .tag("tool", toolName != null ? toolName : "unknown")
            .register(registry)
            .increment();
    }

    /**
     * Records a task duration with the Timer.
     */
    public void recordTaskDuration(long durationMs, String agentType, String status) {
        taskDuration.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Records an LLM call duration with the Timer.
     */
    public void recordLlmCallDuration(long durationMs, String modelKey) {
        llmCallDuration.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private Counter.Builder outcomeCounter(String agentType, String status) {
        return Counter.builder(TASKS_COMPLETED)
            .description("Total tasks by outcome")
            .tag("agentType", agentType != null ? agentType : "unknown")
            .tag("status", status);
    }
}
