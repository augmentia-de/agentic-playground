package de.augmentia.quad.worker.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Worker metrics for observability and monitoring.
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

    private final MeterRegistry registry;

    // Counters
    public final Counter tasksReceived;
    public final Counter tasksCompleted;
    public final Counter tasksFailed;
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

        this.tasksCompleted = Counter.builder("quad_worker_tasks_completed_total")
            .description("Total tasks completed successfully")
            .tag("status", "completed")
            .register(registry);

        this.tasksFailed = Counter.builder("quad_worker_tasks_completed_total")
            .description("Total tasks that failed")
            .tag("status", "failed")
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
     * Increments the counter for successful tasks.
     */
    public void recordTaskCompleted(String agentType, String status) {
        Counter.builder("quad_worker_tasks_completed_total")
            .tag("agentType", agentType != null ? agentType : "unknown")
            .tag("status", status != null ? status : "completed")
            .register(registry)
            .increment();
    }

    /**
     * Increments the counter for failed tasks.
     */
    public void recordTaskFailed(String agentType, String errorClass) {
        Counter.builder("quad_worker_tasks_failed_total")
            .tag("agentType", agentType != null ? agentType : "unknown")
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
     * Missed Task Duration mit Timer.
     */
    public void recordTaskDuration(long durationMs, String agentType, String status) {
        taskDuration.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Missed LLM Call Duration mit Timer.
     */
    public void recordLlmCallDuration(long durationMs, String modelKey) {
        llmCallDuration.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
}