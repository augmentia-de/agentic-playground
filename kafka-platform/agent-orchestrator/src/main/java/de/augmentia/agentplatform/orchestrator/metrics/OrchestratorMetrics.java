package de.augmentia.agentplatform.orchestrator.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

/**
 * Custom Prometheus metrics for the Kafka orchestrator.
 * Published at {@code /actuator/prometheus}.
 */
@Component
public class OrchestratorMetrics {

    private final MeterRegistry registry;

    public OrchestratorMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // ── Counters ───────────────────────────────────────────────────────

    /** Total workflows started (per status tag). */
    public Counter workflowsStartedCounter(String status) {
        return Counter.builder("orchestrator.workflows_started")
                .tag("status", status)
                .register(registry);
    }

    /** Total routing decisions made (per node type tag). */
    public Counter routingDecisionsCounter(String nodeType, String decision) {
        return Counter.builder("orchestrator.routing_decisions")
                .tag("node_type", nodeType)
                .tag("decision", decision)
                .register(registry);
    }

    /** Total loop iterations executed (per workflow name). */
    public Counter loopIterationsCounter(String workflowName) {
        return Counter.builder("orchestrator.loop_iterations")
                .tag("workflow_name", workflowName)
                .register(registry);
    }

    /** Total checkpoints persisted. */
    public Counter checkpointsPersistedCounter() {
        return Counter.builder("orchestrator.checkpoints_persisted")
                .register(registry);
    }

    /** Total HITL tickets created (per reason tag). */
    public Counter hitlTicketsCreatedCounter(String reason) {
        return Counter.builder("orchestrator.hitl_tickets_created")
                .tag("reason", reason)
                .register(registry);
    }

    /** Total DLQ messages enqueued (per error type tag). */
    public Counter dlqEnqueuedCounter(String errorType) {
        return Counter.builder("orchestrator.dlq_enqueued")
                .tag("error_type", errorType)
                .register(registry);
    }

    /**
     * Total retry attempts (per agent type).
     *
     * <p><b>Important:</b> the label must <em>not</em> be the {@code executionId}.
     * One UUID per workflow execution creates a new time series per
     * execution — unbounded cardinality, Prometheus runs until OOM. The
     * execution can be traced via the log (with {@code executionId}).
     */
    public Counter retryAttemptsTotal(String agentType) {
        return Counter.builder("orchestrator.retry_attempts_total")
                .tag("agent_type", agentType == null ? "unknown" : agentType)
                .register(registry);
    }

    /** Routing decisions total per trigger type (§19.2). */
    public Counter routingDecisionsTotal(String triggerType) {
        return Counter.builder("orchestrator.routing_decisions_total")
                .tag("trigger_type", triggerType)
                .register(registry);
    }

    /** Loop iteration total per step ID (§19.2). */
    public Counter loopIterationTotal(String stepId) {
        return Counter.builder("orchestrator.loop_iteration_total")
                .tag("step_id", stepId)
                .register(registry);
    }

    /** Loop exit reason total (§19.2). */
    public Counter loopExitReasonTotal(String reason) {
        return Counter.builder("orchestrator.loop_exit_reason_total")
                .tag("reason", reason)
                .register(registry);
    }

    /** Human-in-the-loop ticket escalation total (§19.2). */
    public Counter hitlTicketEscalationTotal(String reason) {
        return Counter.builder("orchestrator.hitl_ticket_escalation_total")
                .tag("reason", reason)
                .register(registry);
    }

    /** Pruning steps total (§19.2). */
    public Counter pruningStepsTotal(String pruneReason) {
        return Counter.builder("orchestrator.pruning_steps_total")
                .tag("prune_reason", pruneReason)
                .register(registry);
    }

    /**
     * Results that were discarded because their checkpoint was
     * no longer IN_PROGRESS (late, duplicated or after abort).
     *
     * <p>Makes an otherwise invisible path measurable: previously such
     * results were discarded without comment, which made a silent result loss
     * indistinguishable from an expected duplicate.
     *
     * @param reason {@code superseded}, {@code no_checkpoint} or {@code not_in_progress}
     */
    public Counter staleResultTotal(String reason) {
        return Counter.builder("orchestrator.stale_result_total")
                .tag("reason", reason)
                .register(registry);
    }

    // ── Timers / Summaries ─────────────────────────────────────────────

    /** Workflow execution duration histogram. */
    public void recordWorkflowDurationSeconds(double seconds) {
        DistributionSummary.builder("orchestrator.workflow_duration_seconds")
                .baseUnit("seconds")
                .register(registry)
                .record(seconds);
    }

    /** Routing evaluation duration histogram. */
    public void recordRoutingEvaluationMs(long durationMs) {
        DistributionSummary.builder("orchestrator.routing_evaluation_ms")
                .baseUnit("milliseconds")
                .register(registry)
                .record((double) durationMs);
    }

    /** DAG validation duration histogram. */
    public void recordValidationMs(long durationMs) {
        DistributionSummary.builder("orchestrator.validation_duration_ms")
                .baseUnit("milliseconds")
                .register(registry)
                .record((double) durationMs);
    }

    /** JOIN wait duration in seconds (§19.2). */
    public void joinWaitDurationSeconds(double seconds) {
        DistributionSummary.builder("orchestrator.join_wait_duration_seconds")
                .baseUnit("seconds")
                .register(registry)
                .record(seconds);
    }

    // ── Gauges ─────────────────────────────────────────────────────────

    /** Currently open HITL ticket count. */
    public void gaugeHitlOpenCount(long count) {
        registry.gauge("orchestrator.hitl_open_count", Tags.empty(), count);
    }

    /** Stale executions detected by sweep reaper. */
    public void gaugeStaleExecutionsFound(long count) {
        registry.gauge("orchestrator.stale_executions_found", Tags.empty(), count);
    }

    /** Worker load score (average partition lag). */
    public void gaugeWorkerLoadScore(double score) {
        registry.gauge("orchestrator.worker_load_score", Tags.empty(), score);
    }

    /** Target worker count for autoscaling. */
    public void gaugeWorkerScaleTarget(int target) {
        registry.gauge("orchestrator.worker_scale_target", Tags.empty(), target);
    }

    // ── Convenience helpers ────────────────────────────────────────────

    /** Record a complete workflow start event (counter + timer). */
    public void recordWorkflowStarted(String workflowName, long durationMs) {
        workflowsStartedCounter("started").increment();
        recordWorkflowDurationSeconds(durationMs / 1000.0);
    }

    /** Record a routing decision for metrics dashboards. */
    public void recordRoutingDecision(String nodeType, String outcome, long durationMs) {
        routingDecisionsCounter(nodeType, outcome).increment();
        recordRoutingEvaluationMs(durationMs);
    }
}
