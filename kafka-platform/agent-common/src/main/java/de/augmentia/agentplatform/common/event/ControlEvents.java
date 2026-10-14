package de.augmentia.agentplatform.common.event;

import de.augmentia.agentplatform.common.model.HitlAction;

import java.time.Instant;
import java.util.Map;

/**
 * Control-plane events (SPEC2 §4.3 + §15.4/§15.6). These are the non-agent
 * Kafka messages: timeouts, loop advancement, sweep ticks, cancellation, hitl.
 */
public final class ControlEvents {

    private ControlEvents() {}

    /** Delay-queue style timeout event (agent.control.timeouts, keyed task_id). */
    public record TimeoutEvent(
            String taskId,
            String workflowId,
            Instant deadlineAt,
            boolean escalateToHitl
    ) {}

    /** Loop iteration advancement (agent.control.loop, keyed workflow_id, §15.4). */
    public record LoopEvent(
            String workflowId,
            String stepId,
            int iteration,
            boolean exitConditionTrue,
            int maxIterations,
            Map<String, Object> resultData
    ) {}

    /** Periodic deterministic routing sweep tick (agent.control.sweep, keyed sweep_id, §15.6). */
    public record SweepEvent(
            String sweepId,
            Instant generatedAt,
            String scope
    ) {}

    /** Cancellation signal (agent.control.cancellation, keyed workflow_id). */
    public record CancellationEvent(
            String workflowId,
            String reason,
            String actor
    ) {}

    /** Human-in-the-loop escalation / resume (agent.control.hitl, keyed workflow_id, §10.5). */
    public record HitlEvent(
            String workflowId,
            String ticketId,
            String stepId,
            int iteration,
            HitlAction action,
            String decidedBy
    ) {}

    /**
     * Email intake event — the historical email→intake shape.
     *
     * @deprecated no longer produced by the platform. Since WP04 the email path starts workflows
     *     through {@code WorkflowStartService} and publishes a {@link WorkflowStartEvent}, so this
     *     record exists only for compatibility with external consumers that still bind to it.
     */
    @Deprecated
    public record EmailIntakeEvent(
            String workflowId,
            String triggerKey,
            String emailBody,
            java.util.List<String> attachments,
            String initiatorEmail,
            Instant receivedAt
    ) {}
}