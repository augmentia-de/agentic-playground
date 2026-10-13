package de.augmentia.agentplatform.common.event;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Task assignment event (SPEC2 §4.3, agent.tasks.<agent_type>).
 */
public record AgentTaskEvent(
        TaskHeader header,
        AgentConfig agentConfig,
        TaskPayload payload
) {

    public record TaskHeader(
            String workflowId,
            String taskId,
            String parentTaskId,
            String traceParent,
            int attempt,
            int maxAttempts,
            Instant createdAt,
            long timeoutMs
    ) {}

    public record AgentConfig(
            String agentType,
            int version,
            String model,
            double temperature
    ) {}

    public record TaskPayload(
            String inputPrompt,
            List<String> contextReferences,
            Map<String, Object> parameters
    ) {}
}