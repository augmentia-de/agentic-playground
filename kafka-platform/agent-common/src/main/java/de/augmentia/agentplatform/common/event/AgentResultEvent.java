package de.augmentia.agentplatform.common.event;

import de.augmentia.agentplatform.common.model.ResultStatus;

import java.time.Instant;
import java.util.Map;

/**
 * Result event (SPEC2 §4.3, agent.execution.results).
 */
public record AgentResultEvent(
        ResultHeader header,
        ResultOutput output,
        Usage usage
) {

    public record ResultHeader(
            String workflowId,
            String taskId,
            String agentType,
            ResultStatus status,
            Instant completedAt,
            long executionDurationMs
    ) {}

    public record ResultOutput(
            Map<String, Object> resultData,
            String errorMessage,
            String stackTrace
    ) {}

    public record Usage(
            int promptTokens,
            int completionTokens,
            double totalCostUsd
    ) {}
}