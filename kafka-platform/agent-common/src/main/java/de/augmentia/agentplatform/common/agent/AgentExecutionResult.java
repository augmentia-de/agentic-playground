package de.augmentia.agentplatform.common.agent;

import de.augmentia.agentplatform.common.model.ResultStatus;

import java.util.Map;

public record AgentExecutionResult(
        ResultStatus status,
        Map<String, Object> resultData,
        String errorMessage,
        String stackTrace,
        int promptTokens,
        int completionTokens,
        double totalCostUsd
) {

    public static AgentExecutionResult completed(Map<String, Object> resultData) {
        return new AgentExecutionResult(ResultStatus.COMPLETED, resultData == null ? Map.of() : resultData,
                null, null, 0, 0, 0.0d);
    }

    public static AgentExecutionResult failedRetryable(String errorMessage) {
        return new AgentExecutionResult(ResultStatus.FAILED_RETRYABLE, Map.of(), errorMessage, null, 0, 0, 0.0d);
    }

    public static AgentExecutionResult failedFatal(String errorMessage) {
        return new AgentExecutionResult(ResultStatus.FAILED_FATAL, Map.of(), errorMessage, null, 0, 0, 0.0d);
    }

    public static AgentExecutionResult needsHitl(String reason, Map<String, Object> resultData) {
        return new AgentExecutionResult(ResultStatus.NEEDS_HITL, resultData == null ? Map.of() : resultData,
                reason, null, 0, 0, 0.0d);
    }
}
