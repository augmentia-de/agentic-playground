package de.augmentia.agentplatform.orchestrator.web;

import java.time.Instant;
import java.util.Map;

public record CheckpointResponse(
        String stepId,
        int iteration,
        String nodeType,
        String agentType,
        String status,
        String decisionBranch,
        String taskId,
        int attempt,
        int retryCount,
        long timeoutMs,
        String errorMessage,
        Instant startedAt,
        Instant completedAt,
        Map<String, Object> output
) {}
