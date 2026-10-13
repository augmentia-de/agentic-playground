package de.augmentia.agentplatform.orchestrator.web;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record WorkflowStatusResponse(
        String workflowExecutionId,
        String workflowName,
        int workflowVersion,
        String status,
        Instant startedAt,
        Instant completedAt,
        Map<String, Object> finalOutput,
        List<CheckpointResponse> checkpoints
) {}
