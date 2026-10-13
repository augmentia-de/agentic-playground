package de.augmentia.agentplatform.common.event;

import java.time.Instant;
import java.util.Map;

public record WorkflowStartEvent(
        String workflowExecutionId,
        String workflowDefinitionId,
        String workflowName,
        int workflowVersion,
        Map<String, Object> inputParams,
        String idempotencyKey,
        Instant createdAt
) {}
