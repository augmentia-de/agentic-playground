package de.augmentia.agentplatform.orchestrator.web;

import java.util.Map;

public record StartWorkflowRequest(
        String workflowName,
        Integer version,
        Map<String, Object> inputParams,
        String idempotencyKey
) {}
