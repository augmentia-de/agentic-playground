package de.augmentia.agentplatform.orchestrator.web;

public record StartWorkflowResponse(
        String workflowExecutionId,
        boolean duplicate
) {}
