package de.augmentia.agentplatform.orchestrator.web;

public record CreateWorkflowDefinitionResponse(
        String definitionId,
        String name,
        int version
) {}
