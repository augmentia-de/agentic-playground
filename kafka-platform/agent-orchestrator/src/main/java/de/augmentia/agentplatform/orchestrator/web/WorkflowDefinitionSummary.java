package de.augmentia.agentplatform.orchestrator.web;

import java.time.Instant;

public record WorkflowDefinitionSummary(
        String definitionId,
        String name,
        int version,
        boolean active,
        Instant createdAt
) {}
