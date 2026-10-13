package de.augmentia.agentplatform.orchestrator.domain;

import java.time.Instant;
import java.util.UUID;

public record WorkflowDefinitionRow(
        UUID id,
        String name,
        int version,
        String dagStructureJson,
        boolean active,
        Instant createdAt
) {}
