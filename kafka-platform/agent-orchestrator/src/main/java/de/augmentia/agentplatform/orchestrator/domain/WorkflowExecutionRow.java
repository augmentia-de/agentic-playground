package de.augmentia.agentplatform.orchestrator.domain;

import de.augmentia.agentplatform.common.model.ExecutionStatus;

import java.time.Instant;
import java.util.UUID;

public record WorkflowExecutionRow(
        UUID id,
        UUID workflowDefinitionId,
        String idempotencyKey,
        ExecutionStatus status,
        String inputParamsJson,
        String finalOutputJson,
        String errorDetailsJson,
        Instant startedAt,
        Instant completedAt,
        Instant updatedAt
) {}
