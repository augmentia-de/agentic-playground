package de.augmentia.agentplatform.orchestrator.domain;

import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.NodeType;

import java.time.Instant;
import java.util.UUID;

public record CheckpointInsert(
        UUID id,
        UUID workflowExecutionId,
        String stepId,
        int iteration,
        NodeType nodeType,
        String agentType,
        UUID taskId,
        CheckpointStatus status,
        String decisionBranch,
        String branchId,
        String inputPayloadJson,
        String outputPayloadJson,
        int attempt,
        int maxAttempts,
        long timeoutMs,
        Instant startedAt,
        Instant completedAt
) {}
