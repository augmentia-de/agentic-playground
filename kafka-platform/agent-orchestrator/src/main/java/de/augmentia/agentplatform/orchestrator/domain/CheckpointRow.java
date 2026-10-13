package de.augmentia.agentplatform.orchestrator.domain;

import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.NodeType;

import java.time.Instant;
import java.util.UUID;

public record CheckpointRow(
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
        int retryCount,
        int attempt,
        int maxAttempts,
        long timeoutMs,
        String errorMessage,
        Instant startedAt,
        Instant completedAt,
        Instant createdAt,
        Instant updatedAt
) {

    public boolean isTerminal() {
        return status == CheckpointStatus.COMPLETED
                || status == CheckpointStatus.SKIPPED
                || status == CheckpointStatus.PRUNED
                || status == CheckpointStatus.CANCELLED
                || status == CheckpointStatus.FAILED
                || status == CheckpointStatus.WAITING_HITL;
    }
}
