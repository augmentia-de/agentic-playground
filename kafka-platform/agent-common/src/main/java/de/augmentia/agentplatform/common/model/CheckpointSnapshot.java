package de.augmentia.agentplatform.common.model;

/**
 * Immutable snapshot of one checkpoint row fed to the deterministic evaluator.
 * Mirrors workflow_checkpoints columns relevant to routing.
 */
public record CheckpointSnapshot(
        String workflowExecutionId,
        String stepId,
        NodeType nodeType,
        CheckpointStatus status,
        int iteration,
        String decisionBranch,
        String agentType
) {

    public static final CheckpointSnapshot PENDING_SENTINEL =
            new CheckpointSnapshot("", "", NodeType.STEP, CheckpointStatus.PENDING, 1, null, null);
}