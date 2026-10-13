package de.augmentia.agentplatform.common.model;

public enum ExecutionStatus {
    PENDING,
    RUNNING,
    PAUSED_HITL,
    COMPLETED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
}
