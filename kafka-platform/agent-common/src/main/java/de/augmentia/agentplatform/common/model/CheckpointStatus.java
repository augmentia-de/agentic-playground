package de.augmentia.agentplatform.common.model;

/** Status of a single DAG checkpoint row (SPEC2 15.2). */
public enum CheckpointStatus {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    WAITING_HITL,
    SKIPPED,
    PRUNED,
    CANCELLED;

    /** Statuses that count as "effective" for downstream dependency satisfaction. */
    public boolean isEffective() {
        return this == COMPLETED || this == SKIPPED || this == PRUNED;
    }

    /** Statuses that are excluded from a JOIN's incoming set. */
    public boolean isDead() {
        return this == PRUNED || this == CANCELLED;
    }
}