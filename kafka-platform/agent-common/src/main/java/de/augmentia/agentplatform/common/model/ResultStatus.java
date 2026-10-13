package de.augmentia.agentplatform.common.model;

/** Result header status produced by a worker (SPEC2 4.3 Result Event). */
public enum ResultStatus {
    COMPLETED,
    FAILED_RETRYABLE,
    FAILED_FATAL,
    NEEDS_HITL
}