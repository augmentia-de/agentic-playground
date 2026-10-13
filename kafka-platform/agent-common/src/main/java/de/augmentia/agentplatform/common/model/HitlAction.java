package de.augmentia.agentplatform.common.model;

/** Human decision on a HITL ticket (SPEC2 §10.5 lifecycle). */
public enum HitlAction {
    APPROVED,
    RETRY,
    SKIP,
    ABORT
}