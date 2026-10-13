package de.augmentia.agentplatform.common.model;

public enum HitlTicketStatus {
    OPEN,
    APPROVED,
    SKIPPED,
    ABORTED,
    EXPIRED,
    RESOLVED;

    public boolean isTerminal() {
        return this == APPROVED || this == SKIPPED || this == ABORTED || this == RESOLVED;
    }
}
