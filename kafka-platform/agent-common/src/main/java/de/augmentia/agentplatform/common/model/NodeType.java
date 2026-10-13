package de.augmentia.agentplatform.common.model;

public enum NodeType {
    STEP,
    DECISION,
    LOOP,
    FORK,
    JOIN,
    END;

    public boolean isRoutingOnly() {
        return this == DECISION || this == FORK || this == JOIN || this == END;
    }
}