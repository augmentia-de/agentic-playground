package de.augmentia.agentplatform.common.model;

import java.util.List;

/** Pinned workflow definition (SPEC2 5.1 / workflow_definitions table). */
public record WorkflowDefinition(
        String id,
        String name,
        int version,
        boolean isActive,
        List<DagNode> nodes
) {

    public DagNode node(String stepId) {
        for (DagNode n : nodes) {
            if (n.stepId().equals(stepId)) return n;
        }
        throw new IllegalArgumentException("Unknown step: " + stepId);
    }

    public DagNode byIdOrNull(String stepId) {
        for (DagNode n : nodes) {
            if (n.stepId().equals(stepId)) return n;
        }
        return null;
    }
}