package de.augmentia.agentflow.app.workflow.declarative;

import java.util.List;

/** Compact view of a loaded workflow definition for listing. */
public record WorkflowDefinitionSummary(
        String id,
        String name,
        String version,
        String status,
        int nodeCount,
        List<String> agentRefs) {
}
