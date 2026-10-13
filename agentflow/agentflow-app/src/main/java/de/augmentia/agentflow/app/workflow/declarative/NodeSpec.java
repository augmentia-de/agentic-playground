package de.augmentia.agentflow.app.workflow.declarative;

import java.io.Serializable;
import java.util.List;

/**
 * A single node of a declarative workflow, flattened (sub-workflows expanded) into a form the
 * durable interpreter can execute. Covers all v2 node types that carry runtime semantics.
 */
public record NodeSpec(
        String id,
        String type,
        String title,
        String agentId,
        String systemPrompt,
        List<String> tools,
        boolean jsonOutput,
        boolean requiresHumanApproval,
        List<String> dependsOn,
        List<Branch> branches,
        String defaultTarget,
        int maxIterations,
        String exitCondition,
        List<String> bodySteps,
        String joinType,
        Integer joinN,
        List<String> guardrailsInput,
        List<String> guardrailsOutput,
        List<String> hooks) implements Serializable {
}
