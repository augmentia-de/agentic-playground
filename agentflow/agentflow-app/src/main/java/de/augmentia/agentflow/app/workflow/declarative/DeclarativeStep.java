package de.augmentia.agentflow.app.workflow.declarative;

import java.io.Serializable;
import java.util.List;

/** One executable agent step derived from a declarative workflow document. */
public record DeclarativeStep(
        String nodeId,
        String title,
        String agentId,
        String systemPrompt,
        List<String> tools,
        boolean jsonOutput,
        boolean requiresHumanApproval,
        List<String> guardrailsInput,
        List<String> guardrailsOutput,
        List<String> hooks) implements Serializable {
}
