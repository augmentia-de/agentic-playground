package de.augmentia.agentflow.app.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Durable run of a rich quad-core agent (tools, skills, guardrails, PII, audit) as an activity.
 */
@WorkflowInterface
public interface RichAgentWorkflow {

    @WorkflowMethod
    String run(String input, java.util.List<String> tools, String skillsDir);
}
