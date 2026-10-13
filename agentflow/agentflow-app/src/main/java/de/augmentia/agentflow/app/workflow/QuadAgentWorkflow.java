package de.augmentia.agentflow.app.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** Durable run of a quad-core agent (LangChain4j) as a Temporal activity. */
@WorkflowInterface
public interface QuadAgentWorkflow {

    @WorkflowMethod
    String run(String input);
}
