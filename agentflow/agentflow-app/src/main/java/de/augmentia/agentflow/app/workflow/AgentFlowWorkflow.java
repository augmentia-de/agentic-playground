package de.augmentia.agentflow.app.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Durable orchestration for one agent flow run. Deterministic workflow code; all
 * non-deterministic work (LLM/tools) lives in {@link AgentFlowActivities}.
 */
@WorkflowInterface
public interface AgentFlowWorkflow {

    @WorkflowMethod
    String run(String input);
}
