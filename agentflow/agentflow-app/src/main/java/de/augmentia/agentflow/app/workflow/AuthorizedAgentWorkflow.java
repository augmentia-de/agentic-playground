package de.augmentia.agentflow.app.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** Durable run of a role-scoped quad-core agent (RBAC). */
@WorkflowInterface
public interface AuthorizedAgentWorkflow {

    @WorkflowMethod
    String run(String input, java.util.List<String> roles);
}
