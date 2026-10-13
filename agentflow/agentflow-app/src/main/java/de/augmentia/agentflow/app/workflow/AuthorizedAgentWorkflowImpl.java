package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/** Workflow implementation for the role-scoped (RBAC) agent run. */
@WorkflowImpl(taskQueues = "agentflow")
public class AuthorizedAgentWorkflowImpl implements AuthorizedAgentWorkflow {

    private final AgentFlowActivities activities = Workflow.newActivityStub(
            AgentFlowActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(5))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                    .build());

    @Override
    public String run(String input, java.util.List<String> roles) {
        return activities.runAuthorizedAgent(input, roles);
    }
}
