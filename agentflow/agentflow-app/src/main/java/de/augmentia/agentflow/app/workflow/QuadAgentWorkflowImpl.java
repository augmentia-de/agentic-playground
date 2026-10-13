package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/** Workflow implementation delegating to the quad-core agent activity. */
@WorkflowImpl(taskQueues = "agentflow")
public class QuadAgentWorkflowImpl implements QuadAgentWorkflow {

    private final AgentFlowActivities activities = Workflow.newActivityStub(
            AgentFlowActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(5))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
                    .build());

    @Override
    public String run(String input) {
        return activities.runQuadAgent(input);
    }
}
