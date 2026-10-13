package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/** Workflow implementation delegating to the rich quad-core agent activity. */
@WorkflowImpl(taskQueues = "agentflow")
public class RichAgentWorkflowImpl implements RichAgentWorkflow {

    private final AgentFlowActivities activities = Workflow.newActivityStub(
            AgentFlowActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(10))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                    .build());

    @Override
    public String run(String input, java.util.List<String> tools, String skillsDir) {
        return activities.runRichAgent(input, tools, skillsDir);
    }
}
