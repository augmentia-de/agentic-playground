package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/** Durable run of an orchestrator agent that delegates to a sub-agent. */
@WorkflowInterface
public interface SubAgentWorkflow {

    @WorkflowMethod
    String run(String input);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements SubAgentWorkflow {
        private final AdvancedActivities activities = Workflow.newActivityStub(
                AdvancedActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        @Override
        public String run(String input) {
            return activities.runWithSubAgents(input);
        }
    }
}
