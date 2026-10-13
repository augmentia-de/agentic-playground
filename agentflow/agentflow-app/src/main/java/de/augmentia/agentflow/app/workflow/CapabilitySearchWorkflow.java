package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/** Durable capability search / RAG run. */
@WorkflowInterface
public interface CapabilitySearchWorkflow {

    @WorkflowMethod
    String run(String input, String query, int topK, String tenant);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements CapabilitySearchWorkflow {
        private final CapabilityActivities activities = Workflow.newActivityStub(
                CapabilityActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        @Override
        public String run(String input, String query, int topK, String tenant) {
            String matches = activities.searchCapabilities(query, topK, tenant);
            String answer = activities.runWithCapabilitySearch(input);
            return "matches=" + matches + "\nanswer=" + answer;
        }
    }
}
