package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Waits durably for the human decision (Temporal signal), then resumes the persisted
 * LangGraph4j graph. The waiting itself is durable: the worker can restart while paused.
 */
@WorkflowImpl(taskQueues = "agentflow")
public class HitlWorkflowImpl implements HitlWorkflow {

    private final HitlActivities activities = Workflow.newActivityStub(
            HitlActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(5))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
                    .build());

    private Boolean approved;

    @Override
    public String run(String input) {
        String threadId = Workflow.getInfo().getWorkflowId();
        HitlActivities.HitlStart start = activities.startGraph(input, threadId);

        Workflow.await(() -> approved != null);

        if (Boolean.TRUE.equals(approved)) {
            return activities.resumeGraph(threadId);
        }
        return "rejected by human (plan=" + start.plan() + ")";
    }

    @Override
    public void decide(boolean approved) {
        this.approved = approved;
    }
}
