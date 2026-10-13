package de.augmentia.agentflow.examples.contract;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Business case 2 — Contract review with human approval.
 *
 * <p>The agent analyzes the contract (PII masked, injection-guarded); the workflow then
 * blocks durably until a human approves or rejects the findings.</p>
 */
@WorkflowInterface
public interface ContractReviewWorkflow {

    @WorkflowMethod
    String run(String contractText);

    @SignalMethod
    void approve(boolean approved);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements ContractReviewWorkflow {
        private final ContractActivities activities = Workflow.newActivityStub(
                ContractActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        private Boolean approved;

        @Override
        public String run(String contractText) {
            String review = activities.analyzeContract(contractText);
            Workflow.await(() -> approved != null);
            String decision = Boolean.TRUE.equals(approved) ? "approved" : "rejected";
            activities.recordDecision(decision + ": " + review);
            return decision + " review=" + review;
        }

        @Override
        public void approve(boolean approved) {
            this.approved = approved;
        }
    }
}
