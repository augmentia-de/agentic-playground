package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/** Saga implementation with explicit compensating actions on failure. */
@WorkflowImpl(taskQueues = "agentflow")
public class SagaWorkflowImpl implements SagaWorkflow {

    private final SagaActivities activities = Workflow.newActivityStub(
            SagaActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    // Single attempt so a business failure surfaces deterministically.
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
                    .build());

    @Override
    public String run(String orderId, double amount) {
        String reservation = activities.reserve(orderId);
        try {
            return "completed: " + activities.charge(orderId, amount);
        } catch (ActivityFailure failure) {
            // Compensate in reverse order; durable and replay-safe.
            activities.release(reservation);
            activities.refund(orderId);
            return "compensated (payment failed) — released=" + reservation;
        }
    }
}
