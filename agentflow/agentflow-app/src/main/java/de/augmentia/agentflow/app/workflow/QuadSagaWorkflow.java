package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/** Durable wrapper around quad-core's session-level saga unwinding. */
@WorkflowInterface
public interface QuadSagaWorkflow {

    @WorkflowMethod
    String run(String input);

    /** Implementation is nested below to keep the module compact. */
    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements QuadSagaWorkflow {
        private final SagaActivities activities = Workflow.newActivityStub(
                SagaActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(30))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
                        .build());

        @Override
        public String run(String input) {
            return activities.quadSaga(input);
        }
    }
}
