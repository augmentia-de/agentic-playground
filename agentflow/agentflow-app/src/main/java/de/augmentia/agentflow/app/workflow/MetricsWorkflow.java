package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Durable run of an agent with quad-core MetricsHook (Micrometer) and TracingHook (OTel)
 * attached; returns a metrics snapshot. Aggregated metrics are also exposed via
 * {@code /actuator/prometheus}.
 */
@WorkflowInterface
public interface MetricsWorkflow {

    @WorkflowMethod
    String run(String input);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements MetricsWorkflow {
        private final AdvancedActivities activities = Workflow.newActivityStub(
                AdvancedActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        @Override
        public String run(String input) {
            return activities.runWithMetrics(input);
        }
    }
}
