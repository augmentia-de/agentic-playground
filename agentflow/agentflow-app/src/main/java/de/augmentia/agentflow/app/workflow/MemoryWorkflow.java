package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Durable memory operation: {@code op=remember} stores a fact, {@code op=recall} returns the
 * scope's memory injection block (quad-core {@code PersistentMemoryStore}).
 */
@WorkflowInterface
public interface MemoryWorkflow {

    @WorkflowMethod
    String run(String op, String scope, String content);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements MemoryWorkflow {
        private final AdvancedActivities activities = Workflow.newActivityStub(
                AdvancedActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(30))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        @Override
        public String run(String op, String scope, String content) {
            if ("recall".equalsIgnoreCase(op)) {
                return activities.recallFacts(scope);
            }
            return activities.rememberFact(scope, content);
        }
    }
}
