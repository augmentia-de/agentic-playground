package de.augmentia.agentflow.examples.onboarding;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Business case 5 — Onboarding assistant.
 *
 * <p>Recalls project memory, produces a personalised first-week plan, and can surface tools
 * from an external MCP server. Combines long-term memory, capability search and MCP.</p>
 */
@WorkflowInterface
public interface OnboardingWorkflow {

    @WorkflowMethod
    String run(String newHire, String projectScope);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements OnboardingWorkflow {
        private final OnboardingActivities activities = Workflow.newActivityStub(
                OnboardingActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        @Override
        public String run(String newHire, String projectScope) {
            String memory = activities.recallProject(projectScope);
            String plan = activities.planOnboarding(newHire, memory);
            activities.rememberProject(projectScope, "onboarded: " + newHire);
            return plan;
        }
    }
}
