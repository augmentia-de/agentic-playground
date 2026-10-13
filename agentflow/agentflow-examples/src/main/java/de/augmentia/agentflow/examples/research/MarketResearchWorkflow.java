package de.augmentia.agentflow.examples.research;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Business case 4 — Market/competitive research.
 *
 * <p>A durable two-step pipeline: an orchestrator agent delegates the fact-finding to a
 * research sub-agent, then a writer agent produces the executive brief.</p>
 */
@WorkflowInterface
public interface MarketResearchWorkflow {

    @WorkflowMethod
    String run(String topic);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements MarketResearchWorkflow {
        private final ResearchActivities activities = Workflow.newActivityStub(
                ResearchActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(8))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        @Override
        public String run(String topic) {
            String research = activities.research(topic);
            return activities.writeBrief(research);
        }
    }
}
