package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Workflow implementation: a single durable activity runs the LangGraph4j graph.
 *
 * <p>The split is deliberate — Temporal owns durability, retries and long-running state;
 * LangGraph4j owns the reasoning graph. Non-deterministic work stays in the activity.</p>
 */
@WorkflowImpl(taskQueues = "agentflow")
public class AgentFlowWorkflowImpl implements AgentFlowWorkflow {

    private final AgentFlowActivities activities = Workflow.newActivityStub(
            AgentFlowActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(5))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(3)
                            .build())
                    .build());

    @Override
    public String run(String input) {
        return activities.runAgentGraph(input);
    }
}
