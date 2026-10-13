package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Durable MCP interaction: {@code op=list} lists the server's tools, {@code op=run} executes a tool.
 * Returns a string (tool list joined by newline, or the tool result).
 */
@WorkflowInterface
public interface McpWorkflow {

    @WorkflowMethod
    String run(String op, String serverUrl, String toolName, String jsonArguments);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements McpWorkflow {
        private final AdvancedActivities activities = Workflow.newActivityStub(
                AdvancedActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(2))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        @Override
        public String run(String op, String serverUrl, String toolName, String jsonArguments) {
            if ("list".equalsIgnoreCase(op)) {
                return String.join("\n", activities.listMcpTools(serverUrl));
            }
            return activities.runMcpTool(serverUrl, toolName, jsonArguments);
        }
    }
}
