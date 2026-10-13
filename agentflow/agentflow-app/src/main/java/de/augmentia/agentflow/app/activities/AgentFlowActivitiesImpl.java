package de.augmentia.agentflow.app.activities;

import de.augmentia.agentflow.app.config.RequestContext;
import de.augmentia.agentflow.app.workflow.AgentFlowActivities;
import de.augmentia.agentflow.core.AgentFlowGraph;
import de.augmentia.agentflow.core.AgentFlowState;
import de.augmentia.quad.core.observability.AgentMetrics;
import org.bsc.langgraph4j.RunnableConfig;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Component;

import java.util.Map;
import io.temporal.spring.boot.ActivityImpl;

/**
 * Activity implementation: runs the LangGraph4j graph with Spring AI for the model calls.
 *
 * <p>This is the non-deterministic unit — Temporal may retry it. All LLM calls happen here
 * (never in workflow code), which keeps the Temporal workflow deterministic.</p>
 */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class AgentFlowActivitiesImpl implements AgentFlowActivities {

    private final ChatClient chatClient;
    private final AgentMetrics agentMetrics;

    public AgentFlowActivitiesImpl(ChatClient.Builder chatClientBuilder, AgentMetrics agentMetrics) {
        this.chatClient = chatClientBuilder.build();
        this.agentMetrics = agentMetrics;
    }

    @Override
    public String runAgentGraph(String input) {
        RequestContext.set(runId());
        try {
            var graph = AgentFlowGraph.build(
                    state -> Map.of(
                            AgentFlowState.PLAN, call("Create a short plan for: " + state.input()),
                            AgentFlowState.TRACE, "planner;"),
                    state -> Map.of(
                            AgentFlowState.RESULT, call("Execute this plan and answer: " + state.plan()),
                            AgentFlowState.TRACE, state.trace() + "executor;"),
                    state -> {
                        int attempts = state.attempts() + 1;
                        return Map.of(
                                AgentFlowState.ATTEMPTS, attempts,
                                AgentFlowState.REVIEW, attempts >= 1 ? "approved" : "retry",
                                AgentFlowState.TRACE, state.trace() + "reviewer;");
                    });

            AgentFlowState result = graph
                    .invoke(Map.of(AgentFlowState.INPUT, input), RunnableConfig.builder().build())
                    .orElseThrow();
            return result.result();
        } catch (Exception e) {
            throw new IllegalStateException("Agent graph failed", e);
        } finally {
            RequestContext.clear();
        }
    }

    /** Current Temporal workflow id (used as the dynamic-header run id). */
    private String runId() {
        try {
            return io.temporal.activity.Activity.getExecutionContext().getInfo().getWorkflowId();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private String call(String prompt) {
        String content = chatClient.prompt().user(prompt).call().content();
        return content != null ? content : "";
    }

    @Override
    public String runQuadAgent(String input) {
        // Reuse of quad-core's ready-to-use default agent (LangChain4j, model from env).
        return de.augmentia.quad.core.agent.Agents.simple().run(input);
    }

    @Override
    public String runRichAgent(String input, java.util.List<String> tools, String skillsDir) {
        java.util.Set<String> allowed = (tools == null || tools.isEmpty())
                ? null : new java.util.LinkedHashSet<>(tools);
        java.nio.file.Path dir = (skillsDir != null && !skillsDir.isBlank())
                ? java.nio.file.Path.of(skillsDir) : null;
        return de.augmentia.agentflow.app.quad.QuadCapabilities
                .build(allowed, dir, java.util.List.of(), false, agentMetrics)
                .run(input);
    }

    @Override
    public String runAuthorizedAgent(String input, java.util.List<String> roles) {
        java.util.Set<String> roleSet = (roles == null)
                ? java.util.Set.of() : new java.util.LinkedHashSet<>(roles);
        java.util.Set<String> allowed = de.augmentia.agentflow.app.quad.QuadSecurity.allowedTools(roleSet);
        return de.augmentia.agentflow.app.quad.QuadCapabilities
                .build(allowed, null, java.util.List.of(), false, agentMetrics)
                .run(input);
    }
}
