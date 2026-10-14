package de.augmentia.agentflow.app.activities;

import de.augmentia.agentflow.app.config.RequestContext;
import de.augmentia.agentflow.app.quad.QuadCapabilities;
import de.augmentia.agentflow.app.quad.QuadSecurity;
import de.augmentia.agentflow.app.workflow.AgentFlowActivities;
import de.augmentia.agentflow.core.AgentFlowGraph;
import de.augmentia.agentflow.core.AgentFlowState;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.Agents;
import de.augmentia.quad.core.observability.AgentMetrics;
import de.augmentia.quad.core.session.AgentSessionState;
import org.bsc.langgraph4j.RunnableConfig;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import io.temporal.spring.boot.ActivityImpl;

/**
 * Activity implementation: runs the LangGraph4j graph with Spring AI for the model calls.
 *
 * <p>This is the non-deterministic unit — Temporal may retry it. All LLM calls happen here
 * (never in workflow code), which keeps the Temporal workflow deterministic.</p>
 *
 * <p>Every activity binds the current run id for the duration of the call (see
 * {@link #withRequestContext(Function)}), so dynamic LLM headers / tenant reach <b>all</b>
 * execution paths, not just the LangGraph one.</p>
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
        return withRequestContext(rid -> {
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
            }
        });
    }

    @Override
    public String runQuadAgent(String input) {
        // Reuse of quad-core's ready-to-use default agent (LangChain4j, model from env).
        return withRequestContext(rid -> runSimpleAgent(input, rid));
    }

    @Override
    public String runRichAgent(String input, List<String> tools, String skillsDir) {
        Set<String> allowed = (tools == null || tools.isEmpty())
                ? null : new LinkedHashSet<>(tools);
        Path dir = (skillsDir != null && !skillsDir.isBlank())
                ? Path.of(skillsDir) : null;
        return withRequestContext(rid -> runCapabilities(allowed, dir, List.of(), false, rid, input));
    }

    @Override
    public String runAuthorizedAgent(String input, List<String> roles) {
        Set<String> roleSet = (roles == null)
                ? Set.of() : new LinkedHashSet<>(roles);
        return withRequestContext(rid ->
                runCapabilities(QuadSecurity.allowedTools(roleSet), null, List.of(), false, rid, input));
    }

    // ── agent seams (package-private so unit tests can substitute a fake body) ──

    /** quad-core's ready-to-use default agent, bound to the current run as its session id. */
    String runSimpleAgent(String input, String runId) {
        return runWithSession(Agents.simple(), input, runId);
    }

    /** Fully-equipped quad-core agent, bound to the current run as its session id. */
    String runCapabilities(Set<String> allowedTools, Path skillsDir, List<String> initialSkills,
                           boolean skillSearch, String runId, String input) {
        Agent agent = QuadCapabilities.build(allowedTools, skillsDir, initialSkills, skillSearch, agentMetrics);
        return runWithSession(agent, input, runId);
    }

    /**
     * Runs the agent with the run id as its session id, so the dynamic session header
     * ({@code ModelHeaders}) is per run rather than a random per-agent UUID.
     */
    private static String runWithSession(Agent agent, String input, String runId) {
        return (runId == null || runId.isBlank())
                ? agent.run(input)
                : agent.run(input, AgentSessionState.create(runId));
    }

    // ── per-run context ─────────────────────────────────────────────────────────

    /**
     * Binds the current Temporal run id for the duration of {@code action}: it is the dynamic-header
     * source for the Spring-AI graph ({@link RequestContext}) and, as the session id handed to the
     * quad-core agent, for the LangChain4j path. Always cleared in a {@code finally} block, because
     * activity threads are reused.
     *
     * @param action the activity body, receiving the resolved run id (possibly {@code null} when run
     *               outside a Temporal activity context)
     */
    private String withRequestContext(Function<String, String> action) {
        String runId = runId();
        RequestContext.set(runId);
        try {
            return action.apply(runId);
        } finally {
            RequestContext.clear();
        }
    }

    /** Current Temporal workflow id (used as the dynamic-header run id). Overridable for tests. */
    String runId() {
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
}
