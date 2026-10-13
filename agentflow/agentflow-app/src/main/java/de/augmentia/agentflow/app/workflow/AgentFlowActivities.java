package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activities are the non-deterministic, retryable units (LLM calls, tool calls, IO).
 * The LangGraph4j graph runs inside {@link #runAgentGraph(String)}.
 */
@ActivityInterface
public interface AgentFlowActivities {

    @ActivityMethod
    String runAgentGraph(String input);

    /** Runs a quad-core agent (LangChain4j) — reuse of the existing framework. */
    @ActivityMethod
    String runQuadAgent(String input);

    /**
     * Runs a fully-equipped quad-core agent: built-in tools (workspace-confined), optional
     * skills (JIT), guardrails, PII anonymization and an audit trail.
     *
     * @param tools      subset of built-in tool names (empty = all)
     * @param skillsDir  optional skills directory for {@code load_skill}
     */
    @ActivityMethod
    String runRichAgent(String input, java.util.List<String> tools, String skillsDir);

    /** Runs a quad-core agent whose tools are scoped by the caller's roles (RBAC). */
    @ActivityMethod
    String runAuthorizedAgent(String input, java.util.List<String> roles);
}
