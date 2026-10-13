package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.List;

/** Advanced quad-core capabilities as durable activities. */
@ActivityInterface
public interface AdvancedActivities {

    /** Typed structured output via a quad-core agent ({@code executeStructured}). */
    @ActivityMethod
    String runStructuredAgent(String input);

    /** Runs an agent with a provenance tracker attached; returns the provenance as JSON. */
    @ActivityMethod
    String runWithProvenance(String input);

    /** Stores a long-term memory fact (quad-core {@code PersistentMemoryStore}). */
    @ActivityMethod
    String rememberFact(String scope, String content);

    /** Recalls a scope's memory as a compact injection block. */
    @ActivityMethod
    String recallFacts(String scope);

    /** Lists tool names of an MCP server (SSE); empty when no server is configured. */
    @ActivityMethod
    List<String> listMcpTools(String serverUrl);

    /** Executes an MCP tool by name. */
    @ActivityMethod
    String runMcpTool(String serverUrl, String toolName, String jsonArguments);

    /** Orchestrator delegating to a sub-agent registered as a tool. */
    @ActivityMethod
    String runWithSubAgents(String input);

    /** Runs an agent with quad-core MetricsHook (Micrometer) + TracingHook (OTel). */
    @ActivityMethod
    String runWithMetrics(String input);
}
