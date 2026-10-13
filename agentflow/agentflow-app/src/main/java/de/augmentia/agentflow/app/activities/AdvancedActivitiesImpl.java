package de.augmentia.agentflow.app.activities;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentflow.app.observability.AgentTelemetry;
import de.augmentia.agentflow.app.structured.ContentPlan;
import de.augmentia.agentflow.app.tools.ClockTool;
import de.augmentia.agentflow.app.workflow.AdvancedActivities;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.quad.core.agent.runtime.AgentEventPublisher;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.observability.AgentMetrics;
import de.augmentia.quad.core.observability.AgentTracing;
import de.augmentia.quad.core.observability.MetricsHook;
import de.augmentia.quad.core.observability.TracingHook;
import de.augmentia.quad.core.observability.provenance.ProvenanceTracker;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.memory.MemoryCategory;
import de.augmentia.quad.core.session.memory.MemoryEntry;
import de.augmentia.quad.core.session.memory.PersistentMemoryStore;
import de.augmentia.quad.core.tool.SseMcpProxy;
import de.augmentia.quad.core.tool.ToolMethod;
import io.opentelemetry.api.GlobalOpenTelemetry;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import io.temporal.spring.boot.ActivityImpl;

/** Advanced quad-core capabilities wired as activities (structured output, provenance, memory, MCP). */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class AdvancedActivitiesImpl implements AdvancedActivities {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PersistentMemoryStore memory;
    private final AgentMetrics agentMetrics;

    public AdvancedActivitiesImpl(PersistentMemoryStore memory, AgentMetrics agentMetrics) {
        this.memory = memory;
        this.agentMetrics = agentMetrics;
    }

    @Override
    public String runStructuredAgent(String input) {
        Agent agent = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSystemPrompt("Return a structured content plan using the provided schema.")
                .withStructuredOutput(ContentPlan.class)
                .withEventPublisher(AgentTelemetry.metricsPublisher(agentMetrics))
                .build();
        AgentResult result = agent.executeStructured(input, new AgentSessionState());
        return result.structuredOutput() != null ? result.structuredOutput() : result.finalAnswer();
    }

    @Override
    public String runWithProvenance(String input) {
        AgentEventPublisher publisher = new AgentEventPublisher();
        ProvenanceTracker tracker = new ProvenanceTracker();
        publisher.addEventListener(tracker::onEvent);
        publisher.addEventListener(new MetricsHook(agentMetrics)::onEvent);

        Agent agent = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withEventPublisher(publisher)
                .withToolObjects(new ClockTool())
                .withSystemPrompt("Use the 'now' tool, then answer briefly.")
                .build();
        AgentSessionState state = new AgentSessionState();
        agent.run(input, state);
        try {
            return MAPPER.writeValueAsString(tracker.forSession(state.getSessionId()));
        } catch (Exception e) {
            return "[]";
        }
    }

    @Override
    public String rememberFact(String scope, String content) {
        String id = "mem-" + UUID.randomUUID();
        memory.remember(new MemoryEntry(id, scope, content, content,
                MemoryCategory.PROJECT_FACT, Instant.now(), false));
        return id;
    }

    @Override
    public String recallFacts(String scope) {
        return memory.formatForInjection(scope);
    }

    @Override
    public List<String> listMcpTools(String serverUrl) {
        if (serverUrl == null || serverUrl.isBlank()) {
            return List.of();
        }
        return new SseMcpProxy(serverUrl, "agentflow").loadTools().stream()
                .map(t -> t.spec().name())
                .toList();
    }

    @Override
    public String runMcpTool(String serverUrl, String toolName, String jsonArguments) {
        if (serverUrl == null || serverUrl.isBlank()) {
            return "[mcp not configured]";
        }
        List<ToolMethod> tools = new SseMcpProxy(serverUrl, "agentflow").loadTools();
        for (ToolMethod tool : tools) {
            if (tool.spec().name().equals(toolName)) {
                try {
                    return tool.execute(jsonArguments, new AgentSessionState()).text();
                } catch (Exception e) {
                    return "error: " + e.getMessage();
                }
            }
        }
        return "[tool not found: " + toolName + "]";
    }

    @Override
    public String runWithSubAgents(String input) {
        Agent researcher = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withToolObjects(new ClockTool())
                .withEventPublisher(AgentTelemetry.metricsPublisher(agentMetrics))
                .withSystemPrompt("You are a research sub-agent. Answer concisely.")
                .build();
        Agent orchestrator = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSubAgent("researcher", researcher)
                .withEventPublisher(AgentTelemetry.metricsPublisher(agentMetrics))
                .withSystemPrompt("Delegate to the 'researcher' tool when needed, then summarise in one sentence.")
                .build();
        return orchestrator.run(input, new AgentSessionState());
    }

    @Override
    public String runWithMetrics(String input) {
        AgentEventPublisher publisher = new AgentEventPublisher();
        publisher.addEventListener(new MetricsHook(agentMetrics)::onEvent);
        publisher.addEventListener(new TracingHook(new AgentTracing(GlobalOpenTelemetry.get()))::onEvent);

        Agent agent = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withEventPublisher(publisher)
                .withToolObjects(new ClockTool())
                .withSystemPrompt("Answer briefly.")
                .build();
        agent.run(input, new AgentSessionState());

        java.util.Map<String, Object> snapshot = new java.util.LinkedHashMap<>();
        snapshot.put("llmCalls", agentMetrics.getLlmCalls().count());
        snapshot.put("toolExecutions", agentMetrics.getToolExecutions().count());
        snapshot.put("errors", agentMetrics.getErrors().count());
        snapshot.put("executions", agentMetrics.getExecutionDuration().count());
        snapshot.put("promptTokens", agentMetrics.getPromptTokens().count());
        snapshot.put("completionTokens", agentMetrics.getCompletionTokens().count());
        try {
            return MAPPER.writeValueAsString(snapshot);
        } catch (Exception e) {
            return "{}";
        }
    }
}
