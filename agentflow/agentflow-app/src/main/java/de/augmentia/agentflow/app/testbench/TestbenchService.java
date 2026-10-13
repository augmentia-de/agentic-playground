package de.augmentia.agentflow.app.testbench;

import de.augmentia.agentflow.app.quad.QuadCapabilities;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.QuadAgent;
import de.augmentia.quad.core.agent.runtime.AgentEventPublisher;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.observability.AgentMetrics;
import de.augmentia.quad.core.observability.MetricsHook;
import de.augmentia.quad.core.observability.provenance.ProvenanceEntry;
import de.augmentia.quad.core.observability.provenance.ProvenanceTracker;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.WorkspaceResolver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Runs a single agent and returns a diagnostic trace: answer, token usage, tool calls, duration,
 * estimated cost and tool provenance. Metrics are captured with a per-run Micrometer registry so
 * no global state is touched.
 */
@Component
public class TestbenchService {

    private final double inputPricePer1k;
    private final double outputPricePer1k;

    public TestbenchService(@Value("${agentflow.testbench.input-price-per-1k:0}") double inputPricePer1k,
                            @Value("${agentflow.testbench.output-price-per-1k:0}") double outputPricePer1k) {
        this.inputPricePer1k = inputPricePer1k;
        this.outputPricePer1k = outputPricePer1k;
    }

    public Map<String, Object> run(TestbenchRequest request) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AgentMetrics metrics = new AgentMetrics(registry);

        AgentEventPublisher publisher = new AgentEventPublisher();
        publisher.addEventListener(new MetricsHook(metrics)::onEvent);
        ProvenanceTracker provenance = new ProvenanceTracker();
        publisher.addEventListener(provenance::onEvent);

        AgentBuilder<QuadAgent> builder = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withEventPublisher(publisher);
        if (request.systemPrompt() != null && !request.systemPrompt().isBlank()) {
            builder = builder.withSystemPrompt(request.systemPrompt());
        }
        List<Object> tools = resolveTools(request.tools());
        if (!tools.isEmpty()) {
            builder = builder.withToolObjects(tools.toArray());
        }
        Agent agent = builder.build();

        AgentSessionState state = new AgentSessionState();
        long start = System.currentTimeMillis();
        String answer = agent.run(request.prompt(), state);
        long durationMs = System.currentTimeMillis() - start;

        int inputTokens = (int) metrics.getPromptTokens().totalAmount();
        int outputTokens = (int) metrics.getCompletionTokens().totalAmount();
        double costUsd = inputTokens / 1000.0 * inputPricePer1k + outputTokens / 1000.0 * outputPricePer1k;

        List<Map<String, Object>> toolTrace = new ArrayList<>();
        for (ProvenanceEntry entry : provenance.forSession(state.getSessionId())) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("tool", entry.toolName());
            t.put("source", entry.source() == null ? "" : entry.source());
            t.put("at", entry.at() == null ? null : entry.at().toString());
            toolTrace.add(t);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("answer", answer);
        out.put("systemPrompt", request.systemPrompt());
        out.put("tools", request.tools() == null ? List.of() : request.tools());
        out.put("inputTokens", inputTokens);
        out.put("outputTokens", outputTokens);
        out.put("toolCalls", (int) metrics.getToolExecutions().count());
        out.put("durationMs", durationMs);
        out.put("costUsd", costUsd);
        out.put("provenance", toolTrace);
        return out;
    }

    private List<Object> resolveTools(List<String> names) {
        if (names == null || names.isEmpty()) {
            return List.of();
        }
        Map<String, Object> builtins = QuadCapabilities.builtinToolBeans(new WorkspaceResolver());
        List<Object> out = new ArrayList<>();
        for (String name : Optional.ofNullable(names).orElse(List.of())) {
            Object bean = builtins.get(name);
            if (bean != null) {
                out.add(bean);
            }
        }
        return out;
    }
}
