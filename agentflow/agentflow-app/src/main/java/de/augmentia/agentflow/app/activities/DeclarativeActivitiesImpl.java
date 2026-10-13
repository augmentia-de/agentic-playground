package de.augmentia.agentflow.app.activities;

import de.augmentia.agentflow.app.observability.AgentTelemetry;
import de.augmentia.agentflow.app.quad.QuadCapabilities;
import de.augmentia.agentflow.app.quad.QuadGuardrails;
import de.augmentia.agentflow.app.workflow.declarative.DeclarativeActivities;
import de.augmentia.agentflow.app.workflow.declarative.DeclarativePlan;
import de.augmentia.agentflow.app.workflow.declarative.DeclarativePlanner;
import de.augmentia.agentflow.app.workflow.declarative.DeclarativeStep;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionRegistry;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.QuadAgent;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.guardrails.Guardrail;
import de.augmentia.quad.core.guardrails.GuardrailPlugin;
import de.augmentia.quad.core.observability.AgentMetrics;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.WorkspaceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import io.temporal.spring.boot.ActivityImpl;

/** Executes declarative workflow steps by mapping agent nodes to quad-core agents. */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class DeclarativeActivitiesImpl implements DeclarativeActivities {

    private static final Logger log = LoggerFactory.getLogger(DeclarativeActivitiesImpl.class);

    private final DeclarativePlanner planner;
    private final AgentMetrics agentMetrics;

    public DeclarativeActivitiesImpl(WorkflowDefinitionRegistry registry, AgentMetrics agentMetrics) {
        this.planner = new DeclarativePlanner(registry);
        this.agentMetrics = agentMetrics;
    }

    @Override
    public DeclarativePlan plan(String workflowId) {
        return planner.plan(workflowId);
    }

    @Override
    public String runStep(DeclarativeStep step, String task) {
        AgentBuilder<QuadAgent> builder = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSystemPrompt(step.systemPrompt())
                .withEventPublisher(AgentTelemetry.metricsPublisher(agentMetrics));
        List<Object> tools = resolveTools(step.tools());
        if (!tools.isEmpty()) {
            builder = builder.withToolObjects(tools.toArray());
        }
        Agent agent = builder.build();

        // Named input/output guardrails (agent-level, resolved from the document). Enforced by
        // quad-core's GuardrailPlugin via the hook pipeline (before/after model call).
        List<Guardrail> input = QuadGuardrails.fromNames(step.guardrailsInput());
        List<Guardrail> output = QuadGuardrails.fromNames(step.guardrailsOutput());
        if (!input.isEmpty() || !output.isEmpty()) {
            agent.addHook(new GuardrailPlugin(input, output));
        }
        // Hook specs such as 'pii:EMAIL,PHONE_NUMBER' (masking) or 'audit'.
        for (String spec : step.hooks()) {
            attachHook(agent, spec);
        }

        return agent.run(task, new AgentSessionState());
    }

    /** Attaches one hook spec {@code name[:arg1,arg2]} (e.g. {@code pii:EMAIL,PHONE_NUMBER}, {@code audit}). */
    private void attachHook(Agent agent, String spec) {
        if (spec == null || spec.isBlank()) {
            return;
        }
        String name = spec;
        List<String> args = List.of();
        int colon = spec.indexOf(':');
        if (colon >= 0) {
            name = spec.substring(0, colon).trim();
            args = Arrays.stream(spec.substring(colon + 1).split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }
        switch (name.toLowerCase(Locale.ROOT)) {
            case "pii" -> agent.addHook(QuadGuardrails.piiAnonymizer(args));
            case "audit" -> agent.addHook(QuadGuardrails.auditHook());
            default -> log.warn("Unknown hook '{}' — ignoring", spec);
        }
    }

    /** Resolves catalog tool names to quad-core built-in tool beans (workspace-confined). */
    private List<Object> resolveTools(List<String> names) {
        if (names == null || names.isEmpty()) {
            return List.of();
        }
        Map<String, Object> builtins = QuadCapabilities.builtinToolBeans(new WorkspaceResolver());
        List<Object> out = new ArrayList<>();
        for (String name : names) {
            Object bean = builtins.get(name);
            if (bean != null) {
                out.add(bean);
            }
        }
        return out;
    }
}
