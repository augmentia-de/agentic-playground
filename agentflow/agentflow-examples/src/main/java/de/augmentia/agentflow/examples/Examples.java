package de.augmentia.agentflow.examples;

import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.QuadAgent;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.gdpr.PiiAnonymizerHook;
import de.augmentia.quad.core.guardrails.Guardrail;
import de.augmentia.quad.core.guardrails.GuardrailResult;
import de.augmentia.quad.core.session.AgentSessionState;

import java.util.Set;

/**
 * Shared helpers for the example business cases. Self-contained so that the examples module
 * stays independent from {@code agentflow-app} (which depends on this module).
 */
public final class Examples {

    private Examples() {
    }

    /** A plain quad agent with model + optional tools and system prompt. */
    public static Agent agent(String systemPrompt, Object... tools) {
        AgentBuilder<QuadAgent> builder = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSystemPrompt(systemPrompt);
        if (tools != null && tools.length > 0) {
            builder = builder.withToolObjects(tools);
        }
        return builder.build();
    }

    /** A durable free-text run (quad agent) for a single prompt. */
    public static String ask(Agent agent, String prompt) {
        return agent.run(prompt, new AgentSessionState());
    }

    /** Blocks obvious prompt-injection patterns on the input side. */
    public static Guardrail promptInjectionGuardrail() {
        return (messages, context) -> {
            String text = String.valueOf(messages).toLowerCase();
            if (text.contains("ignore all previous") || text.contains("jailbreak")
                    || text.contains("disregard the system")) {
                return GuardrailResult.block("Prompt-injection pattern detected");
            }
            return GuardrailResult.ok();
        };
    }

    /** Masks emails, phone numbers and credit cards before they reach the model. */
    public static PiiAnonymizerHook piiHook() {
        return new PiiAnonymizerHook(
                Set.of(PiiAnonymizerHook.MaskType.EMAIL,
                        PiiAnonymizerHook.MaskType.PHONE_NUMBER,
                        PiiAnonymizerHook.MaskType.CREDIT_CARD),
                PiiAnonymizerHook.BlockAction.REDACT,
                "[REDACTED]");
    }
}
