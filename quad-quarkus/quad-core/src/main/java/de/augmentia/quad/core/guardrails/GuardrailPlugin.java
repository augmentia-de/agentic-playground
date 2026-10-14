package de.augmentia.quad.core.guardrails;

import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.hook.pipeline.HookContexts;
import de.augmentia.quad.core.hook.pipeline.HookResult;
import de.augmentia.quad.core.hook.plugin.Plugin;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;

import java.util.List;

/**
 * Plugin that applies input and output guardrails to agent interactions.
 * Registered as a hook in the pipeline (see {@link Agent#addHook}).
 *
 * <p>Enforcement happens in {@link #beforeModelCall} (input guardrails) and
 * {@link #afterModelCall} (output guardrails) — the lifecycle methods the runtime actually
 * triggers. The configured {@link BlockAction} decides what happens on a block:</p>
 *
 * <ul>
 *   <li>{@link BlockAction#THROW} — a {@link GuardrailException} is thrown (propagates out of the
 *       hook pipeline; the hook pipeline never swallows it).</li>
 *   <li>{@link BlockAction#ESCALATE} — the agent is paused and a
 *       {@link GuardrailEscalationException} is thrown for the caller to route to HITL.</li>
 *   <li>{@link BlockAction#CANCEL} — {@link HookResult.Cancel} carrying the guardrail reason; the
 *       runtime surfaces it as {@code "Request cancelled: …"}.</li>
 *   <li>{@link BlockAction#FALLBACK} — {@link HookResult.Cancel} carrying the configured fallback
 *       message.</li>
 * </ul>
 */
public class GuardrailPlugin implements Plugin {

    private final List<Guardrail> inputGuardrails;
    private final List<Guardrail> outputGuardrails;
    private final BlockAction blockAction;
    private final String fallbackMessage;
    private Agent agent;

    public GuardrailPlugin(List<Guardrail> inputGuardrails, List<Guardrail> outputGuardrails) {
        this(inputGuardrails, outputGuardrails, BlockAction.FALLBACK, "I cannot process this request.");
    }

    public GuardrailPlugin(List<Guardrail> inputGuardrails, List<Guardrail> outputGuardrails,
                           BlockAction blockAction, String fallbackMessage) {
        this.inputGuardrails = inputGuardrails;
        this.outputGuardrails = outputGuardrails;
        this.blockAction = blockAction;
        this.fallbackMessage = fallbackMessage;
    }

    @Override
    public String name() {
        return "guardrails";
    }

    @Override
    public void initAgent(Agent agent) {
        this.agent = agent;
    }

    @Override
    public List<Guardrail> getInputGuardrails() {
        return inputGuardrails;
    }

    @Override
    public List<Guardrail> getOutputGuardrails() {
        return outputGuardrails;
    }

    @Override
    public BlockAction getBlockAction() {
        return blockAction;
    }

    @Override
    public String getFallbackMessage() {
        return fallbackMessage;
    }

    /**
     * Evaluates the input guardrails against the messages sent to the model and cancels the call
     * when one blocks.
     */
    @Override
    public HookResult beforeModelCall(HookContexts.BeforeModelCallContext ctx) {
        if (inputGuardrails.isEmpty()) {
            return HookResult.Continue.INSTANCE;
        }
        GuardrailResult result = evaluate(inputGuardrails, ctx.messages(), null);
        return result.pass() ? HookResult.Continue.INSTANCE : block(result);
    }

    /**
     * Evaluates the output guardrails against the model response and cancels the run when one
     * blocks.
     */
    @Override
    public HookResult afterModelCall(HookContexts.AfterModelCallContext ctx, String llmResponse) {
        if (outputGuardrails.isEmpty()) {
            return HookResult.Continue.INSTANCE;
        }
        List<ChatMessage> messages = List.of(AiMessage.from(llmResponse == null ? "" : llmResponse));
        GuardrailResult result = evaluate(outputGuardrails, messages, null);
        return result.pass() ? HookResult.Continue.INSTANCE : block(result);
    }

    private HookResult block(GuardrailResult result) {
        return switch (blockAction) {
            case THROW -> throw new GuardrailException(result.reason());
            case ESCALATE -> escalate(result.reason());
            case CANCEL -> new HookResult.Cancel("Guardrail block: " + result.reason());
            case FALLBACK -> new HookResult.Cancel(
                    fallbackMessage != null ? fallbackMessage : "Guardrail block: " + result.reason());
        };
    }

    /** Pauses the agent (if attached) and signals the caller to escalate to HITL. */
    private HookResult escalate(String reason) {
        if (agent != null) {
            agent.pauseExecution();
        }
        throw new GuardrailEscalationException(reason);
    }

    public Agent agent() {
        return agent;
    }

    public static GuardrailResult evaluate(List<Guardrail> guardrails, List<ChatMessage> messages, String context) {
        for (Guardrail guardrail : guardrails) {
            GuardrailResult result = guardrail.validate(messages, context);
            if (!result.pass()) return result;
        }
        return GuardrailResult.ok();
    }
}
