package de.augmentia.quad.core.hook.pipeline;

import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.ChatMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Container for typed context records passed to hook lifecycle methods.
 * Types are adapted to QUAD's use of LangChain4j {@link ChatMessage} and
 * {@link ToolSpecification} directly (no separate Message wrapper).
 */
public final class HookContexts {

    private HookContexts() {}

    public record BeforeAgentContext(
        String sessionId,
        String prompt,
        Map<String, Object> contextVariables
    ) {}

    public record AfterAgentContext(
        String sessionId,
        String result
    ) {}

    public record BeforeModelCallContext(
        String sessionId,
        StringBuilder systemPrompt,
        List<ChatMessage> messages,
        List<ToolSpecification> tools,
        List<ChatMessage> additionalMessages
    ) {}

    /**
     * Outcome of running all before-model-call hooks.
     *
     * <p>{@code messages} are the (possibly anonymized) messages to send to the model; the running
     * transcript is not modified. {@code tools} is the (possibly filtered) tool list.
     * {@code cancelReason}, when non-null, aborts the call.</p>
     */
    public record BeforeModelCallUpdate(
        List<ChatMessage> messages,
        List<ToolSpecification> tools,
        String cancelReason
    ) {
        public boolean cancelled() {
            return cancelReason != null;
        }
    }

    public record AfterModelCallContext(
        String sessionId,
        String llmResponse,
        int inputTokens,
        int outputTokens
    ) {}

    public record BeforeToolCallContext(
        String sessionId,
        String toolName,
        Map<String, Object> arguments
    ) {}

    public record AfterToolCallContext(
        String sessionId,
        String toolName,
        String result,
        boolean isError,
        List<ChatMessage> additionalMessages
    ) {
        public AfterToolCallContext(String sessionId, String toolName, String result, boolean isError) {
            this(sessionId, toolName, result, isError, new ArrayList<>());
        }
    }
}
