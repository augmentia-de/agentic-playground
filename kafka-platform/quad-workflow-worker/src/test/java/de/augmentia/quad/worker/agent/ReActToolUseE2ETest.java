package de.augmentia.quad.worker.agent;

import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.worker.config.ChatModelRegistry;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.worker.session.TaskSessionContext;
import de.augmentia.quad.worker.tool.AgentToolRegistryFactory;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Level 2 — ReAct + tool use.
 *
 * 1) <b>Deterministic (without key):</b> a tool-aware mock LLM answers in the
 *    first ReAct pass with a ToolExecutionRequest. The agent must really
 *    execute the tool (AgentRuntime.executeTool) and feed the tool result into the
 *    next LLM pass. That is the core proof: ReAct loop +
 *    tool execution + result answer based on the tool result.
 * 2) <b>Real (key-gated):</b> only with OPENAI_API_KEY — the real LLM (OpenRouter/
 *    OpenAI) must, in a task that is only solvable with a tool, actually
 *    trigger the tool call and thereby form the answer.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ReActToolUseE2ETest {

    private static final String SECRET = "secret-value-42";

    // ── Test tool: yields a value the LLM can only get via the tool ──

    public static class SecretReaderTool {
        @Tool("Reads the secret from the workspace. Returns the secret value.")
        public String readSecret(String path) {
            return "SECRET=" + SECRET;
        }
    }

    // ── 1) Deterministischer ReAct + Tool-Use Beweis ──

    @Test
    void reactLoopExecutesToolAndUsesResult() throws Exception {
        ChatModelRegistry registry = ChatModelRegistryFactory.toolAwareMock();

        AgentToolRegistryFactory toolFactory = WorkerTestFixtures.toolFactoryWith(Set.of(new SecretReaderTool()));
        var props = WorkerTestFixtures.baseProperties(java.util.Map.of("researcher",
                new de.augmentia.quad.worker.config.WorkerProperties.AgentConfig(
                    "researcher", "simple", 0.2, 8, null,
                    Set.of("readSecret"), false,
                    "You are a researcher. Use tools when needed. Produce result as JSON response/confidence/status.",
                    null, null, false)));

        WorkerAgentFactory factory = WorkerTestFixtures.factoryWith(registry, toolFactory, props);

        TaskEvent task = WorkerTestFixtures.createTask("react-tool", "Read the secret and answer.");
        AgentSessionState state = new TaskSessionContext("/tmp/quad-worker/workspace-e2e").create(task);
        CurrentSession.setCurrent(state);
        try {
            var agent = factory.create("researcher");
            AgentResult result = agent.executeStructured(
                new TaskPromptBuilder().build(task, null), state);

            assertThat(result.finalAnswer()).isNotBlank();
            assertThat(result.finalAnswer()).contains(SECRET);
        } finally {
            CurrentSession.setCurrent(null);
        }
    }

    // ── 2) Real LLM tool use (only with key) ──

    @Test
    @EnabledIfEnvironmentVariable(named = "OPENAI_API_KEY", matches = ".+")
    void realLlmUsesToolForAnswer() throws Exception {
        ChatModelRegistry registry = WorkerTestFixtures.registry();

        AgentToolRegistryFactory toolFactory = WorkerTestFixtures.toolFactoryWith(Set.of(new SecretReaderTool()));
        var props = WorkerTestFixtures.baseProperties(java.util.Map.of("researcher",
                new de.augmentia.quad.worker.config.WorkerProperties.AgentConfig(
                    "researcher", "simple", 0.2, 8, null,
                    Set.of("readSecret"), true,
                    "You are a researcher. To answer questions about the secret you MUST call the readSecret tool. "
                    + "Never guess the secret. Report the exact secret value you got from the tool.",
                    null, null, false)));

        WorkerAgentFactory factory = WorkerTestFixtures.factoryWith(registry, toolFactory, props);

        TaskEvent task = WorkerTestFixtures.createTask("real-tool",
            "What is the secret value stored in the workspace? Call the readSecret tool and answer with only that value.");
        AgentSessionState state = new TaskSessionContext("/tmp/quad-worker/workspace-e2e").create(task);
        CurrentSession.setCurrent(state);
        try {
            var agent = factory.create("researcher");
            AgentResult result = agent.executeStructured(
                new TaskPromptBuilder().build(task, null), state);

            assertThat(result.finalAnswer()).isNotBlank();
            assertThat(result.finalAnswer()).contains(SECRET);
        } finally {
            CurrentSession.setCurrent(null);
        }
    }

    // ── LLM-Fake: erster Pass -> ToolCall, zweiter Pass -> final mit Tool-Result ──

    private static final class ToolAwareMockChatModel implements ChatModel {
        @Override
        public ChatResponse chat(ChatRequest request) {
            List<? extends dev.langchain4j.data.message.ChatMessage> history = request.messages();
            boolean hasToolResult = history.stream()
                .anyMatch(m -> m instanceof ToolExecutionResultMessage);

            if (!hasToolResult) {
                // ReAct pass 1: the LLM requests tool execution.
                ToolExecutionRequest req = ToolExecutionRequest.builder()
                    .id("call-secret-1")
                    .name("readSecret")
                    .arguments("{\"path\":\"secret\"}")
                    .build();
                return ChatResponse.builder()
                    .aiMessage(AiMessage.from("Need to call the tool first.", List.of(req)))
                    .build();
            }

            // ReAct pass 2: the tool result is in the history -> final answer.
            String toolResult = history.stream()
                .filter(m -> m instanceof ToolExecutionResultMessage)
                .map(m -> ((ToolExecutionResultMessage) m).text())
                .findFirst().orElse("(no tool result)");

            return ChatResponse.builder()
                .aiMessage(AiMessage.from(
                    "{\"response\": \"The secret is in the tool result. " + toolResult + "\", "
                    + "\"confidence\": 0.99, \"status\": \"COMPLETED\"}"))
                .build();
        }
    }

    /** Kleine Factory um Test-interne (package-private) Mock-LLMs zu registrieren. */
    private static final class ChatModelRegistryFactory {
        static ChatModelRegistry toolAwareMock() {
            ChatModelRegistry registry = new ChatModelRegistry();
            registry.register("simple", new ToolAwareMockChatModel());
            return registry;
        }
    }
}