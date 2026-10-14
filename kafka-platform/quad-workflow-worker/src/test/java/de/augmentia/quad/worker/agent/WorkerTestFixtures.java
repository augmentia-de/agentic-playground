package de.augmentia.quad.worker.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.worker.config.ChatModelRegistry;
import de.augmentia.quad.worker.config.WorkerProperties;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.worker.tool.AgentToolRegistryFactory;
import de.augmentia.quad.worker.tool.ToolCatalog;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared fixtures for the worker E2E tests (factory, registry, AgentConfig).<p>
 *
 * The registry is <strong>key-gated</strong>: if {@code OPENAI_API_KEY} is
 * set, the real LLM call (OpenRouter/OpenAI) is used — otherwise the
 * deterministic {@link ChatModelRegistry.MockChatModel}. The tests thereby remain
 * green in every environment, without a secret in the test code.
 */
public final class WorkerTestFixtures {

    public static final String MODEL_KEY = "simple";

    private WorkerTestFixtures() {}

    public static ChatModelRegistry registry() {
        String apiKey = System.getenv("OPENAI_API_KEY");
        ChatModelRegistry registry = new ChatModelRegistry();

        if (apiKey == null || apiKey.isBlank()) {
            registry.register(MODEL_KEY, new ChatModelRegistry.MockChatModel());
            return registry;
        }

        String baseUrl = System.getenv("OPENAI_BASE_URL");
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = "https://openrouter.ai/api/v1";
        }
        String modelName = System.getenv("OPENAI_MODEL");
        if (modelName == null || modelName.isBlank()) {
            modelName = "deepseek/deepseek-v4-flash-0731";
        }

        var builder = OpenAiChatModel.builder()
            .apiKey(apiKey)
            .modelName(modelName)
            .baseUrl(baseUrl)
            .timeout(Duration.ofSeconds(120))
            .maxTokens(512)
            .temperature(0.1);

        if (baseUrl.contains("openrouter")) {
            builder.customHeaders(Map.of(
                "HTTP-Referer", "https://github.com/augmentia-de/quad-workflow-worker",
                "X-Title", "quad-workflow-worker"
            ));
        }
        registry.register(MODEL_KEY, builder.build());
        return registry;
    }

    public static ChatModelRegistry registryWith(ChatModel model) {
        ChatModelRegistry registry = new ChatModelRegistry();
        registry.register(MODEL_KEY, model);
        return registry;
    }

    public static WorkerProperties researcherProperties() {
        return baseProperties(Map.of("researcher",
            new WorkerProperties.AgentConfig(
                "researcher", MODEL_KEY, 0.2, 8, null,
                Set.of(), false,
                "You are a test researcher. Always answer with JSON fields: response, confidence, status.",
                null, null, false)));
    }

    public static WorkerProperties.AgentConfig researcherConfig(String systemPrompt, boolean structuredOutput) {
        return new WorkerProperties.AgentConfig(
            "researcher", MODEL_KEY, 0.2, 8, null,
            Set.of(), structuredOutput, systemPrompt,
            null, null, false);
    }

    public static WorkerProperties baseProperties(Map<String, WorkerProperties.AgentConfig> agents) {
        return new WorkerProperties(
            true,
            "quad-workflow-worker",
            "agent\\.tasks\\..*",
            "agent.execution.results",
            "agent.execution.dlq",
            8,
            120_000L,
            "/tmp/quad-worker/workspace-e2e",
            agents,
            new WorkerProperties.KafkaConfig()
        );
    }

    public static AgentToolRegistryFactory toolFactory() {
        ObjectMapper mapper = new ObjectMapper();
        ToolCatalog catalog = new ToolCatalog(new de.augmentia.quad.core.tool.ToolArgsMapper(mapper), Set.of());
        return new AgentToolRegistryFactory(catalog, new de.augmentia.quad.core.tool.ToolArgsMapper(mapper));
    }

    /** Factory with real tools: allows tool use in the ReAct loop. */
    public static AgentToolRegistryFactory toolFactoryWith(java.util.Set<Object> builtinTools) {
        ObjectMapper mapper = new ObjectMapper();
        ToolCatalog catalog = new ToolCatalog(new de.augmentia.quad.core.tool.ToolArgsMapper(mapper), builtinTools);
        return new AgentToolRegistryFactory(catalog, new de.augmentia.quad.core.tool.ToolArgsMapper(mapper));
    }

    /** Factory with tools, constructor trustworthy for tool tests. */
    public static WorkerAgentFactory factoryWith(ChatModelRegistry registry, AgentToolRegistryFactory toolFactory,
                                          WorkerProperties props) {
        return new WorkerAgentFactory(
            registry,
            toolFactory,
            null,
            new de.augmentia.quad.core.session.WorkspaceResolver(),
            new de.augmentia.quad.core.agent.runtime.IdempotencyStore(),
            new de.augmentia.quad.core.agent.runtime.OTelAgentTracer(),
            null,
            props
        );
    }

    public static WorkerAgentFactory factoryWith(ChatModelRegistry registry) {
        return new WorkerAgentFactory(
            registry,
            toolFactory(),
            null,
            new de.augmentia.quad.core.session.WorkspaceResolver(),
            new de.augmentia.quad.core.agent.runtime.IdempotencyStore(),
            new de.augmentia.quad.core.agent.runtime.OTelAgentTracer(),
            null,
            researcherProperties()
        );
    }

    public static WorkerAgentFactory factoryWith(ChatModelRegistry registry, WorkerProperties props,
                                          java.util.List<de.augmentia.quad.worker.config.AgentConfigSource> sources) {
        return new WorkerAgentFactory(
            registry,
            toolFactory(),
            null,
            new de.augmentia.quad.core.session.WorkspaceResolver(),
            new de.augmentia.quad.core.agent.runtime.IdempotencyStore(),
            new de.augmentia.quad.core.agent.runtime.OTelAgentTracer(),
            null,
            props,
            sources
        );
    }

    public static TaskEvent createTask(String taskId, String input) {
        return createTask(taskId, input, 120_000L);
    }

    public static TaskEvent createTask(String taskId, String input, long timeoutMs) {
        return new TaskEvent(
            new TaskEvent.Header("wf-e2e", taskId, null, 0, System.currentTimeMillis()),
            "researcher",
            "step-1",
            0,
            input,
            null,
            List.of(),
            timeoutMs
        );
    }

    /**
     * Virtual thread executor analogous to the production bean {@code quadAgentExecutor}.
     * Deliberately NOT {@code ForkJoinPool.commonPool()}: the session binding via
     * {@code CurrentSession} (ThreadLocal) has to happen on the executing thread
     * instead, and the parallelism must not depend on the CPU count.
     */
    public static java.util.concurrent.ExecutorService virtualExecutor() {
        return java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    }
}