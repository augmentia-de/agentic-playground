package de.augmentia.quad.worker.agent;

import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.worker.config.ChatModelRegistry;
import de.augmentia.quad.worker.config.WorkerProperties;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.worker.session.TaskSessionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-End Test der QUAD-Worker Pipeline: Factory → Agent → LLM.
 *
 * Item 3: lifts the complete task execution without Kafka/DB.
 * Key-gated: if OPENAI_API_KEY is set, the real LLM call is
 * executed (OpenRouter/OpenAI over langchain4j). If no key is present,
 * the registry falls back to the deterministic MockChatModel —
 * the test thereby stays green in every environment.
 *
 * Assertions are kept the same for both modes except for one marker,
 * so that CI without a key (mock) and with a key (real) run identically.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class WorkerExecutionE2ETest {

    private static final boolean REAL_LLM = isRealLlmConfigured();

    private static boolean isRealLlmConfigured() {
        String key = System.getenv("OPENAI_API_KEY");
        return key != null && !key.isBlank();
    }

    @Test
    void fullWorkerPipelineRunsEndToEnd() throws Exception {
        WorkerAgentFactory factory = WorkerTestFixtures.factoryWith(WorkerTestFixtures.registry());

        TaskEvent task = WorkerTestFixtures.createTask("wt-exec", "What did the dog say?");
        TaskPromptBuilder promptBuilder = new TaskPromptBuilder();
        String prompt = promptBuilder.build(task, null);

        // Session setzen (ThreadLocal) wie in TaskExecutor.execute()
        AgentSessionState state = new TaskSessionContext("/tmp/quad-worker/workspace-e2e").create(task);
        CurrentSession.setCurrent(state);
        try {
            AgentResult result = factory.create("researcher")
                .executeStructured(prompt, state);

            assertThat(result).isNotNull();
            assertThat(result.finalAnswer()).isNotBlank();
            // Structure check: the answer must contain the agent type/context,
            // bei Mock exakt, bei echtem LLM zumindest formal.
            if (!REAL_LLM) {
                assertThat(result.finalAnswer()).contains("Mock response");
                assertThat(result.finalAnswer()).contains("confidence");
                assertThat(result.finalAnswer()).contains("COMPLETED");
            }
        } finally {
            CurrentSession.setCurrent(null);
        }
    }

    @Test
    void unknownAgentTypeThrowsE2E() {
        WorkerAgentFactory factory = WorkerTestFixtures.factoryWith(WorkerTestFixtures.registry());

        assertThatThrownBy(() -> factory.create("nonexistent-agent"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown agent type");
    }

    @Test
    void dbSourceWinsBeforeYamlFallback() throws Exception {
        // Item 1: ordered sources — the "DB" source (here a fake) yields
        // a match first, the YAML fallback is NOT consulted.
        ChatModelRegistry registry = WorkerTestFixtures.registry();

        WorkerProperties yamlProps = WorkerTestFixtures.baseProperties(Map.of(
            "researcher", WorkerTestFixtures.researcherConfig("YAML-FALLBACK-SYSTEM-PROMPT", false)));

        var dbSource = new de.augmentia.quad.worker.config.AgentConfigSource() {
            @Override
            public java.util.Optional<WorkerProperties.AgentConfig> find(String agentType) {
                if ("researcher".equals(agentType)) {
                    return java.util.Optional.of(
                        WorkerTestFixtures.researcherConfig("DB-FIRST-SYSTEM-PROMPT", true));
                }
                return java.util.Optional.empty();
            }

            @Override
            public String name() {
                return "fake-db-source";
            }
        };

        var yamlSource = new de.augmentia.quad.worker.config.PropertiesAgentConfigSource(yamlProps);

        WorkerAgentFactory factory = WorkerTestFixtures.factoryWith(
            registry, yamlProps, java.util.List.of(dbSource, yamlSource));

        TaskEvent task = WorkerTestFixtures.createTask("wf-db", "Use DB config");
        AgentSessionState state = new TaskSessionContext("/tmp/quad-worker/workspace-e2e").create(task);
        CurrentSession.setCurrent(state);
        try {
            var agent = factory.create("researcher");
            // The system prompt of the DB source has won (not the YAML fallback).
            assertThat(agent.getSystemPrompt()).isEqualTo("DB-FIRST-SYSTEM-PROMPT");
        } finally {
            CurrentSession.setCurrent(null);
        }
    }

    @Test
    void emptySourceListUsesYamlFallback() throws Exception {
        // Without agents as a YAML entry, but with an empty source list, the
        // YAML fallback must apply (default, no DB mode).
        WorkerAgentFactory factory = WorkerTestFixtures.factoryWith(WorkerTestFixtures.registry());

        TaskEvent task = WorkerTestFixtures.createTask("wf-yaml", "Use YAML config");
        AgentSessionState state = new TaskSessionContext("/tmp/quad-worker/workspace-e2e").create(task);
        CurrentSession.setCurrent(state);
        try {
            var agent = factory.create("researcher");
            assertThat(agent.getSystemPrompt()).contains("test researcher");
        } finally {
            CurrentSession.setCurrent(null);
        }
    }
}