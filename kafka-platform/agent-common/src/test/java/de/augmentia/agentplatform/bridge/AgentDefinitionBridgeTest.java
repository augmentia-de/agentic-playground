package de.augmentia.agentplatform.bridge;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The mapping of a quad agent onto the Kafka agent.
 *
 * <p>There was a bug here that no test saw, because there was no test for
 * {@code fromQuadAgent}. The field in the record was called {@code maxReActIterations} but
 * contained a token budget, and the default was calculated for that interpretation: without
 * {@code maxTokens} the value 8 iterations remained, which the caller reverse-calculated —
 * {@code max_tokens = 32} in the database. The worker turns that into
 * {@code paramsBuilder.maxOutputTokens(32)} and thereby truncates every answer.
 *
 * <p>These tests pin down that the value is passed through and that the default is a
 * Token-Budget ist.
 */
@DisplayName("AgentDefinitionBridge — Token-Budget")
class AgentDefinitionBridgeTest {

    private static AgentDefinitionBridge.QuarkusAgent agent(Integer maxTokens) {
        return agent(maxTokens, null);
    }

    private static AgentDefinitionBridge.QuarkusAgent agent(Integer maxTokens, Integer maxReActIterations) {
        return new AgentDefinitionBridge.QuarkusAgent(
                "coder-1", "Coder", "code", "gpt-4o-mini", 0.2, maxTokens,
                "Du schreibst Code", 120, 2, 1, maxReActIterations, List.of());
    }

    @Nested
    @DisplayName("maxTokens von quad")
    class MaxTokens {

        @Test
        @DisplayName("wandert unveraendert in maxOutputTokens")
        void passesThroughUnchanged() {
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(4096)).maxOutputTokens())
                    .isEqualTo(4096);
        }

        @Test
        @DisplayName("small values also stay unchanged")
        void smallValuesSurvive() {
            // Previously: /4 in the bridge, *4 in the gateway. A value below 4 was
            // rounded to 1 iteration and came back as 4 instead of 1.
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(1)).maxOutputTokens())
                    .isEqualTo(1);
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(12)).maxOutputTokens())
                    .isEqualTo(12);
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(999)).maxOutputTokens())
                    .isEqualTo(999);
        }
    }

    @Nested
    @DisplayName("without maxTokens from quad")
    class MissingMaxTokens {

        @Test
        @DisplayName("a token budget applies, not an iteration value")
        void fallsBackToTokenBudget() {
            // 8 was plausible for iterations; as a token budget it means that
            // the answer is truncated after 8 tokens. 1024 matches what
            // JdbcAgentDefinitionStore.ensureStub writes for the same column.
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(null)).maxOutputTokens())
                    .isEqualTo(1024);
        }

        @Test
        @DisplayName("the budget lies well above a complete answer")
        void budgetIsUsable() {
            var config = AgentDefinitionBridge.fromQuadAgent(agent(null));
            assertThat(config.maxOutputTokens())
                    .as("must not lie in the low three-digit range — "
                            + "the value is a limit for the answer length")
                    .isGreaterThan(512);
        }
    }

    @Nested
    @DisplayName("unsinnige maxTokens von quad")
    class NonsensicalMaxTokens {

        @Test
        @DisplayName("0 and negative values take the default")
        void zeroAndNegativeFallBack() {
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(0)).maxOutputTokens())
                    .isEqualTo(1024);
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(-1)).maxOutputTokens())
                    .isEqualTo(1024);
        }
    }

    @Nested
    @DisplayName("ReAct-Iterationsgrenze")
    class MaxReActIterations {

        @Test
        @DisplayName("kommt von quad unveraendert an")
        void passesThrough() {
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(4096, 25)).maxReActIterations())
                    .isEqualTo(25);
        }

        @Test
        @DisplayName("if it is missing, the default of the caller applies")
        void fallsBackToCallerDefault() {
            // The default comes from the caller so that it is configurable — the bridge
            // has no configuration.
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(4096, null), 33)
                    .maxReActIterations()).isEqualTo(33);
        }

        @Test
        @DisplayName("0 and negative values fall back to the default")
        void nonPositiveFallsBack() {
            // passing 0 through would make the agent fail only at worker start,
            // quad rejects 0 in Agent.setReActMaxIterations, the DB has a
            // CHECK. The upload is the right place for it.
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(4096, 0), 5)
                    .maxReActIterations()).isEqualTo(5);
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(4096, -3), 5)
                    .maxReActIterations()).isEqualTo(5);
        }

        @Test
        @DisplayName("der Default ist selbst >= 0")
        void evenABrokenCallerDefaultYieldsAUsableValue() {
            // A configuration value <= 0 must not get through into the DB.
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(4096, null), 0)
                    .maxReActIterations())
                    .isEqualTo(AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS);
        }

        @Test
        @DisplayName("the default without a caller value matches the previously hardcoded one")
        void defaultMatchesPreviousHardcodedValue() {
            assertThat(AgentDefinitionBridge.fromQuadAgent(agent(4096, null)).maxReActIterations())
                    .as("vorher stand in JdbcAgentConfigSource fest 10")
                    .isEqualTo(10);
        }
    }

    @Test
    @DisplayName("without version there is no crash")
    void missingVersionDoesNotThrow() {
        var withoutVersion = new AgentDefinitionBridge.QuarkusAgent(
                "x", null, null, null, null, null, null, null, null, null, null, null);

        var config = AgentDefinitionBridge.fromQuadAgent(withoutVersion);

        assertThat(config.version()).isEqualTo(1);
        assertThat(config.agentId()).isEqualTo("x");
        assertThat(config.maxOutputTokens()).isEqualTo(1024);
        assertThat(config.maxReActIterations())
                .isEqualTo(AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS);
    }

    @Test
    @DisplayName("the rest of the mapping stays untouched")
    void otherFieldsUnchanged() {
        var withTools = new AgentDefinitionBridge.QuarkusAgent(
                "planner", "Planer", "plan", "gpt-4o", 0.7, 2048,
                "System", 300, 5, 9, 25,
                List.of(new AgentDefinitionBridge.QuarkusToolDefinition(
                        "readFile", "liest", Map.of(), null, List.of(), Map.of())));

        var config = AgentDefinitionBridge.fromQuadAgent(withTools);

        assertThat(config.agentId()).isEqualTo("planner");
        assertThat(config.agentName()).isEqualTo("Planer");
        assertThat(config.category()).isEqualTo("plan");
        assertThat(config.temperature()).isEqualTo(0.7);
        assertThat(config.systemPrompt()).isEqualTo("System");
        assertThat(config.timeoutSeconds()).isEqualTo(300);
        assertThat(config.maxRetryAttempts()).isEqualTo(5);
        assertThat(config.version()).isEqualTo(9);
        assertThat(config.allowedTools()).containsExactly("readFile");
        assertThat(config.maxReActIterations()).isEqualTo(25);
    }
}