package de.augmentia.agentplatform.common.condition;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract between {@code ExpressionEvaluator} and the context that the
 * orchestrator actually builds.
 *
 * <p>The context is created in {@code JdbcCheckpointStore.outputContext} as
 * {@code context[stepId] = <output_payload>}. {@code output_payload} is exactly what
 * the worker reported under {@code result_data} — thus <em>flat</em>:
 * <pre>
 *   context["trigger"] == {status: "COMPLETED", mode: "go", confidence: 0.9}
 * </pre>
 *
 * <p>The valid path to {@code mode} is therefore {@code context['trigger'].mode}.
 * An intermediate level {@code .resultData} does not exist in this context.
 */
class ExpressionEvaluatorContextContractTest {

    private static Map<String, Object> ctx(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    /** The context exactly in the form that outputContext produces. */
    private static Map<String, Object> realContext() {
        return ctx("trigger", Map.of(
                "status", "COMPLETED",
                "mode", "go",
                "confidence", 0.9));
    }

    @Test
    @DisplayName("Flacher Pfad context[step].field wertet korrekt aus")
    void flatPathResolves() {
        assertThat(ExpressionEvaluator.eval(
                "context['trigger'].mode == 'go'", realContext(), Map.of())).isTrue();
    }

    @Test
    @DisplayName("A path with .resultData stays unresolved — the intermediate level does not exist")
    void resultDataPathDoesNotResolve() {
        // that is the form that stands in the shipped workflows.
        // It can never become true, so every DECISION falls to defaultTarget.
        assertThat(ExpressionEvaluator.eval(
                "context['trigger'].resultData.mode == 'go'", realContext(), Map.of()))
                .as("resultData is not a level in the context — the condition is not satisfiable")
                .isFalse();
    }

    @Test
    @DisplayName("The delivered workflow conditions from error-taxonomy are not satisfiable")
    void shippedConditionNeverFires() {
        // Wortgleich aus agent-orchestrator/src/main/resources/workflows/error-taxonomy.json
        String shipped = "context['trigger'].resultData.mode == 'retryable'";
        assertThat(ExpressionEvaluator.eval(shipped, realContext(), Map.of())).isFalse();
    }

    @Test
    @DisplayName("The delivered condition from reference-code-quality is not satisfiable")
    void shippedConfidenceConditionNeverFires() {
        // Wortgleich aus reference-code-quality.json
        String shipped = "context['research_step'].resultData.confidence < 0.8";
        Map<String, Object> lowConfidence = ctx("research_step", Map.of(
                "status", "COMPLETED",
                "confidence", 0.5));
        assertThat(ExpressionEvaluator.eval(shipped, lowConfidence, Map.of()))
                .as("confidence 0.5 < 0.8 should match, but it does not")
                .isFalse();

        // With the corrected path the same intent applies:
        assertThat(ExpressionEvaluator.eval(
                "context['research_step'].confidence < 0.8", lowConfidence, Map.of())).isTrue();
    }
}
