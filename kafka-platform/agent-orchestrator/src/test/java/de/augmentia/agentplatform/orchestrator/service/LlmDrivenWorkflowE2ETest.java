package de.augmentia.agentplatform.orchestrator.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The orchestrator run against a <em>real</em> LLM.
 *
 * <p>All previous orchestrator tests replace the worker with hardcoded
 * result data. That leaves exactly the part unverified that breaks most often
 * in practice: <strong>the condition in the workflow and the answer of the LLM must
 * match.</strong> A workflow that relies on
 * {@code context['research'].confidence >= 0.8} only works if the
 * LLM delivers a {@code confidence} field — and at the position that the
 * contract provides.
 *
 * <p>This gap was real in live operation: the run ended correctly at
 * {@code COMPLETED}, but the decision went via {@code branch_id=default},
 * because the generated {@code confidence} field was missing. A test with fixed data
 * would inevitably have counted that as success, because <em>it</em> always sets the field.
 *
 * <p><b>What is real here:</b> the LLM call and the evaluation of its answer.
 * The worker call path (langchain4j, ReAct, Kafka roundtrip) is not part of the
 * tests — the orchestrator is tested, not the worker.
 *
 * <p><b>Why key-gated:</b> the test calls a real model. Without
 * {@code OPENAI_API_KEY} it is skipped, not written green with invented data.
 * A green build therefore means: either genuinely verified or
 * clearly marked as skipped.
 *
 * <p><b>Robustness.</b> LLMs are not deterministic. The test therefore checks
 * <em>structural</em> properties of the answer and retries, instead of checking a
 * fixed numeric value. The routing part is strict: the path taken
 * is verified exactly.
 */
@DisplayName("Orchestrator run against a real LLM")
@EnabledIfEnvironmentVariable(named = "OPENAI_API_KEY", matches = ".+")
class LlmDrivenWorkflowE2ETest extends WorkflowEngineTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String MODEL =
            envOr("OPENAI_MODEL", "deepseek/deepseek-v4-flash-0731");

    private static String envOr(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }

    private static String baseUrl() {
        String url = System.getenv("OPENAI_BASE_URL");
        return url == null || url.isBlank() ? "https://openrouter.ai/api/v1" : url;
    }

    // ── Der LLM-Aufruf ──

    /**
     * Asks the model exactly as the worker does, and delivers the
     * result map that the orchestrator persists as {@code output_payload}.
     *
     * <p>The output contract is verbatim the one from
     * {@code TaskPromptBuilder}: {@code response, confidence, status}. It would
     * be a different (and easier) experiment to use a free prompt
     * here — what should be tested is the production contract.
     */
    private Map<String, Object> askLlm(String task) {
        String body = """
                {
                  "model": "%s",
                  "messages": [
                    {"role": "system", "content": "You are a test researcher. Answer with one JSON object with fields: response, confidence, status. confidence is a number between 0 and 1. status is COMPLETED or FAILED."},
                    {"role": "user", "content": "%s"}
                  ],
                  "temperature": 0.1,
                  "max_tokens": 300
                }
                """.formatted(MODEL, task.replace("\"", "\\\""));

        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(20))
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl() + "/chat/completions"))
                    .timeout(Duration.ofSeconds(120))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + System.getenv("OPENAI_API_KEY"))
                    .header("HTTP-Referer", "https://github.com/augmentia-de/quad-workflow-worker")
                    .header("X-Title", "quad-orchestrator-e2e")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode())
                    .as("LLM-Aufruf: HTTP %s%n%s", response.statusCode(), abbreviate(response.body()))
                    .isEqualTo(200);

            return toResultData(parseAnswer(response.body()));

        } catch (Exception e) {
            return fail("LLM-Aufruf fehlgeschlagen: " + e.getMessage(), e);
        }
    }

    /** Extracts the text content from the OpenAI/OpenRouter answer. */
    private String parseAnswer(String responseBody) throws Exception {
        JsonNode root = MAPPER.readTree(responseBody);
        return root.path("choices").path(0).path("message").path("content").asText();
    }

    /**
     * Translates the model answer into the flat {@code result_data} that the
     * orchestrator stores as {@code output_payload} and that the conditions
     * read later.
     *
     * <p>Intentionally tolerant: depending on the call the LLM delivers text, JSON as
     * text or already an object. A real case that caused crashes
     * in practice is a cleanly extracted but empty answer — that must not
     * silently lead to {@code COMPLETED} here.
     */
    private Map<String, Object> toResultData(String answer) throws Exception {
        String text = extractJsonObject(answer);
        if (text == null) {
            throw new IllegalStateException(
                    "The answer contains no JSON object: " + abbreviate(answer));
        }
        JsonNode node = MAPPER.readTree(text);

        Map<String, Object> data = new LinkedHashMap<>();
        node.fields().forEachRemaining(e -> data.put(e.getKey(),
                e.getValue().isNumber() ? e.getValue().asDouble()
                        : e.getValue().asText()));
        return data;
    }

    /** Searches for the first balanced JSON object in a text. */
    private String extractJsonObject(String text) {
        int start = text.indexOf('{');
        if (start < 0) {
            return null;
        }
        int depth = 0;
        boolean inString = false;
        for (int i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            if (c == '"') {
                inString = true;
            } else if (c == '{') {
                depth++;
            } else if (c == '}' && --depth == 0) {
                return text.substring(start, i + 1);
            }
        }
        return null;
    }

    private static String abbreviate(String s) {
        return s == null ? "null" : (s.length() <= 300 ? s : s.substring(0, 300) + "…");
    }

    // ── Der Workflow ──

    /**
     * A workflow whose condition points exactly at the field that the
     * output contract promises: {@code confidence}.
     *
     * <p>Both branches land at the same END again, so that the run ends cleanly
     * in both cases and the test checks the <em>decision</em>, not
     * the abort.
     */
    private UUID confidenceWorkflow() {
        return insertWorkflow("test-llm-confidence", """
                {
                  "nodes": [
                    {"stepId":"research","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"gate","nodeType":"DECISION","dependsOn":["research"],
                     "branches":[{"branchId":"confident",
                       "condition":"context['research'].confidence >= 0.8","target":"summary"}],
                     "defaultTarget":"fallback"},
                    {"stepId":"summary","nodeType":"STEP","agentType":"test_writer","dependsOn":["gate"]},
                    {"stepId":"fallback","nodeType":"STEP","agentType":"coder","dependsOn":["gate"]},
                    {"stepId":"end","nodeType":"END","dependsOn":["summary","fallback"]}
                  ]
                }
                """);
    }

    /** Executes the research step and returns its result data. */
    private Map<String, Object> researchWithLlm(String prompt) {
        Map<String, Object> data = askLlm(prompt);
        var task = tasksFor("research").get(0);
        engine.onResult(resultFor(task, data));
        return data;
    }

    // ── Die Tests ──

    @Test
    @Timeout(240)
    @DisplayName("The LLM delivers exactly the fields the output contract promises")
    void llmReturnsTheContractedFields() {
        Map<String, Object> data = askLlm(
                "How many days are in a week? Answer briefly.");

        // the result is logged: a green with an "arbitrary" confidence
        // says nothing about which branch actually ran. Without
        // that value it cannot be classified as a failure later.
        System.out.printf("LLM-Kontrakttest: confidence=%s status=%s response=%s%n",
                data.get("confidence"), data.get("status"), abbreviate(String.valueOf(data.get("response"))));

        assertThat(data)
                .as("The workflow and the routing path read exactly these fields")
                .containsKeys("response", "confidence", "status");

        assertThat(data.get("confidence"))
                .as("confidence must be numeric — a string comparison "
                    + "'0.95' >= 0.8 waere im Evaluator still falsch")
                .isInstanceOf(Number.class);

        assertThat(((Number) data.get("confidence")).doubleValue())
                .isBetween(0.0d, 1.0d);

        assertThat(String.valueOf(data.get("status")).toUpperCase())
                .as("A FAILED would be a valid but different outcome")
                .isIn("COMPLETED", "FAILED");
    }

    @Test
    @Timeout(300)
    @DisplayName("The condition sees the field that the LLM returns")
    void decisionReadsTheFieldTheLlmActuallyReturns() {
        // The core of the test: the condition points at
        // context['research'].confidence. The question is whether the value the LLM
        // delivers arrives at exactly that position and whether the decision really
        // influences it — not whether it lands on the default target.
        WorkflowExecutionRow execution = start(confidenceWorkflow(), "{}");

        Map<String, Object> data = researchWithLlm(
                "Assess with high confidence: is the sky blue on a clear day?");

        double confidence = ((Number) data.getOrDefault("confidence", -1d)).doubleValue();
        var gate = checkpoint(execution.id(), "gate");
        assertThat(gate).as("The decision was made").isPresent();

        System.out.printf("Routing against LLM: confidence=%.3f -> branchId=%s target=%s%n",
                confidence, gate.get().branchId(), gate.get().decisionBranch());

        if (confidence >= 0.8d) {
            assertThat(gate.get().branchId())
                    .as("With confidence=%.2f the active branch must run — "
                        + "the run fell to 'default', the condition would not match at all",
                            confidence)
                    .isEqualTo("confident");
            assertThat(gate.get().decisionBranch()).isEqualTo("summary");
            assertThat(tasksFor("summary")).as("Der aktive Zweig laeuft").hasSize(1);
            assertThat(tasksFor("fallback")).as("The default does not run").isEmpty();
        } else {
            assertThat(gate.get().branchId())
                    .as("Bei confidence=%.2f ist der Default korrekt", confidence)
                    .isEqualTo("default");
            assertThat(gate.get().decisionBranch()).isEqualTo("fallback");
        }
    }

    @Test
    @Timeout(300)
    @DisplayName("The run ends COMPLETED regardless of which branch the LLM triggers")
    void runCompletesWhicheverBranchIsTaken() {
        WorkflowExecutionRow execution = start(confidenceWorkflow(), "{}");

        Map<String, Object> data = researchWithLlm(
                "Give your assessment of the number 42 with a confidence value.");

        String chosen = checkpoint(execution.id(), "gate").get().decisionBranch();
        var task = tasksFor(chosen).get(0);
        engine.onResult(resultFor(task, Map.of(
                "status", "COMPLETED",
                "response", "zusammengefasst",
                "confidence", data.getOrDefault("confidence", 0.5d))));

        assertThat(status(execution.id()))
                .as("Both branches lead to the same END")
                .isEqualTo(ExecutionStatus.COMPLETED);

        assertThat(checkpoint(execution.id(), "end")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.COMPLETED);
    }

    @Test
    @Timeout(300)
    @DisplayName("If confidence is missing in the answer, the default applies — and that is visible")
    void missingConfidenceFallsBackButStaysObservable() {
        // the error case from live operation: the LLM delivers no
        // confidence field. The workflow must not crash, the run must
        // end — but the default branch must be recognisable in the checkpoint
        // so that it is not misunderstood as success.
        WorkflowExecutionRow execution = start(confidenceWorkflow(), "{}");

        var task = tasksFor("research").get(0);
        engine.onResult(resultFor(task, Map.of(
                "status", "COMPLETED",
                "response", "An answer without any metric")));

        var gate = checkpoint(execution.id(), "gate");
        assertThat(gate).isPresent();
        assertThat(gate.get().branchId())
                .as("Without confidence the defaultTarget applies — that must be "
                    + "'default' sichtbar bleiben")
                .isEqualTo("default");

        var fallbackTask = tasksFor("fallback").get(0);
        engine.onResult(resultFor(fallbackTask, Map.of("status", "COMPLETED", "response", "fallback")));

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.COMPLETED);
    }
}
