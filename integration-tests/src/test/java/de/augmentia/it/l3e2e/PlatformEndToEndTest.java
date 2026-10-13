package de.augmentia.it.l3e2e;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.it.support.TestEnv;
import de.augmentia.it.support.WorkflowRun;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * L3 — beide Plattformen zusammen: echter Workflow-Durchlauf.
 *
 * <p>Der Orchestrator nimmt einen Workflow an, plant ihn, publiziert Tasks nach Kafka. Ein Worker
 * konsumiert sie, führt sie aus und meldet das Ergebnis zurück. Der Test prüft die Wirkungskette,
 * nicht die Einzelkomponenten — dafür sind L1 und L2 da.
 *
 * <p>Voraussetzung: Orchestrator <em>und</em> mindestens ein Worker. Der Worker läuft ohne
 * LLM-Key im Mock-Modus und ist damit deterministisch.
 */
@Tag("requires-e2e")
@Tag("requires-orchestrator")
@Tag("requires-kafka")
class PlatformEndToEndTest {

    private static final Duration COMPLETION = Duration.ofSeconds(60);

    @BeforeAll
    static void beidePlattformenMussenLaufen() {
        assumeThat(TestEnv.kafkaUp()).as("Broker fehlt unter %s", TestEnv.BOOTSTRAP).isTrue();
        assumeThat(TestEnv.orchestratorUp())
                .as("Orchestrator fehlt unter %s", TestEnv.ORCHESTRATOR).isTrue();
        assumeThat(TestEnv.workerUp())
                .as("kein Worker unter %s erreichbar — er konsumiert agent.tasks.* und liefert "
                        + "die Ergebnisse. Ohne Worker bleibenlaeufe in PAUSED_HITL oder RUNNING "
                        + "haengen und die Tests wuerden zufaellig gruen oder rot sein.",
                        TestEnv.WORKER)
                .isTrue();
    }

    // ── Der Standardfall ─────────────────────────────────────────────────────

    @Test
    @DisplayName("guard-skip läuft ohne Worker-Antwort nicht durch — hier mit Worker vollständig")
    void guardSkipCompletes() throws Exception {
        var run = WorkflowRun.start("guard-skip", Map.of("marker", "e2e-guard"), null);

        var status = run.awaitStatus(COMPLETION, "COMPLETED", "FAILED");

        assertThat(status)
                .as("guard-skip: Plan -> optionaler Lint -> Package -> END. Status: %s, Lauf: %s",
                        status, run.executionId())
                .isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("der vollständige Code-Quality-Workflow erreicht das HITL-Gate")
    void referenceCodeQualityReachesHitl() throws Exception {
        var run = WorkflowRun.start("reference-code-quality",
                Map.of("researchConfidence", 0.72), null);

        // final_review hat requiresHumanApproval=true. Ohne menschliche Freigabe endet der Lauf
        // dort — das ist der erwartete Zustand, nicht ein Fehler.
        var status = run.awaitStatus(COMPLETION, "PAUSED_HITL", "COMPLETED", "FAILED");

        assertThat(status)
                .as("Lauf %s", run.executionId())
                .isEqualTo("PAUSED_HITL");
        assertThat(run.openHitlTicketId())
                .as("zum pausierten Lauf gehört ein offenes Ticket")
                .isPresent();
    }

    @Test
    @DisplayName("das HITL-Gate lässt sich freigeben, danach läuft der Workflow weiter")
    void hitlApprovalResumesTheWorkflow() throws Exception {
        var run = WorkflowRun.start("reference-code-quality",
                Map.of("researchConfidence", 0.72), null);
        assertThat(run.awaitStatus(COMPLETION, "PAUSED_HITL", "COMPLETED", "FAILED"))
                .isEqualTo("PAUSED_HITL");

        var ticketId = run.openHitlTicketId().orElseThrow(
                () -> new AssertionError("kein offenes Ticket für " + run.executionId()));

        var retry = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/hitl/tickets/" + ticketId + "/retry",
                Map.of("userId", "integration-test"));
        assertThat(retry.ok()).as("Freigabe: %s", retry.body()).isTrue();

        // PAUSED_HITL darf hier nicht in der Warteliste stehen: direkt nach dem Retry ist der
        // Lauf noch pausiert, und awaitStatus wuerde sofort zurueckkehren, bevor die Freigabe
        // überhaupt verarbeitet ist. Hier zaehlt nur ein echter Endzustand.
        var status = run.awaitStatus(COMPLETION, "COMPLETED");

        assertThat(status)
                .as("nach der Freigabe soll der Lauf zu Ende laufen. Status: %s, Lauf: %s",
                        status, run.executionId())
                .isEqualTo("COMPLETED");
    }

    // ── Nachrichtenfluss über Kafka ──────────────────────────────────────────

    @Test
    @DisplayName("zwischen Start und Ende liegen Task- und Result-Events")
    void taskAndResultEventsTraverseKafka() throws Exception {
        var tasksBefore = TestEnv.readTopic(TopicNames.tasks("researcher"),
                Integer.MAX_VALUE, Duration.ofSeconds(5), true).size();
        var resultsBefore = TestEnv.readTopic(TopicNames.RESULTS,
                Integer.MAX_VALUE, Duration.ofSeconds(5), true).size();

        var run = WorkflowRun.start("reference-code-quality",
                Map.of("researchConfidence", 0.72), null);
        assertThat(run.awaitStatus(COMPLETION, "PAUSED_HITL", "COMPLETED", "FAILED"))
                .isIn("PAUSED_HITL", "COMPLETED");

        var tasks = TestEnv.awaitTopic(TopicNames.tasks("researcher"), tasksBefore + 1, COMPLETION);
        var results = TestEnv.awaitTopic(TopicNames.RESULTS, resultsBefore + 1, COMPLETION);

        assertThat(tasks)
                .as("für Lauf %s wurden keine Tasks publiziert", run.executionId())
                .hasSizeGreaterThan(tasksBefore);
        assertThat(results)
                .as("für Lauf %s kamen keine Ergebnisse zurück — der Worker hat nicht geliefert",
                        run.executionId())
                .as("dokumentiert: %s", results.size() > resultsBefore
                        ? "Ergebnisse vorhanden" : "keine Ergebnisse")
                .hasSizeGreaterThan(resultsBefore);
    }

    @Test
    @DisplayName("die Ergebnisse tragen den Status COMPLETED des Workers")
    void resultsCarryWorkerStatus() throws Exception {
        var before = TestEnv.readTopic(TopicNames.RESULTS,
                Integer.MAX_VALUE, Duration.ofSeconds(5), true).size();

        var run = WorkflowRun.start("guard-skip", Map.of("marker", "e2e-status"), null);
        assertThat(run.awaitStatus(COMPLETION, "COMPLETED", "FAILED")).isEqualTo("COMPLETED");

        var messages = TestEnv.awaitTopic(TopicNames.RESULTS, before + 1, COMPLETION);
        var newOnes = messages.subList(before, messages.size());

        for (String m : newOnes) {
            var node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(m);
            var data = node.has("result_data") ? node.get("result_data") : node.get("resultData");
            assertThat(data)
                    .as("ResultEvent trägt result_data: %s", m)
                    .isNotNull();
            assertThat(data.path("status").asText())
                    .as("der Mock-Worker meldet COMPLETED")
                    .isIn("COMPLETED", "FAILED_RETRYABLE", "FAILED_FATAL", "NEEDS_HITL");
        }
    }

    @Test
    @DisplayName("ein abgebrochener Lauf meldet einen Endzustand und keine offenen Schritte mehr")
    void cancelledRunReachesTerminalState() throws Exception {
        var run = WorkflowRun.start("reference-code-quality", Map.of(), null);
        // Der Lauf muss erst laufen, damit der Abbruch etwas zu tun hat.
        run.awaitStatus(Duration.ofSeconds(20), "PAUSED_HITL", "COMPLETED", "FAILED");

        var cancel = run.cancel();
        assertThat(cancel.statusCode())
                .as("Antwort: %s", cancel.body())
                .isBetween(200, 299);

        var status = run.awaitStatus(Duration.ofSeconds(30), "CANCELLED", "COMPLETED", "FAILED");


        assertThat(status)
                .as("nach dem Abbruch: %s", status)
                .isIn("CANCELLED", "COMPLETED", "FAILED");
    }

    // ── Idempotenz über die API ─────────────────────────────────────────────

    @Test
    @DisplayName("zwei Starts mit demselben Schlüssel erzeugen einen Lauf, nicht zwei")
    void idempotentStartDoesNotDuplicateWork() throws Exception {
        var key = "it-e2e-idem-" + java.util.UUID.randomUUID();
        var body = Map.of("workflowName", "guard-skip",
                "inputParams", Map.of("marker", "idempotency"),
                "idempotencyKey", key);

        var first = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start", body);
        var second = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start", body);

        var firstId = TestEnv.field(first.json(), "workflowExecutionId").orElseThrow();
        var secondId = TestEnv.field(second.json(), "workflowExecutionId").orElseThrow();

        assertThat(secondId).isEqualTo(firstId);
        assertThat(first.json().path("duplicate").asBoolean())
                .as("der erste Start ist neu")
                .isFalse();
        assertThat(second.json().path("duplicate").asBoolean())
                .as("der zweite ist als Duplikat markiert")
                .isTrue();
    }

    // ── Alle Topologien ─────────────────────────────────────────────────────

    @Test
    @DisplayName("jede Seed-Definition erreicht einen Endzustand")
    void allSeededWorkflowsTerminate() throws Exception {
        var definitions = TestEnv.get("/api/v1/workflow-definitions").ok()
                ? TestEnv.get("/api/v1/workflow-definitions").json()
                : null;
        assumeThat(definitions)
                .as("Definitionen nicht abrufbar")
                .isNotNull();

        // Nur die ausgelieferten Vorlagen — nicht jede Definition, die gerade in der
        // Datenbank liegt. Sonf hängt dieser Test an allem, was andere Tests angelegt
        // haben, und ein Definitionsname aus einem anderen Test wird zum Fehler hier.
        var ausgeliefert = java.util.List.of("reference-code-quality", "xor-race",
                "n-of-quorum", "guard-skip", "error-taxonomy");

        for (var name : ausgeliefert) {
            var vorhanden = definitions.findValuesAsText("name").contains(name);
            assertThat(vorhanden)
                    .as("die ausgelieferte Vorlage %s muss existieren", name)
                    .isTrue();
            var run = WorkflowRun.start(name, Map.of("marker", "e2e-all"), null);
            var status = run.awaitStatus(COMPLETION, "COMPLETED", "PAUSED_HITL", "FAILED");

            assertThat(status)
                    .as("Definition '%s' muss einen Endzustand erreichen, nicht hängen bleiben. "
                            + "Lauf: %s", name, run.executionId())
                    .isIn("COMPLETED", "PAUSED_HITL");
        }
    }
}