package de.augmentia.it.l1api;

import java.util.Map;

import de.augmentia.it.support.TestEnv;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * L1 — die REST-API des kafka-Orchestrators, gegen eine laufende Instanz.
 *
 * <p>Braucht nur HTTP, kein Kafka: der Orchestrator nimmt eine Definition an und liefert einen Lauf
 * zurück, auch wenn niemand die Topics abhört. Getaggt mit {@code requires-orchestrator}, damit ein
 * normaler Checkout nicht rot wird.
 */
@Tag("requires-orchestrator")
class OrchestratorApiTest {

    @BeforeAll
    static void orchestratorMussLaufen() {
        assumeThat(TestEnv.orchestratorUp())
                .as("Orchestrator nicht erreichbar unter %s — scripts/run-integration-tests.sh start starten",
                        TestEnv.ORCHESTRATOR)
                .isTrue();
    }

    // ── Health ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("health meldet UP")
    void healthIsUp() {
        var res = TestEnv.get("/actuator/health");

        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(res.json().path("status").asText()).isEqualTo("UP");
    }

    @Test
    @DisplayName("actuator/health gruppiert liveness und readiness")
    void healthExposesGroups() {
        assertThat(TestEnv.get("/actuator/health").json().path("groups").toString())
                .contains("liveness", "readiness");
    }

    // ── Workflow-Definitionen ────────────────────────────────────────────────

    @Test
    @DisplayName("die Seed-Definitionen sind nach dem Start vorhanden")
    void seededDefinitionsArePresent() {
        var res = TestEnv.get("/api/v1/workflow-definitions");

        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(res.json()).isNotEmpty();

        var names = res.json().findValuesAsText("name");
        assertThat(names).contains("reference-code-quality", "xor-race", "n-of-quorum",
                "guard-skip", "error-taxonomy");
    }

    @Test
    @DisplayName("jede Seed-Definition ist aktiv und hat eine Version")
    void seededDefinitionsAreActive() {
        for (var node : TestEnv.get("/api/v1/workflow-definitions").json()) {
            assertThat(node.path("active").asBoolean())
                    .as("Definition %s muss aktiv sein", node.path("name").asText())
                    .isTrue();
            assertThat(node.path("version").asInt()).isPositive();
            assertThat(node.path("definitionId").asText()).isNotBlank();
        }
    }

    @Test
    @DisplayName("eine neue Definition lässt sich anlegen und taucht in der Liste auf")
    void createDefinition() {
        var name = "it-api-" + java.util.UUID.randomUUID();
        var body = Map.of(
                "name", name,
                "version", 1,
                "dag_structure", Map.of("nodes", java.util.List.of(
                        Map.of("stepId", "s1", "nodeType", "STEP", "agentType", "researcher",
                                "dependsOn", java.util.List.of()),
                        Map.of("stepId", "end", "nodeType", "END",
                                "dependsOn", java.util.List.of("s1")))));

        var created = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflow-definitions", body);

        assertThat(created.statusCode())
                .as("Antwort: %s", created.body())
                .isBetween(200, 299);
        var definitionId = TestEnv.field(created.json(), "definitionId").orElse(null);
        assertThat(definitionId).isNotNull();

        var listed = TestEnv.get("/api/v1/workflow-definitions").json().findValuesAsText("name");
        assertThat(listed).contains(name);
    }

    @Test
    @DisplayName("eine unvollständige Definition wird abgelehnt")
    void createDefinitionRejectsIncompletePayload() {
        // Kein name — der Orchestrator kann die Definition dann nicht referenzieren.
        var body = Map.of("version", 1,
                "dag_structure", Map.of("nodes", java.util.List.of()));

        var res = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflow-definitions", body);

        assertThat(res.ok())
                .as("ohne name darf nichts gespeichert werden: %s", res.body())
                .isFalse();
    }

    @Test
    @DisplayName("ein STEP ohne agentType wird abgelehnt (R8)")
    void createDefinitionRejectsStepWithoutAgentType() {
        var body = Map.of(
                "name", "it-noagent-" + java.util.UUID.randomUUID(),
                "version", 1,
                "dag_structure", Map.of("nodes", java.util.List.of(
                        Map.of("stepId", "s1", "nodeType", "STEP"),
                        Map.of("stepId", "end", "nodeType", "END",
                                "dependsOn", java.util.List.of("s1")))));

        var res = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflow-definitions", body);

        assertThat(res.ok())
                .as("R8: STEP ohne agentType hat kein Task-Topic und muss abgelehnt werden: %s", res.body())
                .isFalse();
    }

    // ── Laufe ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("ein Start liefert eine Execution-ID und den Status RUNNING")
    void startWorkflow() {
        var key = "it-" + java.util.UUID.randomUUID();
        var res = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start",
                Map.of("workflowName", "reference-code-quality",
                        "inputParams", Map.of("researchConfidence", 0.72),
                        "idempotencyKey", key));

        assertThat(res.statusCode()).as("Antwort: %s", res.body()).isBetween(200, 299);
        var execId = TestEnv.field(res.json(), "workflowExecutionId").orElse(null);
        assertThat(execId).isNotNull();

        var status = TestEnv.get("/api/v1/workflows/" + execId);
        assertThat(status.statusCode()).isEqualTo(200);
        assertThat(TestEnv.field(status.json(), "status")).isPresent();
        assertThat(TestEnv.field(status.json(), "workflowName")).contains("reference-code-quality");
    }

    @Test
    @DisplayName("derselbe idempotencyKey liefert denselben Lauf zurück")
    void idempotentStartReturnsTheSameExecution() {
        var key = "it-idem-" + java.util.UUID.randomUUID();
        var body = Map.of("workflowName", "guard-skip",
                "inputParams", Map.of("x", 1),
                "idempotencyKey", key);

        var first = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start", body);
        var second = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start", body);

        assertThat(first.statusCode()).isBetween(200, 299);
        assertThat(second.statusCode()).isBetween(200, 299);

        var firstId = TestEnv.field(first.json(), "workflowExecutionId").orElseThrow();
        var secondId = TestEnv.field(second.json(), "workflowExecutionId").orElseThrow();

        assertThat(secondId)
                .as("gleicher idempotencyKey -> gleicher Lauf, sonst startet zweimal dieselbe Arbeit")
                .isEqualTo(firstId);
    }

    @Test
    @DisplayName("eine unbekannte Definition wird abgelehnt")
    void startUnknownWorkflowIsRejected() {
        var res = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start",
                Map.of("workflowName", "gibt-es-nicht",
                        "inputParams", Map.of(),
                        "idempotencyKey", "it-" + java.util.UUID.randomUUID()));

        assertThat(res.ok()).as("Antwort: %s", res.body()).isFalse();
    }

    @Test
    @DisplayName("eine unbekannte Execution-ID liefert 404")
    void unknownExecutionIdIs404() {
        var res = TestEnv.get("/api/v1/workflows/" + java.util.UUID.randomUUID());

        assertThat(res.statusCode()).isEqualTo(404);
    }

    @Test
    @DisplayName("checkpoints eines frischen Laufs sind erreichbar")
    void checkpointsEndpointResponds() {
        var res = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start",
                Map.of("workflowName", "guard-skip",
                        "inputParams", Map.of(),
                        "idempotencyKey", "it-" + java.util.UUID.randomUUID()));
        var execId = TestEnv.field(res.json(), "workflowExecutionId").orElseThrow();

        var cps = TestEnv.get("/api/v1/workflows/" + execId + "/checkpoints");

        assertThat(cps.statusCode()).isEqualTo(200);
        assertThat(cps.json().isArray())
                .as("die Liste ist auch im laufenden Zustand ein Array, nicht null")
                .isTrue();
    }

    // ── HITL ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("die Ticket-Liste ist abrufbar")
    void hitlTicketsAreListable() {
        var res = TestEnv.get("/api/v1/hitl/tickets?status=OPEN");

        assertThat(res.statusCode()).as("Antwort: %s", res.body()).isEqualTo(200);
        assertThat(res.json().isArray()).isTrue();
    }

    // ── Unified Gateway ─────────────────────────────────────────────────────

    @Test
    @DisplayName("der Unified Gateway meldet Metriken")
    void unifiedGatewayMetrics() {
        var res = TestEnv.get("/api/v1/unified/metrics");

        assertThat(res.statusCode()).as("Antwort: %s", res.body()).isBetween(200, 299);
    }
}