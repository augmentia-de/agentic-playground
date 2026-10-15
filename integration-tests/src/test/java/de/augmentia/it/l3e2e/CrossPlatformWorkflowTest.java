package de.augmentia.it.l3e2e;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import de.augmentia.it.support.TestEnv;
import de.augmentia.it.support.WorkflowRun;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * L3, Teil 2 — beide Plattformen im selben Lauf.
 *
 * <p>Ein Workflow wird auf <b>quad</b> angelegt, über die Unified-Bridge nach
 * <b>kafka-platform</b> übernommen und dort asynchron ausgeführt. Geprüft wird die Kette in
 * beiden Richtungen: quad nimmt die Definition und führt sie selbst aus, kafka-platform
 * konvertiert dieselbe Definition in seinen eigenen DAG-Typ und führt denselben Ablauf über
 * Kafka aus.
 *
 * <p>Voraussetzung: quad unter {@code it.quad.url} (Default {@value TestEnv#QUAD}),
 * Orchestrator unter {@code it.orchestrator.url} (Default {@value TestEnv#ORCHESTRATOR})
 * und ein Worker.
 */
@Tag("requires-e2e")
@Tag("requires-orchestrator")
class CrossPlatformWorkflowTest {

    private static final String QUAD = TestEnv.QUAD;
    private static final Duration COMPLETION = Duration.ofSeconds(60);

    /** Vier Knoten: drei Agent-Schritte in Reihe und ein END. Bewusst über alle drei Ebenen. */
    private static Map<String, Object> quadWorkflow(String name) {
        return Map.of(
                "name", name,
                "nodes", List.of(
                        Map.of("id", "plan", "type", "agent", "agentType", "planner", "name", "Plan"),
                        Map.of("id", "build", "type", "agent", "agentType", "coder", "name", "Build"),
                        Map.of("id", "check", "type", "agent", "agentType", "linter", "name", "Check"),
                        Map.of("id", "done", "type", "end")),
                "edges", List.of(
                        Map.of("source", "plan", "target", "build"),
                        Map.of("source", "build", "target", "check"),
                        Map.of("source", "check", "target", "done")));
    }

    @BeforeAll
    static void beideBackendsMüssenLaufen() {
        assumeThat(TestEnv.quadUp())
                .as("quad fehlt unter %s", QUAD).isTrue();
        assumeThat(TestEnv.orchestratorUp())
                .as("Orchestrator fehlt unter %s", TestEnv.ORCHESTRATOR).isTrue();
    }

    // ── quad nimmt die Definition an ─────────────────────────────────────────

    @Test
    @DisplayName("quad legt einen Workflow an und liefert ihn unverändert zurück")
    void quadAcceptsAndReturnsTheWorkflow() {
        var name = "quad-rest-" + UUID.randomUUID();
        var created = TestEnv.postJson(QUAD + "/api/ui/workflows", quadWorkflow(name));
        assertThat(created.ok()).as("Antwort: %s", created.body()).isTrue();

        var id = TestEnv.field(created.json(), "id").orElseThrow(
                () -> new AssertionError("keine id in " + created.body()));
        var fetched = TestEnv.get(QUAD + "/api/ui/workflows/" + id);
        assertThat(fetched.ok()).as("Abruf: %s", fetched.body()).isTrue();

        assertThat(TestEnv.field(fetched.json(), "name")).contains(name);

        var nodes = fetched.json().path("nodes");
        assertThat(nodes).as("quad speichert alle vier Knoten").hasSize(4);
        assertThat(TestEnv.field(nodes.get(0), "agentType"))
                .as("der Agent-Typ bleibt am Knoten stehen")
                .contains("planner");

        assertThat(TestEnv.delete(QUAD + "/api/ui/workflows/" + id).ok())
                .as("Aufraeumen: %s", id).isTrue();
    }

    @Test
    @Disabled("BEFUND, reproduzierbar: quad nimmt einen Workflow ohne Knoten an, startet ihn "
            + "und meldet den Lauf als 'completed'. Damit signalisiert 'completed' nichts — "
            + "es lief schlicht nichts. Die Validierung muesste beim Anlegen oder beim Start "
            + "greifen.")
    void quadRejectsEmptyWorkflow() {
        var res = TestEnv.postJson(QUAD + "/api/ui/workflows",
                Map.of("name", "leer-" + UUID.randomUUID(), "nodes", List.of(), "edges", List.of()));

        assertThat(res.ok())
                .as("quad darf eine leere Definition nicht annehmen: %s", res.body())
                .isFalse();
        assertThat(res.statusCode()).isBetween(400, 499);
    }

    @Test
    @DisplayName("IST-BEFUND: ein Workflow ohne Knoten laeuft auf quad durch und gilt als erledigt")
    void emptyWorkflowRunsAndReportsCompleted() throws Exception {
        // Der Begleittest zum @Disabled-Befund. Er haelt fest, was quad heute tut, damit der
        // Befund nicht nur notiert, sondern messbar ist.
        var name = "leer-" + UUID.randomUUID();
        var created = TestEnv.postJson(QUAD + "/api/ui/workflows",
                Map.of("name", name, "nodes", List.of(), "edges", List.of()));
        assertThat(created.ok()).as("quad nimmt die leere Definition an: %s", created.body()).isTrue();

        var id = TestEnv.field(created.json(), "id").orElseThrow();
        var run = TestEnv.postJson(QUAD + "/api/ui/workflows/" + id + "/execute",
                Map.of("initialData", "{}"));
        assertThat(run.ok()).as("und startet sie: %s", run.body()).isTrue();

        var runId = TestEnv.field(run.json(), "runId").orElseThrow();

        // Der Lauf ist direkt nach dem Start noch 'running'; der Endzustand kommt asynchron.
        var status = awaitRunStatus(runId);

        assertThat(status)
                .as("BEFUND: Lauf %s der leeren Definition '%s' gilt als '%s'. Erwartet waere "
                        + "zumindest 'failed' oder eine Ablehnung beim Anlegen.", runId, name, status)
                .isEqualTo("completed");

        TestEnv.delete(QUAD + "/api/ui/workflows/" + id);
    }

    /**
     * Wartet auf den Endzustand eines quad-Laufs. Liefert den zuletzt gesehenen Zustand,
     * wenn der Deckel ablaeuft (Awaitility statt festem {@code Thread.sleep}).
     */
    private static String awaitRunStatus(String runId) {
        var last = new AtomicReference<>("running");
        try {
            return Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(400))
                    .until(() -> {
                        String s = TestEnv.get(QUAD + "/api/ui/runs/" + runId)
                                .json().path("status").asText();
                        last.set(s);
                        return ("running".equals(s) || "queued".equals(s)) ? null : s;
                    }, java.util.Objects::nonNull);
        } catch (ConditionTimeoutException e) {
            return last.get();
        }
    }

    // ── die Bridge ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("die Bridge übernimmt den quad-Workflow und startet ihn auf kafka-platform")
    void bridgeConvertsAndStartsOnKafka() throws Exception {
        var name = "bridge-" + UUID.randomUUID();

        // 1. auf quad definieren und über den Export wieder auslesen — so fließt exakt das
        //    Format, das die Bridge in der Praxis bekommt.
        var quadId = TestEnv.field(
                        TestEnv.postJson(QUAD + "/api/ui/workflows", quadWorkflow(name)).json(),
                        "id")
                .orElseThrow();
        var exported = TestEnv.get(QUAD + "/api/ui/workflows/export/" + quadId).json();
        var flatWorkflow = exported.path("workflows").get(0);

        // 2. an die Bridge geben. Der Default-Backend ist kafka: sie schreibt direkt in die
        //    gemeinsame Definitionstabelle und startet, ohne einen HTTP-Roundtrip zu quad.
        var bridged = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/unified/workflows/from-quad", flatWorkflow);
        assertThat(bridged.ok()).as("Bridge-Antwort: %s", bridged.body()).isTrue();

        var body = bridged.json();
        assertThat(body.path("converted").asBoolean())
                .as("die Bridge hat konvertiert: %s", bridged.body()).isTrue();
        assertThat(TestEnv.field(body, "name")).contains(name);
        assertThat(body.path("nodeCount").asInt())
                .as("alle vier quad-Knoten sind angekommen")
                .isEqualTo(4);

        // 3. und er läuft auf kafka-platform durch
        var executionId = TestEnv.field(body, "executionId").orElseThrow(
                () -> new AssertionError("die Bridge startet den Workflow, liefert aber keine "
                        + "executionId: " + bridged.body()));
        var run = WorkflowRun.forId(executionId, name);

        var status = run.awaitStatus(COMPLETION, "COMPLETED", "FAILED");
        assertThat(status)
                .as("der übernommene Workflow muss auf kafka-platform durchlaufen. "
                        + "Status: %s, Lauf: %s", status, executionId)
                .isEqualTo("COMPLETED");

        // 4. und die Schritte tragen die Agent-Typen, die auf quad standen
        var definitions = TestEnv.get("/api/v1/workflow-definitions").json();
        var stored = new java.util.ArrayList<com.fasterxml.jackson.databind.JsonNode>();
        for (var d : definitions) {
            if (name.equals(d.path("name").asText())) stored.add(d);
        }
        assertThat(stored).as("die Definition liegt jetzt in kafka-platform").isNotEmpty();

        var agentTypes = TestEnv.get("/api/v1/workflows/" + executionId).json()
                .path("checkpoints");
        var seen = new java.util.ArrayList<String>();
        for (var c : agentTypes) {
            var at = c.path("agentType").asText(null);
            if (at != null && !at.isBlank()) seen.add(at);
        }
        assertThat(seen)
                .as("die Agent-Typen aus quad (%s) sind im kafka-Lauf erhalten", seen)
                .containsExactlyInAnyOrder("planner", "coder", "linter");

        TestEnv.delete(QUAD + "/api/ui/workflows/" + quadId);
    }

    @Test
    @DisplayName("dieselbe quad-Definition wird nur einmal übernommen")
    void bridgeIsIdempotentForTheSameDefinition() {
        var name = "bridge-idem-" + UUID.randomUUID();
        var flat = quadWorkflow(name);

        var first = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/unified/workflows/from-quad", flat);
        var second = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/unified/workflows/from-quad", flat);

        assertThat(first.ok()).as("erster Aufruf: %s", first.body()).isTrue();
        assertThat(second.ok()).as("zweiter Aufruf: %s", second.body()).isTrue();

        assertThat(TestEnv.field(second.json(), "kafkaId"))
                .as("beide Aufrufe treffen dieselbe kafka-Definition: %s vs %s",
                        first.json(), second.json())
                .hasValue(TestEnv.field(first.json(), "kafkaId").orElseThrow());
        assertThat(second.json().path("version").asInt())
                .as("die zweite Konvertierung erzeugt keine neue Version")
                .isEqualTo(first.json().path("version").asInt());
    }

    @Test
    @DisplayName("die Bridge lehnt einen quad-Knotentyp ab, den kafka-platform nicht kennt")
    void bridgeRejectsUnsupportedNodeType() {
        var payload = Map.of(
                "name", "unsupported-" + UUID.randomUUID(),
                "nodes", List.of(
                        Map.of("id", "a", "type", "agent", "agentType", "planner"),
                        Map.of("id", "b", "type", "nested-workflow", "agentType", "coder")),
                "edges", List.of(Map.of("source", "a", "target", "b")));

        var res = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/unified/workflows/from-quad", payload);

        assertThat(res.ok())
                .as("nested-workflow kann der Kafka-Orchestrator nicht ausfuehren, das muss "
                        + "sichtbar sein und nicht zu einem halben Workflow fuehren. Antwort: %s",
                        res.body())
                .isFalse();
        assertThat(res.statusCode()).isBetween(400, 499);
    }

    @Test
    @DisplayName("der Backend-Header entscheidet, wohin die Bridge geht")
    void backendHeaderRoutesToQuarkus() {
        var name = "bridge-proxy-" + UUID.randomUUID();

        // Ohne Header: die Bridge schreibt in die kafka-Tabelle und startet den Lauf dort.
        var viaKafka = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/unified/workflows/from-quad", quadWorkflow(name));
        assertThat(viaKafka.statusCode())
                .as("ohne Header geht es nach kafka: %s", viaKafka.body()).isEqualTo(202);
        assertThat(TestEnv.field(viaKafka.json(), "executionId"))
                .as("der kafka-Pfad startet den Lauf sofort")
                .isPresent();

        // Mit X-Backend: quarkus wird an quad weitergereicht und dort angelegt. Es entsteht
        // KEIN kafka-Lauf — das ist der ganze Unterschied zwischen den beiden Wegen.
        var viaQuarkus = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/unified/workflows/from-quad",
                quadWorkflow(name),
                Map.of("X-Backend", "quarkus"));

        assertThat(viaQuarkus.json().path("proxied").asBoolean())
                .as("die Antwort kommt von quad, nicht von der Bridge: %s", viaQuarkus.body())
                .isTrue();
        assertThat(viaQuarkus.json().path("status").asInt())
                .as("quad hat angelegt: 201")
                .isEqualTo(201);
        assertThat(TestEnv.field(viaQuarkus.json().path("data"), "id"))
                .as("quad vergibt seine eigene id: %s", viaQuarkus.body())
                .isPresent();
        assertThat(viaQuarkus.json().has("executionId"))
                .as("der Proxy-Pfad startet keinen kafka-Lauf: %s", viaQuarkus.body())
                .isFalse();

        // Und die Definition ist wirklich auf quad gelandet.
        var quadId = TestEnv.field(viaQuarkus.json().path("data"), "id").orElseThrow();
        var onQuad = TestEnv.get(QUAD + "/api/ui/workflows/" + quadId);
        assertThat(TestEnv.field(onQuad.json(), "name"))
                .as("die Definition steht auf quad")
                .contains(name);

        // Aufraeumen: der kafka-Lauf vom ersten Aufruf und die quad-Definition.
        if (TestEnv.field(viaKafka.json(), "executionId").isPresent()) {
            TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/"
                    + TestEnv.field(viaKafka.json(), "executionId").orElseThrow() + "/cancel",
                    Map.of());
        }
        TestEnv.delete(QUAD + "/api/ui/workflows/" + quadId);
    }
}