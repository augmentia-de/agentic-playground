package de.augmentia.it.support;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import de.augmentia.agentplatform.common.kafka.TopicNames;

/**
 * Ein laufender Workflow auf dem kafka-Orchestrator, und die Hilfen, um seinen Fortschritt zu
 * beobachten.
 *
 * <p>Ein Start ist asynchron: Der Orchestrator legt einen Lauf an und publiziert einen
 * Intake-Event; die eigentliche Arbeit passiert in Kafka. Der Test wartet deshalb nicht auf eine
 * HTTP-Antwort, sondern auf einen beobachtbaren Zustand.
 */
public final class WorkflowRun {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(45);

    private final String executionId;
    private final String workflowName;
    private final boolean duplicate;

    private WorkflowRun(String executionId, String workflowName, boolean duplicate) {
        this.executionId = executionId;
        this.workflowName = workflowName;
        this.duplicate = duplicate;
    }

    public String executionId() { return executionId; }
    public String workflowName() { return workflowName; }
    public boolean duplicate() { return duplicate; }

    /**
     * Ein Lauf, den jemand anderes gestartet hat — die Bridge startet Workflowe selbst und
     * liefert nur die executionId zurück.
     */
    public static WorkflowRun forId(String executionId, String workflowName) {
        return new WorkflowRun(executionId, workflowName, false);
    }

    /** Task-Topic eines Agent-Typs, wie der Orchestrator ihn benennt. */
    public static String taskTopic(String agentType) {
        return TopicNames.tasks(agentType);
    }

    public static WorkflowRun start(String workflowName, Map<String, Object> inputParams, String idempotencyKey) {
        var request = Map.of(
                "workflowName", workflowName,
                "inputParams", inputParams == null ? Map.of() : inputParams,
                "idempotencyKey", idempotencyKey == null ? TestEnv.uniqueKey("auto") : idempotencyKey);

        var res = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start", request);
        if (!res.ok()) {
            throw new IllegalStateException("Start fehlgeschlagen (" + res.statusCode() + "): " + res.body());
        }
        var node = res.json();
        var id = TestEnv.field(node, "workflowExecutionId")
                .orElseThrow(() -> new IllegalStateException("Keine executionId in: " + res.body()));
        boolean dup = node.hasNonNull("duplicate") && node.get("duplicate").asBoolean();
        return new WorkflowRun(id, workflowName, dup);
    }

    public static WorkflowRun start(String workflowName) {
        return start(workflowName, Map.of(), null);
    }

    // ── Beobachtung ─────────────────────────────────────────────────────────

    public TestEnv.Response status() {
        return TestEnv.get(TestEnv.ORCHESTRATOR + "/api/v1/workflows/" + executionId);
    }

    public String statusName() {
        return TestEnv.field(status().json(), "status").orElse("UNKNOWN");
    }

    public TestEnv.Response checkpoints() {
        return TestEnv.get(TestEnv.ORCHESTRATOR + "/api/v1/workflows/" + executionId + "/checkpoints");
    }

    /**
     * Checkpoint-Status je Schritt.
     *
     * <p><b>Wichtig für Zusicherungen.</b> Die Liste enthält <em>jeden</em> Schritt der
     * Definition, auch die nicht gelaufenen: ein Decision-Zweig bekommt für beide Ziele
     * einen Checkpoint, das nicht genommene mit Status {@code SKIPPED}. „Ist im Checkpoint
     * vorhanden" bedeutet also <em>nicht</em> „ist gelaufen". Für Aussagen über den
     * tatsächlichen Ablauf immer den Status prüfen.
     */
    public Map<String, String> checkpointStatus() {
        Map<String, String> status = new java.util.LinkedHashMap<>();
        var res = checkpoints();
        if (!res.ok()) {
            return status;
        }
        for (var cp : res.json()) {
            status.put(cp.path("stepId").asText(),
                    cp.path("status").asText("UNKNOWN"));
        }
        return status;
    }

    /** true, wenn der Schritt abgeschlossen wurde — im Gegensatz zu übersprungen. */
    public boolean ranToCompletion(String stepId) {
        return "COMPLETED".equalsIgnoreCase(checkpointStatus().get(stepId));
    }

    public boolean wasSkipped(String stepId) {
        return "SKIPPED".equalsIgnoreCase(checkpointStatus().get(stepId));
    }

    /**
     * Was die Entscheidung zu diesem Schritt protokolliert hat.
     *
     * <p>Das Format ist nicht einheitlich und wurde nicht erfunden, sondern beobachtet:
     * <ul>
     *   <li>Am <b>Entscheidungsknoten</b> steht der Name des gewählten Ziels, z. B.
     *       {@code "loesung"}.</li>
     *   <li>An einem <b>übersprungenen Zweigziel</b> steht {@code "decision:not-chosen"}.</li>
     * </ul>
     * Für den Seed-Workflow {@code guard-skip} sieht man ein drittes Format:
     * {@code "guard:false"}. Deshalb vergleicht man inhaltlich statt auf Gleichheit.
     */
    public Optional<String> decisionBranchOf(String stepId) {
        var res = checkpoints();
        if (!res.ok()) {
            return Optional.empty();
        }
        for (var cp : res.json()) {
            if (stepId.equals(cp.path("stepId").asText())) {
                var branch = cp.path("decisionBranch");
                return branch.isMissingNode() || branch.isNull()
                        ? Optional.empty()
                        : Optional.of(branch.asText());
            }
        }
        return Optional.empty();
    }

    /** Die Schritte, die abgeschlossen wurden — die tatsächliche Spur des Laufs. */
    public List<String> executedSteps() {
        return checkpointStatus().entrySet().stream()
                .filter(e -> "COMPLETED".equalsIgnoreCase(e.getValue()))
                .map(Map.Entry::getKey)
                .toList();
    }

    public TestEnv.Response cancel() {
        return TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/" + executionId + "/cancel", Map.of());
    }

    /**
     * Wartet, bis der Lauf einen der gewünschten Endzustände erreicht.
     *
     * @param wanted Endzustände, z.B. {@code "COMPLETED"}, {@code "PAUSED_HITL"}, {@code "FAILED"}
     * @return der zuletzt gesehene Zustand
     */
    public String awaitStatus(Duration timeout, String... wanted) throws InterruptedException {
        var deadline = Instant.now().plus(timeout);
        String last = "UNKNOWN";
        while (Instant.now().isBefore(deadline)) {
            var res = status();
            if (res.ok()) {
                last = TestEnv.field(res.json(), "status").orElse("UNKNOWN");
                for (String w : wanted) {
                    if (w.equalsIgnoreCase(last)) return last;
                }
                if ("FAILED".equalsIgnoreCase(last) || "CANCELLED".equalsIgnoreCase(last)) return last;
            }
            Thread.sleep(400);
        }
        return last;
    }

    public String awaitStatus(String... wanted) throws InterruptedException {
        return awaitStatus(DEFAULT_TIMEOUT, wanted);
    }

    /** HITL-Ticket eines Laufs, falls eines offen ist. */
    public Optional<String> openHitlTicketId() {
        var res = TestEnv.get(TestEnv.ORCHESTRATOR + "/api/v1/hitl/tickets?status=OPEN");
        if (!res.ok()) return Optional.empty();
        for (var t : res.json()) {
            var exec = TestEnv.field(t, "workflowExecutionId");
            if (exec.isPresent() && exec.get().equals(executionId)) {
                return TestEnv.field(t, "ticketId");
            }
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "WorkflowRun[" + workflowName() + " " + executionId + "]";
    }

}