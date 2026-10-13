package de.augmentia.it.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import de.augmentia.agentplatform.common.kafka.TopicNames;

/**
 * Ein Workflow, so beschrieben wie man ihn einem Menschen erklären würde.
 *
 * <p>Die bisherigen Tests bauen ihre Knoten inline als {@code Map.of(...)}. Das ist an
 * zwei Stellen unbequem: es wiederholt die halbe Struktur bei jedem Test, und es ist nicht
 * lesbar. Ein Szenario aus sechs Schritten sieht in einer {@code Map.of}-Kette nicht mehr
 * wie ein Ablauf aus, sondern wie Datenmüll. Hier steht deshalb der Ablauf im Text:
 *
 * <pre>
 *   scenario("Rechnungsabgleich", "Finanzwesen",
 *            "Extrahiert Rechnungen, prüft sie gegen drei Prüfer und gibt frei, "
 *            + "wenn mindestens zwei zustimmen.")
 *       .then(step("extract", "researcher"))
 *       .then(fork("verify", "a", "b.a.review", "b", "b.b.review", "c", "b.c.review"))
 *       .then(join("quorum", "N_OF", 2, "a", "b", "c"))
 *       .then(step("post", "quality"))
 *       .then(end())
 *       .install();
 * </pre>
 *
 * <p>Die Beschreibungen sind kein Beiwerk. Sie sagen, was der Lauf im Alltag tun soll, und
 * sie stehen deshalb im Bericht eines Scheiterns bei.
 *
 * <p><b>Was die Szenarien nicht leisten können.</b> Der Worker läuft ohne LLM-Schlüssel
 * im Mock-Modus und antwortet auf jeden Prompt mit
 * {@code {"confidence": 0.95, "status": "COMPLETED"}}
 * ({@code ChatModelRegistry.MockChatModel}). Daraus folgt:
 * <ul>
 *   <li>Bedingungen auf {@code confidence} nehmen immer den Zweig für hohe Konfidenz.</li>
 *   <li>Es entsteht von selbst kein Timeout und kein Retry — der Worker läuft nie in einen
 *       Fehler. Zeitverhalten muss deshalb über die Zeitsteuerung der Definition erzwungen
 *       werden, nicht über ein langsames Modell.</li>
 * </ul>
 */
public final class Scenario {

    private final String name;
    private final String domain;
    private final String description;
    private final List<Map<String, Object>> nodes = new ArrayList<>();
    private final Map<String, Object> context = new LinkedHashMap<>();

    /** Zuletzt angehaengter Knoten — der Vorgänger für den nächsten normalen Schritt. */
    private String lastNodeId;
    /** Der letzte FORK — der Vorgänger für Zweigsschritte. */
    private String lastFork;
    /** Zweigsschritte seit diesem FORK — die Vorgänger für einen JOIN. */
    private final List<String> branchStepsSinceFork = new ArrayList<>();
    /** Typ des Knotens, der als Nächstes angelegt wird; entscheidet die Abhängigkeit. */
    private String nextNodeType = "STEP";

    private Scenario(String name, String domain, String description) {
        this.name = name;
        this.domain = domain;
        this.description = description;
    }

    public static Scenario scenario(String name, String domain, String description) {
        return new Scenario("it-" + slug(domain) + "-" + slug(name), domain, description);
    }

    public String name() { return name; }
    public String domain() { return domain; }
    public String description() { return description; }
    public List<Map<String, Object>> nodes() { return List.copyOf(nodes); }

    public Scenario context(String key, Object value) {
        context.put(key, value);
        return this;
    }

    // ── Knoten ───────────────────────────────────────────────────────────────

    /**
     * Ein Arbeitsschritt: ein Agent bekommt einen Auftrag.
     *
     * <p>{@code null} als Agent laesst das Feld weg. Das erlaubt die negative Probe, ob
     * die Plattform einen Arbeitsschritt ohne Agent tatsaechlich ablehnt.
     */
    public Scenario then(String stepId, String agentType) {
        Scenario s = node(stepId, "STEP");
        return agentType == null ? s : s.with("agentType", agentType);
    }

    public Scenario then(String stepId, String agentType, boolean requiresHumanApproval) {
        return then(stepId, agentType).with("requiresHumanApproval", requiresHumanApproval);
    }

    /** Ein reiner Arbeitsschritt mit weiteren Feldern. */
    public Scenario then(String stepId, String agentType, Map<String, Object> extra) {
        Scenario s = then(stepId, agentType);
        extra.forEach(s::with);
        return s;
    }

    private Scenario node(String stepId, String nodeType) {
        nextNodeType = nodeType;
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("stepId", stepId);
        node.put("nodeType", nodeType);
        node.put("dependsOn", dependsOnFor(stepId));
        nodes.add(node);

        if (stepId.startsWith("b.")) {
            branchStepsSinceFork.add(stepId);
        } else if ("FORK".equals(nodeType)) {
            lastFork = stepId;
            branchStepsSinceFork.clear();
        } else if ("JOIN".equals(nodeType)) {
            branchStepsSinceFork.clear();
        }
        lastNodeId = stepId;
        return this;
    }

    /** Der Endknoten. Genau einer pro Definition (Validator-Regel R6). */
    public Scenario end() {
        return node("__end", "END");
    }

    public Scenario end(String stepId) {
        return node(stepId, "END");
    }

    /**
     * Eine Verzweigung: {@code DECISION} mit einer Bedingung pro Zweig.
     *
     * @param stepId    Knotenname
     * @param branches  abwechselnd {@code branchId, condition}; ein Zweig mit
     *                  {@code null} als Bedingung ist der Default-Zweig (Regel R3
     *                  verlangt, dass genau ein Zweig in einen LOOP fuehrt — das
     *                  prueft der Validator beim Anlegen, nicht dieser Test).
     */
    public Scenario decision(String stepId, List<Branch> branches) {
        return decision(stepId, branches, null);
    }

    /**
     * @param defaultTarget Ziel, das genommen wird, wenn keine Bedingung zutrifft. Pflicht,
     *                      sobald ein Zweig in einen LOOP fuehrt (Regel R3) — dann ist das
     *                      der Ausgang der Schleife.
     */
    public Scenario decision(String stepId, List<Branch> branches, String defaultTarget) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (Branch branch : branches) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("branchId", branch.branchId());
            map.put("condition", branch.condition());
            map.put("target", branch.target());
            list.add(map);
        }
        Scenario s = node(stepId, "DECISION").with("branches", list);
        return defaultTarget == null ? s : s.with("defaultTarget", defaultTarget);
    }

    /**
     * Ein Zweig einer Entscheidung.
     *
     * <p><b>Jeder Zweig braucht eine Bedingung.</b> Es gibt keinen bedingungslosen Zweig —
     * der Validator meldet {@code "branch ... lacks a condition"}. Was ohne passende
     * Bedingung passieren soll, ist {@code defaultTarget} der Entscheidung, und das ist
     * ein eigenes Feld.
     */
    public record Branch(String branchId, String condition, String target) {
        public static Branch when(String branchId, String condition, String target) {
            if (condition == null || condition.isBlank()) {
                throw new IllegalArgumentException(
                        "Zweig %s braucht eine Bedingung — fuer den Fall ohne Treffer "
                                .formatted(branchId) + "gibt es defaultTarget an der Entscheidung");
            }
            return new Branch(branchId, condition, target);
        }
    }

    /**
     * Haengt den gerade angelegten Schritt an eine Entscheidung statt an den Vorgänger.
     *
     * <p>Nötig, weil die beiden Zweigziele Geschwister sind: sie hängen beide an der
     * Entscheidung, nicht eines am anderen. Ohne das liefe der „sonst"-Zweig erst, wenn
     * der erste fertig wäre — die Verzweigung wäre eine Sequenz.
     */
    public Scenario afterDecision(String decisionStepId) {
        nodes.get(nodes.size() - 1).put("dependsOn", List.of(decisionStepId));
        return this;
    }

    /**
     * Parallele Zweige. Die Zweigschritte werden mit {@code branchStep()} angelegt,
     * weil Regel R5 das Namensschema {@code b.<branchId>.<name>} verlangt.
     */
    public Scenario fork(String stepId, Object... branchIds) {
        List<Map<String, Object>> branches = new ArrayList<>();
        for (Object id : branchIds) {
            Map<String, Object> branch = new LinkedHashMap<>();
            branch.put("branchId", String.valueOf(id));
            branch.put("firstStep", "b." + id + ".run");
            branches.add(branch);
        }
        return node(stepId, "FORK").with("branches", branches);
    }

    /** Ein Schritt innerhalb eines Zweigs. */
    public Scenario branchStep(String branchId, String name, String agentType) {
        return then("b." + branchId + "." + name, agentType);
    }

    /**
     * Zusammenführung paralleler Zweige.
     *
     * @param joinType {@code ALL} oder {@code N_OF}
     * @param joinN    bei {@code N_OF}: wie viele Zweige reichen aus
     */
    /**
     * Zusammenfuehrung paralleler Zweige, mit allen Werten ausdruecklich.
     *
     * @param joinType {@code AND} (alle muessen ankommen), {@code XOR} (genau einer)
     *                 oder {@code N_OF} (siehe {@code joinN}). {@code ALL} sieht
     *                 verstaendlich aus und wird trotzdem abgelehnt — der Validator kennt
     *                 nur diese drei (Regel R4).
     * @param joinN    nur fuer {@code N_OF}: wie viele Zweige reichen aus. Bei allen
     *                 anderen Arten darf das Feld nicht gesetzt sein. {@code null} laesst
     *                 es weg, was das Feld nicht gesetzt bedeutet.
     */
    public Scenario join(String stepId, String joinType, Integer joinN, List<String> branchIds) {
        Scenario s = node(stepId, "JOIN")
                .with("joinType", joinType)
                .with("joinBranches", branchIds);
        return joinN == null ? s : s.with("joinN", joinN);
    }

    /** N-von-M: so viele Zweige reichen aus. */
    public Scenario joinNOf(String stepId, int joinN, Object... branchIds) {
        return join(stepId, "N_OF", joinN, branchNames(branchIds));
    }

    /** Genau einer der Zweige gewinnt. */
    public Scenario joinXor(String stepId, Object... branchIds) {
        return join(stepId, "XOR", null, branchNames(branchIds));
    }

    /** Alle Zweige muessen ankommen. */
    public Scenario joinAll(String stepId, Object... branchIds) {
        return join(stepId, "AND", null, branchNames(branchIds));
    }

    private static List<String> branchNames(Object... branchIds) {
        List<String> names = new ArrayList<>();
        for (Object id : branchIds) {
            names.add(String.valueOf(id));
        }
        return names;
    }

    /**
     * Eine Schleife ueber einen Koerper (Region).
     *
     * @param bodySteps    Schritte, die je Iteration wiederholt werden
     * @param maxIterations  Deckel, damit ein Fehler nicht endlos laeuft
     * @param exitCondition Ausdruck, bei dem die Schleife endet; {@code null} laeuft
     *                     bis zum Deckel
     */
    /**
     * Eine kooperative Schleife: ein Agent, der {@code maxIterations}-mal laeuft, bis die
     * Abbruchbedingung zutrifft.
     *
     * <p>Das Feld heisst {@code bodyAgentType}, nicht {@code agentType} — ein LOOP ist im
     * Validator (R8) ein Knoten mit zwingender {@code exitCondition}. Ohne sie wird die
     * Definition abgelehnt.
     */
    public Scenario loop(String stepId, String bodyAgentType, int maxIterations, String exitCondition) {
        if (exitCondition == null || exitCondition.isBlank()) {
            throw new IllegalArgumentException(
                    "LOOP %s braucht eine exitCondition (R8)".formatted(stepId));
        }
        return node(stepId, "LOOP")
                .with("bodyAgentType", bodyAgentType)
                .with("maxIterations", maxIterations)
                .with("exitCondition", exitCondition);
    }

    /**
     * Eine Schleife ueber einen Koerper aus benannten Schritten.
     *
     * <p>{@code bodySteps} sind die <em>Namen</em> der Schritte im Koerper, keine
     * vollstaendigen Knoten — {@code WorkflowDefinitionUpload.DagNodeUpload.bodySteps}
     * ist eine {@code List<String>}.
     */
    public Scenario loop(String stepId, String agentType, List<String> bodySteps,
                         int maxIterations, String exitCondition) {
        return node(stepId, "LOOP")
                .with("bodyAgentType", agentType)
                .with("bodySteps", bodySteps)
                .with("maxIterations", maxIterations)
                .with("exitCondition", exitCondition);
    }

    private Scenario with(String key, Object value) {
        nodes.get(nodes.size() - 1).put(key, value);
        return this;
    }

    /**
     * Baut {@code dependsOn} aus dem bisherigen Verlauf.
     *
     * <p>Drei Faelle, und jeder braucht einen anderen Vorgaenger:
     * <ul>
     *   <li>Ein normaler Schritt haengt am letzten Knoten — das kann ein Schritt, eine
     *       Entscheidung, eine Schleife, ein Fork oder ein Join sein.</li>
     *   <li>Ein Zweigsschritt haengt am letzten FORK, nicht am vorherigen Zweigsschritt:
     *       die Zweige eines Forks laufen nebeneinander, nicht nacheinander.</li>
     *   <li>Ein JOIN haengt an allen Zweigsschritten seit diesem FORK. Haengt er am
     *       letzten Zweigsschritt, wuerde der Graph serialisiert und die Parallelitaet
     *       waere weg — und Regel R6 (jeder Knoten erreichbar, END erreichbar) waere
     *       bei zwei Zuegen nicht mehr erfuellbar.</li>
     * </ul>
     */
    private List<String> dependsOnFor(String stepId) {
        String nodeType = nextNodeType;
        if ("JOIN".equals(nodeType)) {
            return branchStepsSinceFork.isEmpty() ? lastNode() : List.copyOf(branchStepsSinceFork);
        }
        if (stepId.startsWith("b.")) {
            return lastFork == null ? lastNode() : List.of(lastFork);
        }
        return lastNode();
    }

    private List<String> lastNode() {
        return lastNodeId == null ? List.of() : List.of(lastNodeId);
    }

    // ── Anlegen und starten ──────────────────────────────────────────────────

    /**
     * Legt die Definition beim Orchestrator an.
     *
     * @throws AssertionError wenn der Orchestrator sie ablehnt — mit der vollständigen
     *         Antwort, weil dort die Validator-Regel steht, die nicht passt
     */
    public Scenario install() {
        var body = Map.of(
                "name", name,
                "version", 1,
                "dag_structure", Map.of("nodes", nodes));

        var res = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflow-definitions", body);
        if (res.ok() || res.statusCode() == 409) {
            // 409 heisst: die Definition existiert bereits. Das ist der Normalfall beim
            // zweiten Lauf gegen dieselbe Datenbank — Szenarien sollen wiederholbar sein,
            // ohne dass jedes Mal ein neuer Name erfunden wird. Der Name steht im Bericht,
            // ein zufaengiger nicht.
            return this;
        }
        throw new AssertionError("""
                Der Orchestrator hat das Szenario nicht angenommen (HTTP %d).

                  %s — %s (%s)

                  Antwort: %s
                """.formatted(res.statusCode(), name, domain, description, res.body()));
    }

    /** Wie {@link #install()}, aber mit Pruefung der Ablehnung als Erwartung. */
    public TestEnv.Response installExpectingRejection() {
        var body = Map.of("name", name, "version", 1, "dag_structure", Map.of("nodes", nodes));
        return TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflow-definitions", body);
    }

    public WorkflowRun start() {
        return start(Map.of());
    }

    public WorkflowRun start(Map<String, Object> input) {
        return WorkflowRun.start(name, input, null);
    }

    /** Das Task-Topic, auf dem dieser Lauf als Erstes landet. */
    public String firstAgentType() {
        for (var node : nodes) {
            Object agentType = node.get("agentType");
            if (agentType != null) {
                return String.valueOf(agentType);
            }
        }
        throw new AssertionError("Szenario " + name + " hat keinen STEP mit agentType");
    }

    public String taskTopic() {
        return TopicNames.tasks(firstAgentType());
    }

    // ── Bericht ──────────────────────────────────────────────────────────────

    @Override
    public String toString() {
        return """
               Szenario %s (%s)
                 %s
                 Knoten: %s"""
                .formatted(name, domain, description,
                        nodes.stream()
                                .map(n -> n.get("nodeType") + ":" + n.get("stepId"))
                                .toList());
    }

    private static String slug(String raw) {
        String s = raw.toLowerCase()
                .replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("(^-|-$)", "");
        return s.isEmpty() ? "x" : s;
    }

    /** Bequem für {@code assertThat(scenario.toString())} im Fehlerbericht. */
    public Optional<String> firstStepId() {
        return nodes.isEmpty() ? Optional.empty()
                : Optional.of(String.valueOf(nodes.get(0).get("stepId")));
    }
}