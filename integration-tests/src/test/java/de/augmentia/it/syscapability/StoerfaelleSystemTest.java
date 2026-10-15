package de.augmentia.it.syscapability;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.it.support.Scenario;
import de.augmentia.it.support.TestEnv;
import de.augmentia.it.support.WorkflowRun;
import de.augmentia.it.support.Stack;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Was passiert, wenn es schiefgeht.
 *
 * <p>Der Mock-Worker antwortet auf jeden Prompt mit {@code status: COMPLETED} und
 * bricht nie ab. Ein Störfalltest, der sich darauf verlässt, prüft nichts. Die Fehler hier
 * werden deshalb von außen erzeugt:
 *
 * <ul>
 *   <li><b>Abbruch</b> — der Bediener bricht einen laufenden Vorgang ab.</li>
 *   <li><b>Kaputte Nachricht</b> — etwas schreibt Müll auf ein Task-Topic.</li>
 *   <li><b>Ergebnis ohne Auftrag</b> — ein Resultat zu einem Lauf, den es nicht gibt.</li>
 *   <li><b>Zeitüberschreitung</b> — ein Schritt bekommt so wenig Zeit, dass er sie
 *       zwangsläufig überschreitet, und der Reaper muss einspringen.</li>
 * </ul>
 *
 * <p>Die Prüfung ist jedes Mal dieselbe Frage: <em>bleibt das System lauffähig?</em> Ein
 * kaputter Durchlauf ist erlaubt und sogar erwartet — ein System, das nach einem Fehler
 * stehen bleibt, nicht.
 */
@Tag("requires-e2e")
@Tag("requires-orchestrator")
@Tag("requires-kafka")
class StoerfaelleSystemTest {

    private static final Duration ENDZUSTAND = Duration.ofSeconds(90);

    @BeforeAll
    static void stackMussLaufen() {
        Stack.assumeUp();
    }

    private static Scenario rechnungsWorkflow(String name) {
        return Scenario.scenario(name, "Finanzwesen",
                        "Prüft eine Rechnung und bucht sie — der Standardablauf, "
                        + "an dem die Stoerfaelle geprueft wird.")
                .then("pruefung", "reviewer")
                .then("buchung", "quality")
                .end()
                .install();
    }

    // ── Der Bediener bricht ab ──────────────────────────────────────────────

    @Test
    @DisplayName("ein abgebrochener Vorgang endet abgebrochen, nicht erfolgreich")
    void abbruchBrichtDenLaufAb() throws Exception {
        var szenario = rechnungsWorkflow("Rechnungsabbruch");
        var run = szenario.start(Map.of("rechnung", "RE-2026-0901"));

        assertThat(run.cancel().ok()).as("Abbruch: %s", run).isTrue();

        var status = run.awaitStatus(ENDZUSTAND, "CANCELLED", "COMPLETED", "FAILED");

        assertThat(status)
                .as("ein abgebrochener Lauf darf nicht als erfolgreich dastehen. %s", szenario)
                .isIn("CANCELLED", "FAILED");
    }

    @Test
    @DisplayName("nach einem Abbruch läuft der nächste Lauf normal durch")
    void nachAbbruchLaeuftDerNaechste() throws Exception {
        rechnungsWorkflow("Rechnung-nach-Abbruch").start(Map.of("rechnung", "RE-2026-0902")).cancel();

        var danach = rechnungsWorkflow("Rechnung-nach-Abbruch-weiter")
                .start(Map.of("rechnung", "RE-2026-0903"));

        var status = danach.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status)
                .as("ein Abbruch darf keinen der folgenden Läufe mitnehmen")
                .isEqualTo("COMPLETED");
    }

    // ── Müll auf einem Task-Topic ───────────────────────────────────────────

    @Test
    @DisplayName("eine unlesbare Nachricht auf einem Task-Topic wird nicht zum Absturz")
    void kaputteNachrichtBrichtDenWorkerNicht() throws Exception {
        // Der Worker liest dauerhaft agent.tasks.*. Unlesbares JSON darf ihn nicht
        // beenden — sonst stirbt der ganze Konsumierpfad an einer einzigen kaputten Nachricht.
        TestEnv.publishAsync(TopicNames.tasks("reviewer"), "kaputt-" + UUID.randomUUID(),
                "{\"header\": {\"workflowId\": ");

        // Der Orchestrator muss weiter auf gesunde Läufe reagieren.
        var danach = rechnungsWorkflow("Rechnung-nach-Kaputt")
                .start(Map.of("rechnung", "RE-2026-0904"));
        var status = danach.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status)
                .as("eine kaputte Nachricht darf keinen gesunden Lauf blockieren")
                .isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("ein Task ohne Header wird verworfen")
    void taskOhneHeaderWirdVerworfen() throws Exception {
        TestEnv.publishAsync(TopicNames.tasks("linter"), "ohne-header-" + UUID.randomUUID(),
                TestEnv.toJson(Map.of("agentType", "linter", "stepId", "x")));

        var danach = rechnungsWorkflow("Rechnung-nach-Headerlos")
                .start(Map.of("rechnung", "RE-2026-0905"));
        var status = danach.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status)
                .as("ohne Header lässt sich kein Lauf zuordnen — die Nachricht gehört verworfen")
                .isEqualTo("COMPLETED");
    }

    // ── Ergebnis zu einem unbekannten Lauf ──────────────────────────────────

    @Test
    @DisplayName("ein Ergebnis zu einem unbekannten Lauf wird ignoriert")
    void ergebnisZuUnbekanntemLauf() throws Exception {
        var falscheId = UUID.randomUUID().toString();
        TestEnv.publishAsync(TopicNames.RESULTS, falscheId, TestEnv.toJson(Map.of(
                "header", Map.of("workflowId", falscheId, "taskId", UUID.randomUUID().toString(),
                        "attempt", 1, "timestamp", System.currentTimeMillis()),
                "executionId", falscheId,
                "stepId", "nicht-vorhanden",
                "status", "COMPLETED",
                "output", Map.of("irgendwas", "egal"))));

        var danach = rechnungsWorkflow("Rechnung-nach-Geisterlauf")
                .start(Map.of("rechnung", "RE-2026-0906"));
        var status = danach.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status)
                .as("ein Ergebnis ohne passenden Lauf darf keinen Lauf erzeugen oder zerstören")
                .isEqualTo("COMPLETED");
    }

    // ── Zeitüberschreitung ──────────────────────────────────────────────────

    @Test
    @DisplayName("ein Zeitfenster unter einer Sekunde wird abgelehnt")
    void zuKurzesZeitfensterWirdAbgelehnt() {
        // Der Validator laesst kein timeoutMs < 1000 zu (R8). Das ist der Schutz gegen
        // einen Lauf, der zwangslaeufig in einen Timeout laeuft, den kein Worker in der
        // verbleibenden Zeit beantworten kann. Ein Ablauf, der gar nicht erst startet,
        // ist besser als einer, der haengt.
        var szenario = Scenario.scenario("Verzugsmeldung-zu-kurz", "Logistik",
                        "Prueft eine Sendung mit 300 ms Zeitfenster — weniger, als der "
                        + "Validator zulässt.")
                .then("schnell", "planner", Map.of("timeoutMs", 300))
                .end();

        var abgelehnt = szenario.installExpectingRejection();

        assertThat(abgelehnt.statusCode())
                .as("zu kurze Zeitfenster werden abgelehnt, nicht angenommen: %s", abgelehnt.body())
                .isBetween(400, 499);
        assertThat(abgelehnt.body())
                .as("die Ablehnung nennt die Regel — %s", abgelehnt.body())
                .contains("R8");
    }

    @Test
    @DisplayName("mit dem kleinsten zulaessigen Zeitfenster kommt der Lauf zu einem Ende")
    void kleinstesZeitfensterEndetTrotzdem() throws Exception {
        var szenario = Scenario.scenario("Verzugsmeldung", "Logistik",
                        "Prueft eine Sendung mit dem kleinsten zulaessigen Zeitfenster "
                        + "von einer Sekunde.")
                .then("schnell", "planner", Map.of("timeoutMs", 1000))
                .then("nachkontrolle", "quality")
                .end()
                .install();

        var run = szenario.start(Map.of("sendung", "P-4711"));

        var status = run.awaitStatus(ENDZUSTAND, "COMPLETED", "FAILED", "PAUSED_HITL", "CANCELLED");

        assertThat(status)
                .as("der Lauf muss zu einem Ende kommen, gleich in welchem: %s. %s",
                        status, szenario)
                .isIn("COMPLETED", "FAILED", "PAUSED_HITL", "CANCELLED");
    }

    // ── Und danach ist das System wieder normal benutzbar ───────────────────

    @Test
    @DisplayName("nach allen Stoerungen arbeitet das System weiter")
    void nachAllenStoerungenWeiter() throws Exception {
        // Fasst vorherige Tests zusammen, ist aber selbst ein Test: wenn einer der
        // Stoerfalltests etwas zueueckgelasse n hat, faellt es hier auf.
        var laeufe = List.of("RE-A", "RE-B", "RE-C", "RE-D", "RE-E");

        for (var rechnung : laeufe) {
            var run = rechnungsWorkflow("Rechnung-Batch")
                    .start(Map.of("rechnung", rechnung));
            assertThat(run.awaitStatus(ENDZUSTAND, "COMPLETED"))
                    .as("nach allen Stoerungen laeuft %s durch. %s", rechnung, run)
                    .isEqualTo("COMPLETED");
        }
    }
}
