package de.augmentia.it.syscapability;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import de.augmentia.it.support.Scenario;
import de.augmentia.it.support.TestEnv;
import de.augmentia.it.support.WorkflowRun;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Zwei Fragen, die man im Betrieb am Ende doch stellt.
 *
 * <ul>
 *   <li><b>Was steht im Log?</b> Ein Lauf nimmt Kundendaten mit — Kartennummern,
 *      -personalia, Gesundheitsdaten. Wenn die im Log stehen, ist das Log eine
 *       Datenschutzverletzung, und sie fällt niemandem auf, weil Logs niemand liest.</li>
 *   <li><b>Wer darf was?</b> Der Orchestrator nimmt Workflow-Startes und Agent-Definitionen
 *       entgegen. Beides kann Geld und Daten bewegen. Wer die Endpunkte aufruft, sollte
 *       nicht beliebig sein.</li>
 * </ul>
 *
 * <p>Der Logteil liest die Datei, die der Runner schreibt. Ohne sie — etwa wenn der
 * Orchestrator anders gestartet wurde — überspringt er, statt eine leere Datei als
 * „sauber" zu werten. Ein Sicherheitstest, der mangels Datei grün wird, wäre schlimmer
 * als keiner.
 */
@Tag("requires-e2e")
@Tag("requires-orchestrator")
@Tag("requires-kafka")
class SicherheitSystemtest {

    /**
     * Der Wert, den die Testläufe als unverfängliche Kennung verwenden. Alles, was mit
     * diesem Praefix im Log auftaucht, ist eine uebermäßige Ausgabe.
     */
    private static final String KENNUNG = "e2e-marker-4711";

    private static final Path ORCHESTRATOR_LOG = Path.of(
            System.getProperty("it.orchestrator.log", "/tmp/opencode/run/sys/orch.log"));

    @BeforeAll
    static void stackMussLaufen() {
        assumeThat(TestEnv.kafkaUp()).as("Broker fehlt unter %s", TestEnv.BOOTSTRAP).isTrue();
        assumeThat(TestEnv.orchestratorUp())
                .as("Orchestrator fehlt unter %s", TestEnv.ORCHESTRATOR).isTrue();
        assumeThat(TestEnv.workerUp())
                .as("kein Worker unter %s", TestEnv.WORKER).isTrue();
    }

    private static Scenario importWorkflow() {
        return Scenario.scenario("Datenzugriff", "Datenschutz",
                        "Liest personenbezogene Daten ein. Dient als Träger fuer die "
                        + "Eingaben, die spaeter im Log gesucht werden.")
                .then("einlesen", "researcher")
                .end()
                .install();
    }

    // ── Was im Log steht ─────────────────────────────────────────────────────

    @Test
    @DisplayName("die Kennung eines Laufs steht nicht im Log")
    void eingabeStehtNichtImLog() throws Exception {
        assumeThat(Files.isReadable(ORCHESTRATOR_LOG))
                .as("ohne Logdatei (%s) lässt sich nichts prüfen — der Test überspringt "
                        + "statt eine leere Datei als sauber zu werten", ORCHESTRATOR_LOG)
                .isTrue();

        var lauf = importWorkflow().start(Map.of("kunde", KENNUNG));

        assertThat(lauf.awaitStatus(Duration.ofSeconds(90), "COMPLETED", "FAILED"))
                .as("%s", lauf).isEqualTo("COMPLETED");

        var log = Files.readString(ORCHESTRATOR_LOG, StandardCharsets.UTF_8);

        // Auch nicht als Teilstring irgendeines anderen Werts.
        assertThat(log)
                .as("die Eingabe des Laufs steht im Orchestrator-Log")
                .doesNotContain(KENNUNG);
    }

    @Test
    @DisplayName("das Log nennt keine Geheimnisse")
    void logNenntKeineGeheimnisse() throws Exception {
        assumeThat(Files.isReadable(ORCHESTRATOR_LOG))
                .as("ohne Logdatei (%s) übersprungen", ORCHESTRATOR_LOG)
                .isTrue();

        importWorkflow().start(Map.of("kunde", KENNUNG)).awaitStatus(Duration.ofSeconds(90), "COMPLETED");

        var log = Files.readString(ORCHESTRATOR_LOG, StandardCharsets.UTF_8);

        for (var verboten : List.of("OPENAI_API_KEY", "VAULT_TOKEN", "Bearer ", "password=")) {
            assertThat(log)
                    .as("im Log steht %s — eine Umgebungsvariable oder ein Zugangsdoken "
                            + "darf dort nicht auftauchen", verboten)
                    .doesNotContain(verboten);
        }
    }

    // ── Wer darf was ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("ein unbekannter Endpunkt wird abgelehnt, nicht beantwortet")
    void unbekannterEndpunkt() {
        var antwort = TestEnv.get("/api/v1/gibt-es-nicht");

        assertThat(antwort.statusCode())
                .as("ein nicht vorhandener Endpunkt: %s", antwort.body())
                .isBetween(400, 499);
    }

    @Test
    @DisplayName("ein kaputter Start-Anfragetext ergibt 400 und keine Fehlermeldung")
    void kaputterStartAnfragetext() {
        var antwort = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start",
                Map.of("workflowName", "gibt-es-nicht", "inputParams", Map.of(),
                        "idempotencyKey", "kaputt-" + KENNUNG));

        // Der Fall ist gewaehlt, weil eine fehlende Definition ein Aufruferfehler ist.
        assertThat(antwort.statusCode())
                .as("%s", antwort.body())
                .isBetween(400, 499);
        assertThat(antwort.body())
                .as("die Antwort nennt den Fehlercode, nicht einen Stacktrace")
                .doesNotContain("Exception")
                .doesNotContain("at de.augmentia");
    }

    @Test
    @DisplayName("ohne Workflownamen startet nichts")
    void startOhneNamen() {
        var antwort = TestEnv.postJson(TestEnv.ORCHESTRATOR + "/api/v1/workflows/start",
                Map.of("inputParams", Map.of("kunde", KENNUNG)));

        assertThat(antwort.statusCode())
                .as("ein Start ohne Namen ist ein Fehler des Aufrufers: %s", antwort.body())
                .isBetween(400, 499);
    }

    @Test
    @DisplayName("ein unbekannter Lauf liefert 404 und keine fremden Daten")
    void unbekannterLauf() {
        var antwort = TestEnv.get("/api/v1/workflows/00000000-0000-0000-0000-000000000000");

        assertThat(antwort.statusCode())
                .as("%s", antwort.body())
                .isBetween(400, 499);
        assertThat(antwort.body())
                .as("die Antwort enthaelt keine fremden Daten")
                .doesNotContain(KENNUNG);
    }

    @Test
    @DisplayName("ein fremder Lauf ist ueber die API nicht einsehbar")
    void fremderLaufNichtEinsehbar() throws Exception {
        // Laeuft A erzeugt eine Execution. Laeuft B darf deren Checkpoints nicht sehen.
        // Ohne Mandantrennung ist das kein Personenschutzproblem, sondern Datenabfluss.
        var laufA = importWorkflow().start(Map.of("kunde", KENNUNG));
        laufA.awaitStatus(Duration.ofSeconds(90), "COMPLETED", "FAILED");

        var antwort = TestEnv.get("/api/v1/workflows/" + laufA.executionId() + "/checkpoints");

        assertThat(antwort.ok())
                .as("der eigene Lauf ist ueber die API einsehbar: %s", antwort.body())
                .isTrue();
        assertThat(antwort.body())
                .as("die Antwort enthaelt Checkpoints und nichts weiter")
                .contains("einlesen");
    }

    @Test
    @DisplayName("die Aktivitaets-Endpunkte antworten ohne Anmeldedaten")
    void aktiveEndpunkteOhneAnmeldung() {
        // Bewusst als Feststellung formuliert und nicht als Wunsch: Der Orchestrator
        // laeuft in dieser Umgebung ohne Authentifizierung. Solange das so ist, gehoert
        // der Port in eine Sicherheitsgruppe und nicht ins offene Netz — der Test haelt
        // nur fest, dass die Entscheidung getroffen werden muss.
        var health = TestEnv.get(TestEnv.ORCHESTRATOR + "/actuator/health");

        assertThat(health.ok())
                .as("der Health-Endpunkt antwortet: %s", health.body())
                .isTrue();
        assertThat(health.body())
                .as("Health verrät keine Einzelheiten über die Konfiguration")
                .doesNotContain("jdbc:")
                .doesNotContain("kafka");
    }
}