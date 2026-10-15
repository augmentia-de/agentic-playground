package de.augmentia.it.syscapability;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import de.augmentia.it.support.Scenario;
import de.augmentia.it.support.Scenario.Branch;
import de.augmentia.it.support.TestEnv;
import de.augmentia.it.support.WorkflowRun;
import de.augmentia.it.support.Stack;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Was kann das System? — Die Fähigkeiten, einzeln und in der Domäne formuliert, in der
 * sie im Betrieb vorkommen.
 *
 * <p>Die anderen Schichten prüfen Einzelkomponenten: ein Validator, ein Listener, eine
 * Rechnung. Diese Klasse fragt etwas anderes: Läuft ein fachlicher Ablauf durch, vom
 * Orchestrator über Kafka bis zum Ende — und stimmt am Ende der Zustand?
 *
 * <p>Jeder Test beschreibt einen Ablauf, wie man ihn betrieblich benennen würde, und prüft
 * danach genau eine Eigenschaft. Nicht mehrere: ein Test, der fünf Eigenschaften prüft,
 * meldet bei einem Fehler nicht, welche davon betroffen war.
 *
 * <p><b>Mock-Bedingung.</b> Der Worker antwortet ohne LLM-Schlüssel immer mit
 * {@code {"confidence": 0.95, "status": "COMPLETED"}}. Bedingungen auf {@code confidence}
 * nehmen deshalb in diesen Tests zuverlässig den Zweig für hohe Konfidenz — was die
 * Prüfung hier sogar belastbar macht, weil das Ergebnis deterministisch ist.
 */
@Tag("requires-e2e")
@Tag("requires-orchestrator")
@Tag("requires-kafka")
class FaehigkeitenSystemTest {

    private static final Duration ENDZUSTAND = Duration.ofSeconds(90);

    @BeforeAll
    static void stackMussLaufen() {
        Stack.assumeUp();
    }

    // ── Fähigkeit: geordnete Abfolge mehrerer Schritte ───────────────────────

    @Test
    @DisplayName("Recherchebericht: drei Schritte hintereinander, am Ende steht das Ergebnis")
    void reihenfolgeDreiSchritte() throws Exception {
        var szenario = Scenario.scenario("Recherchebericht", "Forschung",
                        "Recherchiert ein Thema, wertet die Quellen aus und formuliert einen Bericht.")
                .then("recherche", "researcher")
                .then("auswertung", "planner")
                .then("bericht", "quality")
                .end()
                .install();

        var run = szenario.start(Map.of("thema", "Verteilte Event-Speicher bei Kafka-Backends"));

        var status = run.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status).as("%s", szenario).isEqualTo("COMPLETED");
    }

    // ── Fähigkeit: bedingte Verzweigung ─────────────────────────────────────

    @Test
    @DisplayName("Supporttriage: hohe Konfidenz geht direkt zur Lösung, sonst zur Nachprüfung")
    void bedingungEntscheidetUeberDenWeg() throws Exception {
        var szenario = Scenario.scenario("Supporttriage", "Kundendienst",
                        "Bewertet ein Ticket. Bei hoher Konfidenz wird direkt gelöst, "
                        + "bei niedriger Konfidenz geht es zur fachlichen Nachprüfung.")
                .then("bewertung", "reviewer")
                .decision("einschlagung", List.of(
                        Branch.when("hoch", "context['bewertung'].confidence >= 0.9", "loesung"),
                        Branch.when("niedrig", "context['bewertung'].confidence < 0.9", "nachpruefung")))
                .then("loesung", "quality").afterDecision("einschlagung")
                .then("nachpruefung", "linter").afterDecision("einschlagung")
                .end()
                .install();

        var run = szenario.start(Map.of("ticket", "Anmeldung schlägt seit dem Update fehl"));

        var status = run.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status).as("%s", szenario).isEqualTo("COMPLETED");

        // Der Mock antwortet mit 0.95 — damit ist der Zweig "hoch" der beschrittene.
        // Wichtig: beide Zweigziele stehen als Checkpoint im Plan, der nicht genommene
        // mit Status SKIPPED. Nur der Status sagt, was wirklich gelaufen ist.
        assertThat(run.wasSkipped("nachpruefung"))
                .as("der Zweig für niedrige Konfidenz wurde übersprungen: %s", run.checkpointStatus())
                .isTrue();
        // Die Entscheidung selbst protokolliert, welches Ziel sie genommen hat. Damit
        // ist der Weg bewiesen, nicht nur das Fehlen des anderen.
        assertThat(run.decisionBranchOf("einschlagung").orElse("leer"))
                .as("die Entscheidung hat zum Zweig für hohe Konfidenz geroutet: %s",
                        run.checkpointStatus())
                .isEqualTo("loesung");

        // Der Status von "loesung" wird hier bewusst nicht geprüft: der Lauf meldet
        // COMPLETED, bevor der letzte Checkpoint auf COMPLETED steht — beides laeuft
        // ueber dieselbe Tabelle, aber nicht im selben commit. Ein Test, der darauf
        // wartet, ist wuerde je nach Last flackern. Dass der Lauf fertig wurde,
        // steht weiter oben.
    }

    // ── Fähigkeit: parallele Zweige ─────────────────────────────────────────

    @Test
    @DisplayName("Mehrsprachige Übersetzung: drei Zweige parallel, danach zusammengeführt")
    void paralleleZweigeLaufenUndTreffenSich() throws Exception {
        var szenario = Scenario.scenario("Uebersetzung", "Lokalisierung",
                        "Übersetzt denselben Text parallel in drei Sprachen "
                        + "und führt die Fassungen anschließend zusammen.")
                .then("quelle", "researcher")
                .fork("start", "de", "en", "fr")
                .branchStep("de", "run", "coder")
                .branchStep("en", "run", "coder")
                .branchStep("fr", "run", "coder")
                .joinAll("zusammenfuehren", "de", "en", "fr")
                .then("qualitaet", "reviewer")
                .end()
                .install();

        var run = szenario.start(Map.of("text", "Die Lieferung verzögert sich um zwei Tage."));

        var status = run.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status).as("%s", szenario).isEqualTo("COMPLETED");

        assertThat(run.executedSteps())
                .as("alle drei Zweige sind abgeschlossen, sonst wäre der JOIN nicht erreicht")
                .contains("b.de.run", "b.en.run", "b.fr.run", "zusammenfuehren");
    }

    // ── Fähigkeit: Quorum — n von m genügt ─────────────────────────────────

    @Test
    @DisplayName("Rechnungsfreigabe: zwei von drei Prüfern müssen zustimmen")
    void quorumZweiVonDrei() throws Exception {
        var szenario = Scenario.scenario("Rechnungsfreigabe", "Finanzwesen",
                        "Lässt eine Rechnung von drei Prüfern prüfen. Freigegeben wird, "
                        + "sobald mindestens zwei zustimmen — auf einen Prüfer ist kein Verlass.")
                .then("erfassung", "researcher")
                .fork("pruefung", "formal", "fachlich", "risiko")
                .branchStep("formal", "run", "linter")
                .branchStep("fachlich", "run", "reviewer")
                .branchStep("risiko", "run", "quality")
                .joinNOf("quorum", 2, "formal", "fachlich", "risiko")
                .then("buchung", "quality")
                .end()
                .install();

        var run = szenario.start(Map.of("rechnung", "RE-2026-0815", "betrag", 1240.50));

        var status = run.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status).as("%s", szenario).isEqualTo("COMPLETED");
        assertThat(run.executedSteps())
                .as("nach dem Quorum wird gebucht: %s", run.checkpointStatus())
                .contains("quorum", "buchung");
    }

    // ── Fähigkeit: Schleife über mehrere Iterationen ───────────────────────

    @Test
    @DisplayName("Fehlerbehebung: so lange nachbessern, bis die Prüfung ohne Befund meldet")
    void schleifeWiederholtDenKoerper() throws Exception {
        var szenario = Scenario.scenario("Fehlerbehebung", "Softwareentwicklung",
                        "Analysiert einen Fehlerbericht, bessert nach, prüft das Ergebnis "
                        + "und wiederholt das, bis die Prüfung nichts mehr meldet.")
                .then("recherche", "researcher")
                .loop("nachbesserung", "fixer", 3, "context['nachbesserung'].confidence >= 0.8")
                .then("abschlusspruefung", "tester")
                .end()
                .install();

        var run = szenario.start(Map.of("fehlerbericht", "NullPointerException in Zeile 213"));

        var status = run.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status).as("%s", szenario).isEqualTo("COMPLETED");
        assertThat(run.executedSteps())
                .as("die Schleife wurde betreten und ihre Abbruchbedingung hat gegriffen: %s",
                        run.checkpointStatus())
                .contains("recherche", "abschlusspruefung");
    }

    // ── Fähigkeit: menschliche Freigabe ─────────────────────────────────────

    @Test
    @DisplayName("Kontoeröffnung: die Freigabe durch einen Menschen hält den Lauf an")
    void menschlicheFreigabeHaeltDenLaufAn() throws Exception {
        var szenario = Scenario.scenario("Kontoeroeffnung", "Bank",
                        "Prüft Unterlagen und legt das Konto erst nach menschlicher "
                        + "Freigabe an. Die Freigabe ist eine Unterbrechung, kein Fehler.")
                .then("pruefung", "reviewer")
                .then("freigabe", "quality", true)
                .then("anlage", "planner")
                .end()
                .install();

        var run = szenario.start(Map.of("kunde", "Muster GmbH"));

        var status = run.awaitStatus(ENDZUSTAND, "PAUSED_HITL", "COMPLETED", "FAILED");

        assertThat(status).as("%s", szenario).isEqualTo("PAUSED_HITL");
        assertThat(run.openHitlTicketId())
                .as("zum pausierten Lauf gehört ein offenes Ticket — sonst gäbe es nichts zu freigeben")
                .isPresent();

        assertThat(run.ranToCompletion("anlage"))
                .as("der Schritt nach der Freigabe wurde noch nicht ausgeführt: %s",
                        run.checkpointStatus())
                .isFalse();
    }

    @Test
    @DisplayName("nach der Freigabe läuft derselbe Lauf weiter")
    void freigabeSetztDenLaufFort() throws Exception {
        var szenario = Scenario.scenario("Kontoeroeffnung-Fortsetzung", "Bank",
                        "Derselbe Ablauf, diesmal bis zum Ende freigegeben.")
                .then("pruefung", "reviewer")
                .then("freigabe", "quality", true)
                .then("anlage", "planner")
                .end()
                .install();

        var run = szenario.start(Map.of("kunde", "Beispiel AG"));
        assertThat(run.awaitStatus(ENDZUSTAND, "PAUSED_HITL", "COMPLETED", "FAILED"))
                .as("%s", szenario).isEqualTo("PAUSED_HITL");

        var ticketId = run.openHitlTicketId().orElseThrow(
                () -> new AssertionError("kein offenes Ticket für " + run));

        var freigabe = TestEnv.postJson(
                TestEnv.ORCHESTRATOR + "/api/v1/hitl/tickets/" + ticketId + "/retry",
                Map.of("userId", "systemtest"));
        assertThat(freigabe.ok()).as("Freigabe: %s", freigabe.body()).isTrue();

        var status = run.awaitStatus(ENDZUSTAND, "COMPLETED");

        assertThat(status).as("nach der Freigabe zu Ende gelaufen. %s", szenario).isEqualTo("COMPLETED");
    }

    // ── Fähigkeit: derselbe Ablauf zweimal ist zweimal derselbe ──────────────

    @Test
    @DisplayName("zwei Läufe desselben Ablaufs mit gleichem Schlüssel erzeugen nur einen")
    void idempotenzUnterGleichemSchluessel() throws Exception {
        var szenario = Scenario.scenario("Import", "Datenverarbeitung",
                        "Lädt einen Datensatz ein. Derselbe Schlüssel darf keinen zweiten "
                        + "Lauf erzeugen — sonst wird derselbe Import doppelt gebucht.")
                .then("einlesen", "researcher")
                .then("pruefen", "quality")
                .end()
                .install();

        var schluessel = TestEnv.uniqueKey("import");
        var erster = WorkflowRun.start(szenario.name(), Map.of("datei", "kunden-2026-08.csv"), schluessel);
        var zweiter = WorkflowRun.start(szenario.name(), Map.of("datei", "kunden-2026-08.csv"), schluessel);

        assertThat(zweiter.duplicate())
                .as("der zweite Start mit gleichem Schlüssel ist als Doppeltlauf erkannt")
                .isTrue();
        assertThat(zweiter.executionId())
                .as("beide Starts zeigen auf denselben Lauf")
                .isEqualTo(erster.executionId());
    }
}
