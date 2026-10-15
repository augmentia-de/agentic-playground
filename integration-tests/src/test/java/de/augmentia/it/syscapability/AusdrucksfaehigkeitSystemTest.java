package de.augmentia.it.syscapability;

import java.util.List;
import java.util.Map;

import de.augmentia.it.support.Scenario;
import de.augmentia.it.support.Scenario.Branch;
import de.augmentia.it.support.TestEnv;
import de.augmentia.it.support.Stack;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kann die Plattform alles ausdrücken, was sie ausdrücken soll — und lässt sie weg, was
 * sie nicht kann?
 *
 * <p>Diese Klasse fragt das Datenmodell ab, nicht den Ablauf. Sie beantwortet zwei
 * Fragen, die auseinanderfallen und beide wichtig sind:
 *
 * <ol>
 *   <li><b>Umfang:</b> Sind alle sechs Knotentypen und alle drei Zusammenfuehrungsarten
 *       in einem Lauf benutzbar? Eine Faehigkeit, die es nicht gibt, ist keine Faehigkeit.</li>
 *   <li><b>Grenzen:</b> Weist die Plattform das zurück, was sie nicht ausfuehren kann?
 *       Eine Definition, die angenommen wird und dann haengt, ist schlimmer als eine
 *       abgelehnte.</li>
 * </ol>
 *
 * <p>Die Grenzen sind nicht Dekoration. Regel R6 etwa verlangt genau einen END-Knoten —
 * ohne diese Regel kaeme jede aus der Oberflaeche kommende Definition an, und die Bruecke
 * muesste den END synthetisch ergaenzen. Dass {@code bridge} das tut, ist kein Feature,
 * sondern eine Ausweichroute vor dieser Regel.
 */
@Tag("requires-e2e")
@Tag("requires-orchestrator")
class AusdrucksfaehigkeitSystemTest {

    @BeforeAll
    static void stackMussLaufen() {
        Stack.assumeOrchestratorUp();
    }

    private static Scenario installiert(String name, String domain, String description,
                                        java.util.function.Consumer<Scenario> aufbau) {
        var szenario = Scenario.scenario(name, domain, description);
        aufbau.accept(szenario);
        return szenario.install();
    }

    // ── Umfang ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("ein Lauf benutzt alle sechs Knotentypen")
    void alleKnotentypenBenutzbar() {
        var szenario = installiert("Alle-Knotentypen", "Grundlagen",
                "Ein Ablauf, in dem jeder Knotentyp mindestens einmal vorkommt: "
                + "Arbeitsschritt, Entscheidung, Schleife, Gabelung, Zusammenfuehrung, Ende.",
                s -> s.then("vorbereiten", "planner")
                        .decision("entscheidung", List.of(
                                Branch.when("normal", "context['vorbereiten'].confidence >= 0.5", "schleife")),
                                // Regel R3: fuehrt ein Zweig in eine Schleife, muss die
                                // Entscheidung den Ausgang benennen — sonst weiss der
                                // Orchestrator nicht, wo es nach der Schleife weitergeht.
                                "sammeln")
                        .loop("schleife", "refiner", 2, "context['schleife'].confidence >= 0.5")
                        .then("sammeln", "researcher")
                        .fork("gabel", "a", "b")
                        .branchStep("a", "run", "coder")
                        .branchStep("b", "run", "tester")
                        .joinAll("zusammen", "a", "b")
                        .end());

        assertThat(szenario.nodes())
                .as("der Ablauf deckt STEP, DECISION, LOOP, FORK, JOIN und END ab")
                .hasSize(9)
                .extracting(n -> String.valueOf(n.get("nodeType")))
                .contains("STEP", "DECISION", "LOOP", "FORK", "JOIN", "END");
    }

    @Test
    @DisplayName("alle drei Zusammenfuehrungsarten werden angenommen")
    void alleJoinArtenAngenommen() {
        // AND: alle Zweige müssen ankommen. XOR: genau einer. N_OF: eine Anzahl.
        installiert("Join-AND", "Grundlagen", "Alle Zweige müssen ankommen.",
                s -> s.then("start", "planner")
                        .fork("gabel", "a", "b")
                        .branchStep("a", "run", "coder")
                        .branchStep("b", "run", "coder")
                        .joinAll("zusammen", "a", "b")
                        .end());

        installiert("Join-XOR", "Grundlagen", "Genau einer der Zweige gewinnt.",
                s -> s.then("start", "planner")
                        .fork("gabel", "a", "b")
                        .branchStep("a", "run", "coder")
                        .branchStep("b", "run", "tester")
                        .joinXor("zusammen", "a", "b")
                        .end());

        installiert("Join-N_OF", "Grundlagen", "Einer von zwei Zweigen genügt.",
                s -> s.then("start", "planner")
                        .fork("gabel", "a", "b")
                        .branchStep("a", "run", "coder")
                        .branchStep("b", "run", "tester")
                        .joinNOf("zusammen", 1, "a", "b")
                        .end());
    }

    @Test
    @DisplayName("die mitgelieferten Vorlagen sind alle startbar")
    void alleSeedVorlagenSindDefiniert() {
        var namen = TestEnv.get("/api/v1/workflow-definitions").json().findValuesAsText("name");

        assertThat(namen)
                .as("die Vorlagen decken dieRouting-Muster ab, die der Orchestrator kann")
                .contains("reference-code-quality", "xor-race", "n-of-quorum",
                        "guard-skip", "error-taxonomy");
    }

    // ── Grenzen ──────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("was die Plattform ablehnt")
    class Grenzen {

        @Test
        @DisplayName("zwei Schritte mit demselben Namen")
        void doppelteStepIds() {
            var abgelehnt = Scenario.scenario("Doppelte-Ids", "Grundlagen",
                            "Zwei Schritte mit demselben Namen — das waere nicht unterscheidbar.")
                    .then("schritt", "planner")
                    .then("schritt", "quality")
                    .end()
                    .installExpectingRejection();

            assertThat(abgelehnt.statusCode()).as("%s", abgelehnt.body()).isBetween(400, 499);
            assertThat(abgelehnt.body()).contains("R7");
        }

        @Test
        @DisplayName("ein Arbeitsschritt ohne Agent")
        void schrittOhneAgent() {
            var abgelehnt = Scenario.scenario("Ohne-Agent", "Grundlagen",
                            "Ein Arbeitsschritt ohne agentType — wer sollte ihn ausfuehren?")
                    .then("arbeiten", null)
                    .end()
                    .installExpectingRejection();

            assertThat(abgelehnt.statusCode()).as("%s", abgelehnt.body()).isBetween(400, 499);
            assertThat(abgelehnt.body()).contains("R8");
        }

        @Test
        @DisplayName("eine Abhaengigkeit auf einen unbekannten Schritt")
        void abhaengigkeitAufUnbekannt() {
            var abgelehnt = Scenario.scenario("Unbekannte-Abhaengigkeit", "Grundlagen",
                            "Ein Schritt verweist auf einen Schritt, den es nicht gibt.")
                    .then("erster", "planner")
                    .then("zweiter", "quality", Map.of("dependsOn", List.of("gibt-es-nicht")))
                    .end()
                    .installExpectingRejection();

            assertThat(abgelehnt.statusCode()).as("%s", abgelehnt.body()).isBetween(400, 499);
            assertThat(abgelehnt.body()).contains("R7");
        }

        @Test
        @DisplayName("eine Endlosschleife ohne Schleifenknoten")
        void endlosschleife() {
            // Achtung, die Richtung: der Validator laesst Kanten von einem Knoten zu
            // seinen dependsOn laufen. Ein gerichteter Kreis braucht daher drei Knoten,
            // in denen jeder auf den naechsten zeigt und der letzte auf den ersten.
            var abgelehnt = Scenario.scenario("Endlosschleife", "Grundlagen",
                            "Drei Arbeitsschritte, jeder verweist auf den naechsten, "
                            + "der letzte auf den ersten. Ohne Schleifenknoten endet das nie.")
                    .then("a", "planner", Map.of("dependsOn", List.of("c")))
                    .then("b", "quality", Map.of("dependsOn", List.of("a")))
                    .then("c", "planner", Map.of("dependsOn", List.of("b")))
                    .end()
                    .installExpectingRejection();

            assertThat(abgelehnt.statusCode()).as("%s", abgelehnt.body()).isBetween(400, 499);
            assertThat(abgelehnt.body()).contains("R2");
        }

        @Test
        @DisplayName("eine Zusammenfuehrung ohne Zweige")
        void joinOhneZweige() {
            var abgelehnt = Scenario.scenario("Join-ohne-Zweige", "Grundlagen",
                            "Eine Zusammenfuehrung, auf die nichts zielt.")
                    .then("a", "planner")
                    .join("zusammen", "AND", null, List.of())
                    .end()
                    .installExpectingRejection();

            assertThat(abgelehnt.statusCode()).as("%s", abgelehnt.body()).isBetween(400, 499);
            assertThat(abgelehnt.body()).contains("R4");
        }

        @Test
        @DisplayName("eine Zusammenfuehrung, die keine der erlaubten Arten ist")
        void joinMitUnbekannterArt() {
            // Der Validator kennt AND, XOR und N_OF. "ALL" sieht verstaendlich aus und
            // wird trotzdem abgelehnt — genau deshalb steht es hier.
            var abgelehnt = Scenario.scenario("Join-mit-ALL", "Grundlagen",
                            "Eine Zusammenfuehrung mit der Art 'ALL'.")
                    .then("start", "planner")
                    .fork("gabel", "a", "b")
                    .branchStep("a", "run", "coder")
                    .branchStep("b", "run", "coder")
                    .join("zusammen", "ALL", null, List.of("a", "b"))
                    .end()
                    .installExpectingRejection();

            assertThat(abgelehnt.statusCode()).as("%s", abgelehnt.body()).isBetween(400, 499);
            assertThat(abgelehnt.body()).contains("R4");
        }
    }
}
