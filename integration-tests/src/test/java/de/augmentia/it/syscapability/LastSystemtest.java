package de.augmentia.it.syscapability;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * Wie viel schafft das System, und womit merkt man, dass es zu viel war?
 *
 * <p>Diese Klasse behauptet keine Leistungszusage. Ein Test mit einer Zeitgrenze, die auf
 * einer ruhigen Maschine gilt und auf einer ausgelasteten nicht, wird zu einem
 * Zufallsgenerator, den irgendwann jemand stummschaltet. Stattdessen zwei Aussagen, die
 * unter Last stabil bleiben:
 *
 * <ol>
 *   <li><b>Vollständigkeit:</b> N gestartete Läufe kommen auch N Endzustände. Eine Plattform,
 *       die bei viel Verkehr Läufe verschluckt, verliert Arbeit ohne Fehlermeldung — das ist
 *       der teuerste Fehler, den sie machen kann.</li>
 *   <li><b>Transparenz:</b> Nach der Last ist der Rückstand der Topics ablesbar. Fehlende
 *       Arbeit zeigt sich als Rückstand; wer nur auf HTTP-Antworten sieht, sieht sie nicht.</li>
 * </ol>
 *
 * <p>Die Zahlen sind bewusst klein gewählt (20 Läufe). Der Test soll eine Properties-Eigenschaft
 * zeigen, keine Kapazitätsgrenze messen — die hängt von der Maschine ab und wäre hier nicht
 * aussagekräftig.
 */
@Tag("requires-e2e")
@Tag("requires-orchestrator")
@Tag("requires-kafka")
class LastSystemtest {

    private static final Duration ENDZUSTAND = Duration.ofSeconds(180);

    @BeforeAll
    static void stackMussLaufen() {
        Stack.assumeUp();
    }

    private static Scenario importWorkflow() {
        return Scenario.scenario("Datenimport", "Datenverarbeitung",
                        "Liest einen Satz ein, prueft ihn und uebernimmt ihn. "
                        + "Der Standardablauf fuer die Lastmessung.")
                .then("einlesen", "researcher")
                .then("pruefen", "quality")
                .then("uebernehmen", "planner")
                .end()
                .install();
    }

    @Test
    @DisplayName("zwanzig gleichzeitige Importe kommen alle zu einem Ende")
    void zwanzigImporteVollstaendig() throws Exception {
        var szenario = importWorkflow();

        List<WorkflowRun> laeufe = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            laeufe.add(szenario.start(Map.of("datei", "import-" + i)));
        }

        var fertige = new ArrayList<String>();
        var unfertige = new ArrayList<String>();
        for (var lauf : laeufe) {
            var status = lauf.awaitStatus(ENDZUSTAND, "COMPLETED", "FAILED", "PAUSED_HITL", "CANCELLED");
            if ("COMPLETED".equals(status)) {
                fertige.add(lauf.executionId());
            } else {
                unfertige.add(lauf.executionId() + " -> " + status);
            }
        }

        assertThat(unfertige)
                .as("von %d Laeufen kamen %d zu einem Ende. Verlorene Arbeit ist der "
                        + "teuerste Fehler: Sie faellt nur auf, wenn man hier nachzaehlt.",
                        laeufe.size(), fertige.size())
                .isEmpty();
    }

    @Test
    @DisplayName("nach der Last stimmt die Bilanz: jeder Lauf hat seine drei Schritte")
    void bilanzNachDerLastStimmt() throws Exception {
        var szenario = importWorkflow();

        var laeufe = new ArrayList<WorkflowRun>();
        for (int i = 0; i < 10; i++) {
            laeufe.add(szenario.start(Map.of("datei", "bilanz-" + i)));
        }

        for (var lauf : laeufe) {
            assertThat(lauf.awaitStatus(ENDZUSTAND, "COMPLETED"))
                    .as("%s", lauf).isEqualTo("COMPLETED");
        }

        for (var lauf : laeufe) {
            assertThat(lauf.executedSteps())
                    .as("Lauf %s hat alle drei Schritte: %s", lauf.executionId(), lauf.checkpointStatus())
                    .contains("einlesen", "pruefen", "uebernehmen");
        }
    }

    @Test
    @DisplayName("der Topic-Rueckstand waechst mit jedem Lauf sichtbar mit")
    void rueckstandIstSichtbar() throws Exception {
        // Ohne diese Aussage waere ein Puffern von Tasks unsichtbar: die Starts antworten,
        // die Arbeit wartet, und niemand merkt es. Der Rueckstand ist das einzige Mass,
        // das die Arbeit sichtbar macht, die noch nicht erledigt ist.
        //
        // Frueher pruefte der Test nur "endOffsets >= 0" — das gilt immer und sagte
        // nichts ueber die Arbeit. Jetzt wird die Differenz gemessen: jeder gestartete
        // Lauf muss mindestens eine Nachricht auf dem Task-Topic hinterlassen.
        var szenario = importWorkflow();
        var vorher = Math.max(0, endOffsets(szenario.taskTopic()));

        var laeufe = new ArrayList<WorkflowRun>();
        for (int i = 0; i < 8; i++) {
            laeufe.add(szenario.start(Map.of("datei", "rueckstand-" + i)));
        }
        for (var lauf : laeufe) {
            lauf.awaitStatus(ENDZUSTAND, "COMPLETED");
        }

        var rueckstand = Math.max(0, endOffsets(szenario.taskTopic())) - vorher;

        assertThat(rueckstand)
                .as("Auf %s liegen nach %d Laeufen %d neue Nachrichten; mindestens eine "
                        + "je Lauf war erwartet.",
                        szenario.taskTopic(), laeufe.size(), rueckstand)
                .isGreaterThanOrEqualTo(laeufe.size());

        // Und die Kette ist laufbereit: ein frischer Lauf findet ein funktionierendes
        // System vor.
        var danach = szenario.start(Map.of("datei", "nach-dem-Rueckstand"));
        assertThat(danach.awaitStatus(ENDZUSTAND, "COMPLETED"))
                .as("%s", danach).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("viele Läufe hintereinander bleiben getrennt")
    void vieleHintereinander() throws Exception {
        var szenario = importWorkflow();

        for (int i = 0; i < 12; i++) {
            var lauf = szenario.start(Map.of("datei", "serie-" + i));
            assertThat(lauf.awaitStatus(ENDZUSTAND, "COMPLETED"))
                    .as("Lauf %d/%d: %s", i + 1, 12, lauf.checkpointStatus())
                    .isEqualTo("COMPLETED");
        }
    }

    /** Der Endstand des Topics — die Masse, die noch aufgearbeitet werden muss. */
    private static long endOffsets(String topic) {
        var props = new java.util.Properties();
        props.put("bootstrap.servers", TestEnv.BOOTSTRAP);
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "it-lag-messung-" + java.util.UUID.randomUUID());
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            consumer.poll(Duration.ofSeconds(2));
            var zuweisung = consumer.assignment();
            if (zuweisung.isEmpty()) {
                return -1;
            }
            long summe = 0;
            for (var tp : zuweisung) {
                summe += consumer.endOffsets(java.util.List.of(tp)).getOrDefault(tp, 0L);
            }
            return summe;
        } catch (Exception e) {
            throw new IllegalStateException("Topic " + topic + " nicht lesbar", e);
        }
    }
}
