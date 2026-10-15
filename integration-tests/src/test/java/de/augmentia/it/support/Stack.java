package de.augmentia.it.support;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Gemeinsame Voraussetzung fuer alle Testklassen, die gegen den laufenden Stack testen.
 *
 * <p>Die fuenf {@code syscapability}-Klassen und {@code PlatformEndToEndTest} hatten
 * dieselbe {@code @BeforeAll}-Methode jeweils kopiert ({@code stackMussLaufen}). Kopien
 * driften: aendert jemand die Bedingung in einer Klasse, gilt sie fuer die anderen nicht
 * mehr. Hier steht sie einmal.
 *
 * <p>Die Pruefungen blockieren nie: fehlt ein Dienst, wird die Klasse uebersprungen
 * (Assumption) statt rot — ein Checkout ohne laufende Infrastruktur bleibt gruen.
 */
public final class Stack {

    private Stack() {}

    /** Ueberspringt die Klasse, wenn Broker, Orchestrator oder Worker fehlen. */
    public static void assumeUp() {
        assumeThat(TestEnv.kafkaUp())
                .as("Broker fehlt unter %s", TestEnv.BOOTSTRAP).isTrue();
        assumeThat(TestEnv.orchestratorUp())
                .as("Orchestrator fehlt unter %s", TestEnv.ORCHESTRATOR).isTrue();
        assumeThat(TestEnv.workerUp())
                .as("kein Worker unter %s — ohne Worker wartet die Arbeit in Kafka, "
                        + "statt abgearbeitet zu werden", TestEnv.WORKER).isTrue();
    }

    /** Ueberspringt die Klasse, wenn nur der Orchestrator fehlt (kein Broker/Worker noetig). */
    public static void assumeOrchestratorUp() {
        assumeThat(TestEnv.orchestratorUp())
                .as("Orchestrator fehlt unter %s", TestEnv.ORCHESTRATOR).isTrue();
    }
}
