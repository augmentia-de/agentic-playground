# WP03 — Kafka: Publish/Ack & Worker-Konfiguration

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Kritisch |
| **Module** | `kafka-platform/quad-workflow-worker` |
| **Review-Referenz** | B2, B37, B38, B39, B40 (§5.1, §5.3) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 1–2 Tage |

## Ziel

Ein Arbeitsergebnis gilt erst dann als „verarbeitet", wenn es **bestätigt** in Kafka liegt.
Zusätzlich: konsistente Konfiguration, korrekte Metriken, keine doppelten Beans.

## Befunde (Ist-Zustand)

- **B2 – Ack vor bestätigtem Publish:** `WorkflowResultPublisher.send` und
  `WorkflowDlqPublisher.send` nutzen asynchrones `producer.send(...)` ohne `.get()`/Flush;
  `TaskExecutor.safeExecute` liefert danach sofort `true`; `WorkflowTaskListener` ackt
  (`WorkflowResultPublisher.java:36-41`, `WorkflowDlqPublisher.java:46`,
  `TaskExecutor.java:140-183`, `WorkflowTaskListener.java:107-132`). Kein Shutdown-Flush.
- **B37 – Metrik-Mismatch:** `WorkerMetrics` registriert `…completed_total{status="failed"}`, der
  Failure-Pfad inkrementiert `…failed_total` (`:46-49,90-94`).
- **B38 – doppelte Beans:** `GracefulShutdown`, `WorkerMetrics` je als `@Component` **und** `@Bean`
  (`QuadWorkerConfig.java:428-455`) → nur wegen `allow-bean-definition-overriding: true` bootbar (`application.yml:20`).
- **B39 – Bootstrap-Defaults inkonsistent:** Producer `localhost:9092` (`QuadWorkerConfig.java:52-58`)
  vs. Consumer `kafka:9092` (`KafkaConfig.java:26`); nicht-transaktionaler Producer bei
  `read_committed` (`KafkaConfig.java:61`).
- **B40 – `worker.quad.concurrency` ungenutzt:** gebunden, aber nirgends gelesen
  (`application.yml:51`, echte Grenze `TaskExecutor.java:97`).

## 1. Tests zuerst (Red)

Testort: `kafka-platform/quad-workflow-worker/src/test/java/de/augmentia/quad/worker/…`
(vorhandener Rahmen: `WorkflowTaskListenerTest`, `WorkerTestFixtures`, `QuadIntegrationContractTest`).

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `ResultPublisherAckTest#ackOnlyAfterSendConfirmed` | `safeExecute` liefert erst `true`, wenn Publish bestätigt |
| 2 | `ResultPublisherAckTest#sendFailure_returnsFalse_noAck` | Publish schlägt fehl → `false` → kein Ack |
| 3 | `ResultPublisherAckTest#flushOnShutdown` | gepufferte Nachrichten werden beim Shutdown geflusht |
| 4 | `WorkerMetricsTest#failureIncrementsRegisteredCounter` | Failure erhöht **genau** den exportierten Zähler |
| 5 | `WorkerContextTest#noDuplicateBeans` | Kontext startet auch ohne `allow-bean-definition-overriding` |
| 6 | `KafkaConfigTest#bootstrapDefaultsConsistent` | Producer- und Consumer-Default identisch |
| 7 | `WorkerPropertiesBindingTest#concurrency_isApplied` | `concurrency` steuert die Listener-Parallelität |

**Abnahme (Red):** 1, 2, 4, 5, 7 fehlschlagen.

## 2. Änderungen (Green)

1. **Bestätigtes Senden:** `WorkflowResultPublisher`/`WorkflowDlqPublisher` synchronisieren die
   Bestätigung (`.get(timeout)` oder Callback + `CountDownLatch`), `TaskExecutor` gibt erst nach
   erfolgreicher Bestätigung `true` zurück. `KafkaTemplate.flush()` in einen Shutdown-Hook
   (`GracefulShutdown`).
2. **Metrik:** einen Zähler pro Status (`completed_total{status=…}`) verwenden, keine zweiseitige
   Registrierung; Felder statt Neu-`register` nutzen.
3. **Beans:** Doppel-Definitionen entfernen; `allow-bean-definition-overriding` auf `false`.
4. **Config:** gemeinsamen Bootstrap-Default (Property mit klarem Default); Producer
   transaktional/replay-konsistent zu `read_committed` machen oder `isolation.level` anpassen.
5. **`concurrency`:** entweder Listener-Concurrency daraus setzen **oder** Property entfernen und
   Doku korrigieren.

## 3. Verifikation

```bash
mvn -pl kafka-platform/quad-workflow-worker -am test
cd kafka-platform && mvn -pl quad-workflow-worker test
```
- Lasttest: Producer-Ausfall simulieren (Broker kurz stoppen) → Record wird redelivered, kein Verlust.
- Shutdown-Test: Worker bei ausstehenden Nachrichten stoppen → keine Verluste im Log.

## 4. Risiken / Hinweise

- Synchrones `.get()` reduziert Durchsatz; Timeout und Batch-Größe abwägen
  (Alternative: Callback + Semaphore pro Record).
- Redelivery setzt Idempotenz auf Orchestrator-Seite voraus (vorhanden, aber prüfen).

## Definition of Done

- [ ] Ack erst nach bestätigtem Publish (Tests 1–3 grün).
- [ ] Shutdown-Flush vorhanden.
- [ ] Metrik eindeutig und exportiert (Test 4).
- [ ] Keine doppelten Beans, kein `allow-bean-definition-overriding` (Test 5).
- [ ] Bootstrap-/Isolation-Konsistenz (Test 6), `concurrency` wirkt oder entfernt (Test 7).
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
