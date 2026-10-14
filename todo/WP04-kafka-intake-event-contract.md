# WP04 — Kafka: Intake-/Event-Vertrag

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Kritisch |
| **Module** | `kafka-platform/agent-orchestrator`, `kafka-platform/agent-common` |
| **Review-Referenz** | B1, B9, §5.4 (IntakeListener-No-op, Redis-Typo) |
| **Abhängigkeit** | – (nach WP03 empfohlen) |
| **Geschätzter Aufwand** | 1–2 Tage |

## Ziel

E-Mail- (und allgemein Fremd-)Ereignisse erreichen den Intake in einer Form, die der Orchestrator
**versteht** und validiert; fehlerhafte Nachrichten führen zu klaren Fehlern statt zu DLQ-Rauschen.

## Befunde (Ist-Zustand)

- **B1 – Eventform inkompatibel:** `EmailInputTransformer` publiziert `ControlEvents.EmailIntakeEvent`
  (`workflowId, triggerKey, emailBody, initiatorEmail, receivedAt`) auf `TopicNames.INTAKE`
  (`EmailInputTransformer.java:58-59`, Record `ControlEvents.java:59-66`). Der Orchestrator liest
  dasselbe Topic als `WorkflowStartEvent` (`DagTopicListener.java:36`), Felder überschneiden sich
  nicht → `workflowExecutionId == null` → `UUID.fromString(...)` wirft (`WorkflowEngine.java:112`) → DLQ.
  Das Feature ist damit vollständig tot.
- **B9 – UUID vor Validierung:** `WorkflowEngine.onResult` parst `UUID` vor dem Null-/Blank-Check
  (`:139`); der Start-Selfcheck erzeugt ein synthetisches Event → DLQ-Rauschen bei **jedem** Boot.
- **§5.4 – `IntakeListener.handleIntake` ist No-op** (ackt nur, `:38-47`); baut pro Nachricht einen
  neuen `ObjectMapper` (`:59`).
- **§5.4 – Redis-Key-Typo** `email:initator:` (`EmailInitiatorStore.java:29`).

## 1. Tests zuerst (Red)

Testort: `kafka-platform/agent-orchestrator/src/test/java/de/augmentia/agentplatform/orchestrator/…`
(Rahmen: `IntakeAndConfigListenerTest`, `WorkflowEngineCharacterizationTest`, `WorkflowEngineTestBase`).

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `EmailIntakeContractTest#emailEvent_startsWorkflow` | `EmailIntakeEvent` auf `INTAKE` → Workflow startet mit korrektem Input |
| 2 | `EmailIntakeContractTest#emailEvent_roundTripsThroughKafkaSerialization` | Producer-JSON → Consumer-Objekt verlustfrei |
| 3 | `WorkflowEngineTest#onIntake_missingWorkflowExecutionId_clearError` | fehlende ID → fachlicher Fehler/Bad-Request, nicht `NumberFormatException` |
| 4 | `WorkflowEngineTest#onResult_invalidUuid_noDlqNoise` | ungültige `taskId` → klare Validierung, kein Selbstverursachter DLQ |
| 5 | `WorkflowEngineTest#startupSelfCheck_doesNotThrow` | Boot ohne DLQ-Eintrag |
| 6 | `IntakeListenerTest#handleIntake_doesRealWork_orRemoved` | echtes Routing oder Listener entfernt |
| 7 | `EmailInitiatorStoreTest#keyUsesCorrectSpelling` | Key `email:initiator:` (falls öffentlich) inkl. Migration |

**Abnahme (Red):** Test 1, 3, 4, 5, 6 fehlschlagen bzw. dokumentieren Fehlverhalten.

## 2. Änderungen (Green)

1. **Vertrag festlegen (eine Wahrheit):** entweder
   - (A) Intake konsumiert ein eigenes `EmailIntakeEvent` in einem separaten Listener und mappt auf
     den Workflow-Start, **oder**
   - (B) `WorkflowStartEvent` bekommt ein Korrelationsfeld `initiatorEmail`/`source`, und
     `EmailInputTransformer` erzeugt ein vollständiges `WorkflowStartEvent`.
   Empfehlung: (B) – ein Event-Typ, weniger Topics.
2. **Robuste Validierung:** `UUID.fromString` erst nach Null-/Blank-/Format-Check; bei Fehler
   `DLQ` mit klarer Fehlermeldung **nur** für echte Fremdfehler, nicht für den Selfcheck.
3. **Selfcheck:** synthetisches Event ohne UUID-Pflicht (eigener Marker) oder Selfcheck abschaltbar.
4. **`IntakeListener`:** entweder echte Arbeit oder Listener + Consumer-Group entfernen
   (sonst verwirrende Lag-Metriken). `ObjectMapper` einmalig als Bean.
5. **Redis-Key:** korrekte Schreibweise; Migrations-/Kompatibilitätshinweis dokumentieren.

## 3. Verifikation

```bash
mvn -pl kafka-platform/agent-orchestrator -am test
cd kafka-platform && mvn -pl agent-orchestrator test
```
- End-to-End mit Testcontainers: E-Mail-Event publizieren → Workflow läuft bis ENDE.
- Boot-Log prüfen: keine DLQ-Einträge durch Selfcheck.

## 4. Risiken / Hinweise

- Bestehende Konsumenten des `INTAKE`-Topics (quad-Bridge) dürfen nicht brechen →
  Kompatibilitätstest (`QuadUiPayloadCompatibilityTest` als Vorbild).
- Redis-Key-Änderung ist nicht rückwärtskompatibel → Migration oder Dual-Read.

## Definition of Done

- [ ] E-Mail-Event startet real einen Workflow (Test 1, 2).
- [ ] Keine `NumberFormatException`-DLQ bei ungültigen/fehlenden IDs (Test 3–5).
- [ ] No-op-Intake-Listener beseitigt oder sinnvoll (Test 6).
- [ ] Redis-Key korrekt bzw. migriert (Test 7).
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Vertragsentscheidung (A/B) | |
| | | 2. Tests rot | |
| | | 3. Umsetzung | |
| | | 4. Verifikation | |
