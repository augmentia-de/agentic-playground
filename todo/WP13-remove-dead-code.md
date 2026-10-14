# WP13 — Toten Code entfernen

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | alle |
| **Review-Referenz** | A5, §4.1–§4.4 |
| **Abhängigkeit** | **nach WP05–WP11** (dort wird teils noch angefasst) |
| **Geschätzter Aufwand** | 2 Tage |

## Ziel

Code, der nie ausgeführt wird, wird entfernt oder klar als experimentell/Test gekennzeichnet.
Kein „implementiert"-Versprechen ohne erreichbaren Aufrufpfad.

## Vorgehen

> **Wichtig:** Jede Löschung zuerst mit einem Tot-Code-Nachweis belegen
> (Graph `trace_path` inbound + `grep -rn`), nicht nur „sieht unbenutzt aus".

Pro Kandidat: Nachweis → Löschen/Verlagern → Build + Tests grün → Changelog-Eintrag.

## Befunde (Ist-Zustand, Auswahl)

### quad-core (konkret tot)
`GuardrailException`, `QuadEventStore`, `SessionLockService` (zudem funktionslos: leerer
`LockEntry.refresh`, No-op `heartbeat`, `:40,46`), `AgentMemory`, `MultiSystemMessageChatMemory`,
`SummarizingConversationManager`, `SagaAwareConversationManager` (No-op `:11-15`),
`StateDiffMemoryBridge` (unsynchronisiert, nie geleert `:9-11`), `ToolSetChangeNotifier`.

**Nur test-verdrahtet:** `EventQuery`, `InspectInputsPrefill`, `TextOnlyGuard`,
`StructuredOutputValidator`, `TokenRecovery`, `QuadConfig`, `RunSnapshot`,
`SlidingWindowConversationManager`, `PredictStrategy`, `ReflexionStrategy`, `TemplateStrategy`,
`TimerChannel` → **behalten oder in Test-Modul verschieben**, nicht Produktions-`main`.

### quad-quarkus
- **v2-Transfer-/Publish-Pipeline tot:** `QuadWorkflowPublishCompiler`, `QuadWorkflowUpcaster`,
  `QuadWorkflowSemanticValidator`, `QuadWorkflowHitlNormalizer` nur in Tests/Docs —
  **entweder** in `WorkflowTransferService` verdrahten (siehe Hinweis unten) **oder** entfernen.
- **`quad-quarkus/src/test/tmp/`** (5 E2E-Dateien) wird **nicht kompiliert** (kein `build-helper`,
  keine POM-Referenz) → entweder in `src/test/java` verschieben oder löschen.
- Datei-/Klassen-Mismatch: `NooaWorkflowSchemaValidatorTest.java` deklariert
  `class QuadWorkflowSchemaValidatorTest`.
- Tote Methoden: `AgentController.matches*` (`:736-769`), `McpManagerService.bridgeToSystemProperties`
  (`:241-249`), `WorkflowEngine.onMessage/hasPendingHandler` (`:189-199`),
  `AgentStepExporter.reset` (`:98-102`), `AutomationService.scheduledPrompt` (`:176-179`);
  totes Feld `SandboxPoolClient.executor` (`:48,69`).

### kafka-platform (laut Doku „fertig", unerreichbar)
`SlackNotifier`, `PagerDutyAlertSender`, `SchemaRegistryClient` (Log-No-op), `VaultConfig`,
`SecretResolver` (falls in WP02 entfernt), `PrefixCache`, `MailResponseHandler` (inert).

### agentflow
Mehrere parallele `*WorkflowImpl`-/Stub-Klassen (`QuadSagaWorkflow`, `SagaWorkflowImpl`,
`RichAgentWorkflowImpl`, `HitlWorkflowImpl`, `AuthorizedAgentWorkflowImpl`, …) →
**Dead-Code-Kandidaten**, erst verifizieren (Temporal-Registrierung/Caller), dann entscheiden.

## 1. Tests zuerst

- Für jeden Kandidaten: Nachweis-Skript/Notiz (Graph + `grep`).
- Sicherstellen, dass nach Entfernen die Modul-Testsuite grün bleibt (keine versteckten
  Reflexionsaufrufe → `grep` nach Klassennamen als String).
- **Sonderfall v2-Pipeline:** wenn **verdrahtet** statt gelöscht werden soll, gehört das in ein
  eigenes Paket (Test der semantischen Validierung beim Import), nicht hier.

## 2. Änderungen (Green)

1. Konkret toten Code löschen (Datei + Referenzen + Tests, die nur Deklaration prüfen).
2. Test-only-Code aus `main` in Testquellen verschieben.
3. `src/test/tmp` auflösen (verschieben/löschen).
4. Namens-Mismatch korrigieren.
5. Für jede Entfernung **Doku-Claim** in den Md-Dateien streichen (sonst Doku-Lüge) → Abstimmung mit WP14.

## 3. Verifikation

```bash
mvn -T1C clean install
cd quad-quarkus && mvn test
cd kafka-platform && mvn verify
cd agentflow && mvn test
```
- `grep -rn "<EntfernteKlasse>"` → keine Treffer (außer Changelog).

## 4. Risiken / Hinweise

- Reflexion/SPI/Jackson-Polymorphie kann „tot" vortäuschen → vor Löschen prüfen.
- **Nicht** löschen, was ein früheres WP noch braucht (daher Abhängigkeit).

## Definition of Done

- [ ] Jeder Kandidat: Nachweis dokumentiert.
- [ ] Alle Builds grün.
- [ ] Keine „implementiert"-Claim im Doku ohne erreichbaren Code (mit WP14 synchron).
- [ ] Fortschrittslog mit Löschliste (Datei → Begründung).

## Fortschrittslog

| Datum | Bearbeiter | Entfernt | Nachweis |
|-------|-----------|----------|----------|
| | | | |
