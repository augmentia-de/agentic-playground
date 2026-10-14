# WP07 — Workflow-Engine: Ausführung & Lifecycle

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `quad-quarkus/quad-workflow-engine`, `quad-quarkus/quad-quarkus` |
| **Review-Referenz** | B23, B24, B25, B28, B29, B30 (§5.2, §5.3) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 2–3 Tage |

## Ziel

Die Engine leakt keine Runs, verliert keine asynchronen Ergebnisse, erzeugt valides JSON,
blockiert nicht im Busy-Wait und kann einen Node nicht doppelt ausführen. Sandbox-Prozesse
speichern nicht unbegrenzt.

## Befunde (Ist-Zustand)

- **B23 – Run-Leak:** `WorkflowEngine.execute` ruft `removeRun` nur im Erfolgspfad
  (`WorkflowEngine.java:98`); Fehlerpfad (`:115-129`) lässt `runOutputs`-Eintrag stehen.
- **B24 – stiller No-op:** `WorkflowSupport.completeDeferred` schreibt bei nicht registriertem Run
  nichts und kehrt zurück (`:169-176`); Aufrufer meldet Erfolg.
- **B25 – unescaptes JSON:** Event-Payloads per String-Konkatenation (`:57-58,110,127`).
- **B28 – Busy-Wait/Endlosschleife:** `AsyncNodeExecutor` pollt mit `Thread.sleep(50)`, bei
  `timeout==0` Endlosschleife (`:78-100`); `WorkflowScheduler` pollt ebenso (`:55-64`).
- **B29 – Doppelausführung:** `AsyncNodeExecutor` ruft nach `runChildNode(...)==null` zusätzlich
  `runner.runNode(...)` (`:61-65`).
- **B30 – unbounded stderr:** nur stdout wird gedrosselt, stderr wächst unbegrenzt
  (`SandboxPoolClient.java:132-160`).

## 1. Tests zuerst (Red)

Testort: `quad-quarkus/quad-workflow-engine/src/test/java/de/augmentia/quad/workflow/engine/…`
(vorhanden: `WorkflowEngineContainerlessTest`, `internal/WorkflowSupportTimeoutTest`,
`node/PrefixSourceResolverTest`, `LoopNodeExecutor*Test`).

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `WorkflowEngineLifecycleTest#failedRun_isRemovedFromRegistry` | Fehler → `runOutputs` leer |
| 2 | `WorkflowEngineLifecycleTest#successRun_isRemoved` | Erfolg → leer (Bestand) |
| 3 | `WorkflowSupportTest#completeDeferred_missingRun_fails` | fehlender Run → Exception/Callback, kein stiller Erfolg |
| 4 | `WorkflowEngineEventJsonTest#specialChars_produceValidJson` | Name/Fehlermeldung mit `"`/`\n` → valides JSON (Jackson-Parse) |
| 5 | `AsyncNodeExecutorTest#noTimeout_doesNotHangForever` | `timeout==0` blockiert nicht unbegrenzt |
| 6 | `AsyncNodeExecutorTest#childExecutedExactlyOnce` | Node läuft genau einmal |
| 7 | `SandboxOutputTest#stderrBounded` | stderr-Flut → Ausgabe begrenzt (kein OOM) |

**Abnahme (Red):** 1, 3, 4, 6, 7 fehlschlagen.

## 2. Änderungen (Green)

1. **Lifecycle:** `removeRun` in `finally`; Registry-Einträge bekommen TTL/Cleanup.
2. **`completeDeferred`:** fehlender Run → klarer Fehler (Exception/`Result.failure`), Aufrufer
   propagiert; `DynamicWorkflowResource.complete` darf nicht mehr blind „ok" melden.
3. **JSON:** zentralen `ObjectMapper` (bereits vorhanden) für Event-Payloads nutzen; keine
   String-Konkatenation.
4. **Async:** Benachrichtigung über vorhandenes `monitor`-Objekt statt Polling; `timeout==0`
   definiert (unendlich nur mit echtem Signal oder hart begrenzt).
5. **Doppelausführung:** eindeutige Dispatch-Entscheidung; Fallback entfernen oder idempotent machen.
6. **Sandbox:** auch stderr über den Ringpuffer mit identischem Cap führen.

## 3. Verifikation

```bash
cd quad-quarkus && mvn -pl quad-workflow-engine test
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='*Workflow*'
```
- Langläufiger Test: viele fehlschlagende Runs → Heap stabil.
- Sandbox mit `yes >&2`-ähnlichem Kommando → kein OOM.

## 4. Risiken / Hinweise

- `monitor`-Umbau berührt mehrere Executors → sorgfältig synchronisieren (Lost-Wakeup).
- JSON-Umstellung kann bestehende Event-Konsumenten beeinflussen → Kompatibilitätstest.

## Definition of Done

- [ ] Kein Run-Leak (Test 1, 2).
- [ ] `completeDeferred` schlägt laut fehl statt still (Test 3).
- [ ] Events sind valides JSON (Test 4).
- [ ] Kein Busy-Wait/Endlosschleife (Test 5), keine Doppelausführung (Test 6).
- [ ] stderr begrenzt (Test 7).
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
