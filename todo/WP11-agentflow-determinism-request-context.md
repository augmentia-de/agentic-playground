# WP11 — agentflow: Determinismus & Request-Context

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Hoch |
| **Module** | `agentflow/agentflow-app` |
| **Review-Referenz** | B12 (§5.2), §6 (Declarative: Replay/Pruning/Loop) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 2–3 Tage |

## Ziel

Temporal-Workflow-Code ist deterministisch (kein `HashMap`-Ordnungseinfluss, stabiles Pruning,
sinnvolle Loop-Grenzen), und per-Request-Header/Tenant wirken auf **allen** Activity-Pfaden.

## Befunde (Ist-Zustand)

- **B12 – Request-Context fehlt:** Nur `runAgentGraph` setzt `RequestContext`
  (`AgentFlowActivitiesImpl.java:35,59`); `runQuadAgent`/`runRichAgent`/`runAuthorizedAgent`
  (`:78,84,95`) nicht → Header/Tenant still ignoriert; potenzielles ThreadLocal-Leck.
- **§6 – Declarative-Workflow:**
  - `HashSet`/`HashMap` in Workflow-State (`DeclarativeAgentWorkflow.java:53,71`) — Ordnung kann
    Eingaben/Replay beeinflussen.
  - **Nicht-transitives Pruning** (`:138-142`): nur direkte Nicht-Ziele, Nachfahren weder
    übersprungen noch ausgeführt → stilles Weglassen aus `outputs`.
  - **Loop-Default** `Math.max(1, maxIterations())` (`:149-171`) → unset läuft genau einmal; kein
    Repository-weites Iterations-Limit.

## 1. Tests zuerst (Red)

Testort: `agentflow/agentflow-app/src/test/java/de/augmentia/agentflow/app/workflow/declarative/…`
(vorhanden: `DeclarativeAgentWorkflowTest`, `ConditionEvaluatorTest`, `DeclarativePlannerTest`)
und `…/config/AgentFlowLlmHeaderPropertiesTest`, `LlmConfigBridgeTest`.

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `AgentFlowActivitiesImplTest#runQuadAgent_setsRequestContext` | Header/Tenant wirken |
| 2 | `AgentFlowActivitiesImplTest#runRichAgent_setsRequestContext` | dito |
| 3 | `AgentFlowActivitiesImplTest#runAuthorizedAgent_setsRequestContext` | dito |
| 4 | `AgentFlowActivitiesImplTest#contextIsClearedAfterRun` | kein ThreadLocal-Leck |
| 5 | `DeclarativeAgentWorkflowTest#deterministicContextOrdering` | gleiche Eingabe → gleicher Activity-Input (sortiert) |
| 6 | `DeclarativeAgentWorkflowTest#pruningIsTransitive` | Nachfahren gepruned werden als `skipped` markiert |
| 7 | `DeclarativeAgentWorkflowTest#loopRespectsHardCap` | zu hohe Iterationen → hart begrenzt + Hinweis |
| 8 | `DeclarativeAgentWorkflowTest#unsetIterations_definedBehavior` | `0` → dokumentiertes Verhalten (nicht still 1×) |

**Abnahme (Red):** 1–3, 5, 6, 8 fehlschlagen.

## 2. Änderungen (Green)

1. **Request-Context zentral:** gemeinsamer `withRequestContext(runId, action)`-Helper (oder Aspekt),
   den **alle vier** Activities nutzen; garantiertes Clear im `finally`.
2. **Determinismus:** `TreeMap`/`LinkedHashMap` + explizite Sortierung in
   `context(...)`/Ausgabe-Komposition; im Test `WorkflowReplayer` bzw. Wiederholtest nutzen.
3. **Pruning:** transitiv über Nachfahren propagieren; gepruned Nachfahren explizit als `skipped`
   in `outputs`/Status führen → nicht still weglassen.
4. **Loop:** unset-Verhalten klar definieren (z. B. Fehler oder dokumentierter Default),
   Repository-weites `maxIterations`-Limit mit Log/Ereignis beim Erreichen.

## 3. Verifikation

```bash
cd agentflow && mvn -pl agentflow-app test -Dtest='Declarative*,AgentFlowActivities*'
```
- Temporal-Replay-Test: aufgezeichnete History wiedergeben → keine Non-Determinism-Fehler.

## 4. Risiken / Hinweise

- Aspekt-/Helper-Einführung in Activities: Temporal-Activity-Interfaces beachten (kein State
  über Replays).
- Pruning-Änderung beeinflusst, welche Knoten in `outputs` erscheinen → bestehende Workflows prüfen.

## Definition of Done

- [ ] Alle Activities setzen/bereinigen den Context (Test 1–4).
- [ ] Replay-deterministisch (Test 5).
- [ ] Pruning transitiv und sichtbar (Test 6).
- [ ] Loop-Grenzen definiert (Test 7, 8).
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Replay-Test | |
