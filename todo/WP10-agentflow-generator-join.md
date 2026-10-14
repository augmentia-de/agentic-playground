# WP10 — agentflow: Generator konsolidieren & Join

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Hoch |
| **Module** | `agentflow/agentflow-app`, `quad-quarkus/quad-quarkus` |
| **Review-Referenz** | A1, B6, B11 (§3.2, §5.2) |
| **Abhängigkeit** | – (Testorte von WP01 unabhängig) |
| **Geschätzter Aufwand** | 3–4 Tage |

## Ziel

Die zwei `WorkflowGeneratorService`-Implementierungen werden **vereinheitlicht** (kein divergierender
Copy-Paste-Zweig). Der agentflow-Generator erzeugt bei Fork/Parallel wieder **Join-Nodes**, und der
Plan wird nicht mehr durch fehlendes Token-Limit abgeschnitten.

## Befunde (Ist-Zustand)

- **A1/B6 – divergierte Kopie:** `agentflow/agentflow-app/src/main/java/de/augmentia/agentflow/app/workflow/generate/WorkflowGeneratorService.java`
  ist eine near-verbatim-Kopie von
  `quad-quarkus/quad-quarkus/src/main/java/de/augmentia/quad/quarkus/workflow/WorkflowGeneratorService.java`
  (identische private Methodennamen), aber **17 vs. 29** private Methoden und **keine** Join-Logik
  (`grep 'join'` → 0 Treffer vs. 11 im Original, u. a. `:465 addJoinNodes(...)`, `:548`).
  Folge: parallele Forks ohne Merge.
- **B11 – Plan ohne Token-Limit:** `max-output-tokens: 0` (`application.yml:50`, Default `:0` in `:110`);
  `chatParametersJson` lässt das Feld weg (`:183-184`) → Provider kappt lange Pläne → `parseSteps`
  scheitert. Das Quad-Original defaultet bewusst groß (`:249-254`).

## 1. Tests zuerst (Red)

Testorte (beide vorhanden):
- `quad-quarkus/quad-quarkus/src/test/java/de/augmentia/quad/quarkus/workflow/WorkflowGeneratorJoinTest.java`
- `agentflow/agentflow-app/src/test/java/de/augmentia/agentflow/app/workflow/generate/WorkflowGeneratorServiceTest.java`

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `WorkflowGeneratorServiceTest#forkMerge_containsJoinNode` (agentflow) | Fork→Merge-Plan enthält Join-Node |
| 2 | `WorkflowGeneratorJoinTest#sameInput_sameNodeSet` | Beide Generatoren liefern identische Node-Menge (Paritäts-Test) |
| 3 | `WorkflowGeneratorServiceTest#parallelBranches_downstreamResolves` | Downstream-`dependsOn` löst auf |
| 4 | `WorkflowGeneratorServiceTest#truncatedPlan_actionableError` | Abschneiden → klarer Fehler (nicht Parserfehler) |
| 5 | `WorkflowGeneratorServiceTest#defaultTokenLimit_applied` | Default > 0, im Request sichtbar |
| 6 | `WorkflowGeneratorParsingTest#cyclicPlan_rejected` | Zyklus erkannt (Bestand/Parität) |

**Abnahme (Red):** 1, 2, 3, 5 fehlschlagen.

## 2. Änderungen (Green)

1. **Konsolidieren (bevorzugt):** gemeinsamen Generator in ein wiederverwendbares Modul ziehen
   (z. B. `quad-workflow-core` oder neues `quad-workflow-generate`), das beide Apps nutzen.
   Fallback, falls Konsolidierung zu groß: Join-Logik exakt portieren **und** Paritäts-Test (2) als
   Wächter gegen erneute Divergenz.
2. **Join-Logik:** `addJoinNodes`/`uniqueJoinId`/`addJoinNode` aus dem Original übernehmen
   (`:465,548-601`), inkl. Timeout-Ableitung.
3. **Token-Limit:** Default auf sinnvollen Wert (z. B. 32768) setzen; `application.yml:50` anpassen;
   bei `finish_reason == length` klaren Fehler werfen.
4. **Paritäts-/Regressions-Test** dauerhaft aufnehmen, damit die Kopien nicht wieder auseinanderlaufen.

## 3. Verifikation

```bash
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='WorkflowGenerator*'
cd agentflow && mvn -pl agentflow-app test -Dtest='WorkflowGenerator*'
```
- Manuell: Fork-Prompt generieren, erzeugten Graphen im Editor prüfen (Join vorhanden).
- End-to-End: agentflow generiert → ausführt (falls UI/Engine verfügbar).

## 4. Risiken / Hinweise

- Konsolidierung berührt Build-Reihenfolge/POMs → Root-Reaktor-Test (`mvn -T1C clean install`).
- Prompt-Texte unterscheiden sich evtl. → beim Zusammenführen bewusst wählen und dokumentieren.

## Definition of Done

- [ ] Kein divergierender Generator mehr (gemeinsame Quelle **oder** Paritäts-Wächter grün).
- [ ] Fork→Merge erzeugt Join (Test 1, 3).
- [ ] Kein Token-Abschneiden ohne klaren Fehler (Test 4, 5).
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Entscheidung Konsolidierung vs. Port | |
| | | 2. Tests rot | |
| | | 3. Umsetzung | |
| | | 4. Verifikation | |
