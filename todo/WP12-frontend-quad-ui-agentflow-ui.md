# WP12 — Frontend: quad-ui & agentflow-ui

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `quad-quarkus/quad-ui` (React), `agentflow/agentflow-ui` (Angular) |
| **Review-Referenz** | C1, C2 (§8.6), C3 (siehe WP02) |
| **Abhängigkeit** | C3 → WP02 |
| **Geschätzter Aufwand** | 3–5 Tage |

> Der Sicherheitsaspekt **C3** (API-Key in `localStorage`) wird in **WP02** behandelt.
> Dieses WP kümmert sich um Struktur & Duplikate.

## Ziel

Der `WorkflowComposer` und der Angular-Graph-Editor sind in wartbare Einheiten zerlegt, der
quad-ui-API-Client ist dedupliziert, und die Kern-UI hat angemessene Testabdeckung.

## Befunde (Ist-Zustand)

- **C1 – God-Components:** `quad-ui/src/components/WorkflowComposer.tsx` **1.669 Zeilen**,
  **26 `useState`** (`:226-565`), vier `useEffect` (`:569,580,603,621`);
  `agentflow-ui/src/app/ui/graph-editor.ts` **593 Zeilen**.
- **C2 – Duplikate:** `api.ts` `fetchSkills` (`:437`) und `fetchSkillList` (`:504`) byte-identisch;
  `createAgent`/`updateAgent` duplizieren einen 19-Feld-`flat`-Block (`:154-201`).
- **Abdeckung:** `WorkflowComposer.test.tsx` (168 Z.) ist klein relativ zur 1.669-Zeilen-Komponente.

## 1. Tests zuerst (Red/Charakterisierung)

Zuerst **Charakterisierungstests** ergänzen, die das heutige Verhalten der großen Komponente
festhalten — dann refactoren, ohne Verhalten zu ändern.

| # | Test | Zweck |
|---|------|-------|
| 1 | `WorkflowComposer.test.tsx#rendersAndEditsBaseline` | Grundverhalten fixieren (vor Refactoring) |
| 2 | `WorkflowComposer.test.tsx#forkMergeRoundTrip` | Fork→Merge durch den Composer |
| 3 | `api.test.ts#fetchSkills_equals_fetchSkillList` | Duplikate identisch (Ziel: eines entfernen) |
| 4 | `api.test.ts#agentCreateUpdate_useSharedMapper` | ein Mapper für create/update |
| 5 | `graph-editor.spec.ts#baseline` (Angular) | Grundverhalten fixieren |
| 6 | `agentflow-ui settings.spec.ts` | siehe WP02 (lokale Speicherung) |

Testorte: `quad-quarkus/quad-ui/src/components/*.test.tsx`, `quad-quarkus/quad-ui/src/services/*.test.ts`,
`agentflow/agentflow-ui/src/app/**/*.spec.ts`.

**Abnahme (Red):** Charakterisierungstests laufen grün (Ist-Zustand); Duplikat-Tests (3,4) decken
die Duplikate auf.

## 2. Änderungen (Green)

1. **Composer zerlegen:** in Teilkomponenten/-Hooks mit klaren Verantwortlichkeiten:
   Canvas, Inspector, Run-Panel, Backend-Selector, Materialisierung. Gemeinsamen Zustand in einen
   Reducer/Context ziehen (bestehender `WorkflowContext` als Basis).
2. **API-Client:** `fetchSkills` **oder** `fetchSkillList` entfernen (Aufrufer umstellen);
   `toFlatAgentRequest(agent)` für create+update extrahieren.
3. **Angular-Editor:** `graph-editor.ts` in kleinere Bausteine/Services trennen.
4. **Abdeckung:** für die zentralen Interaktionen des Composers Tests ergänzen (Ziel: die kritischen
   Pfade sind abgedeckt, nicht 100 %).

## 3. Verifikation

```bash
cd quad-quarkus/quad-ui && npm test
cd agentflow/agentflow-ui && npm test
# Builds
cd quad-quarkus/quad-ui && npm run build
cd agentflow/agentflow-ui && npm run build
```
- Manuelle Sichtprüfung: Composer-Funktionen unverändert (Fork/Merge, Agent einfügen, Run starten).

## 4. Risiken / Hinweise

- Reines Refactoring → Charakterisierungstests zuerst, Verhalten einfrieren.
- `quad-ui` nutzt Context + `useState` (kein Store): bei Zerlegung Konsistenz wahren, nicht
  über-engineeren.

## Definition of Done

- [ ] Charakterisierungstests vorhanden und grün.
- [ ] Composer in klar benannte Einheiten zerlegt (keine Datei > ~400 Z.).
- [ ] API-Duplikate entfernt; gemeinsamer Agent-Mapper.
- [ ] `graph-editor.ts` zerlegt.
- [ ] Kernpfade getestet; Builds grün.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Charakterisierungstests | |
| | | 2. Refactoring | |
| | | 3. Verifikation | |
