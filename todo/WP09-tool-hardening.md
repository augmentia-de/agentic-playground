# WP09 — Werkzeuge härten

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `quad-quarkus/quad-tool-builtin`, `quad-quarkus/quad-workflow-*` (Tool-Klassen) |
| **Review-Referenz** | B31, B32, B33, B34 (§5.3) |
| **Abhängigkeit** | WP01 (Containment-Helfer) |
| **Geschätzter Aufwand** | 2 Tage |

## Ziel

Datei-/Suchwerkzeuge sind robust gegen DoS, verlieren keine Fehler, brechen Dateien nicht halb
um, und erfüllen ihre dokumentierten Garantien (z. B. „atomic").

## Befunde (Ist-Zustand)

- **B31 – GrepTool:** Pattern ohne Timeout (ReDoS), `readAllLines` je Datei, naive Include-Glob
  (`GrepTool.java:71,84,95`).
- **B32 – FindTool:** leerer `catch {}`, sammelt ganzen Baum in-memory (`:79,88-94`).
- **B33 – MultiEditTool:** beschreibt „atomic", schreibt aber inkrementell; Teilzustand bei Fehler
  (`MultiEditTool.java:36,81`).
- **B34 – ApplyPatchTool:** fragil; `-` entfernt ersten `indexOf`, IOExceptions verschluckt, ADD
  überschreibt (`ApplyPatchTool.java:55-133`).

## 1. Tests zuerst (Red)

Testorte je Tool (neu, falls nicht vorhanden):
`quad-quarkus/quad-tool-builtin/src/test/java/de/augmentia/quad/core/tool/…`

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `GrepToolTest#catastrophicPattern_timesOut` | bösartiges Pattern → Timeout, kein Hängen |
| 2 | `GrepToolTest#binaryFiles_skipped` | `.jar`/`.class`/`.so` werden übersprungen |
| 3 | `GrepToolTest#includeGlob_escapesRegexMeta` | `*.txt` matcht nicht `xtxt` etc. |
| 4 | `FindToolTest#unreadableDir_logsAndContinues` | Fehler wird gemeldet, nicht verschluckt |
| 5 | `MultiEditToolTest#failure_leavesNoPartialEdits` | Fehler → keine Teiländerung (oder klare Doku „best effort") |
| 6 | `ApplyPatchToolTest#removalUsesContext` | `-` mit Kontext entfernt die richtige Stelle |
| 7 | `ApplyPatchToolTest#addRefusesExisting` | ADD auf existierende Datei → Fehler |
| 8 | `ApplyPatchToolTest#ioErrorPropagated` | IOException wird propagiert |

**Abnahme (Red):** 1–8 fehlschlagen bzw. legen Fehlverhalten offen.

## 2. Änderungen (Green)

1. **GrepTool:** Such-Timeout/limitiertes Matching (Zeilen-/Byte-Limit), Binärerkennung erweitern,
   Include-Glob korrekt escapen (`Pattern.quote` für Literale).
2. **FindTool:** `catch` loggen; Ergebnis-Stream/`limit` statt vollem In-Memory-Baum.
3. **MultiEditTool:** entweder echte Atomarität (erst alle validieren, dann schreiben; Backup/Rollback)
   oder Doku/Name korrigieren („best effort").
4. **ApplyPatchTool:** Parser robust machen (Kontext berücksichtigen, Konflikte melden), Overflow/Edge
   Cases prüfen, I/O-Fehler propagieren, ADD-Kollision verweigern.
5. **Path-Sicherheit:** alle Dateizugriffe über den Containment-Helfer aus WP01.

## 3. Verifikation

```bash
cd quad-quarkus && mvn -pl quad-tool-builtin test
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='*Tool*'
```
- Fuzz-/Property-Tests für den Patch-Parser in Erwägung ziehen.

## 4. Risiken / Hinweise

- Verhaltensänderung an MultiEdit/ApplyPatch kann Agentenprompts betreffen → Tool-Beschreibungen
  mitziehen.
- Timeout-Wert konfigurierbar machen.

## Definition of Done

- [ ] Tests 1–8 grün.
- [ ] Kein ReDoS/kein Hängen bei Grep.
- [ ] Fehler werden nicht mehr verschluckt.
- [ ] Tool-Garantien stimmen mit der Implementierung überein.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
