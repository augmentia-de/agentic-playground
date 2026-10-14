# WP14 — Doku konsolidieren & CI

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `kafka-platform` (Docs), Root, `.github` |
| **Review-Referenz** | A3, A4, §9 |
| **Abhängigkeit** | – (Doku-Teile nach WP13 spiegeln) |
| **Geschätzter Aufwand** | 2 Tage |

## Ziel

Dokumentation beschreibt den **Ist-Zustand**; Testzahlen kommen aus der CI, nicht aus Prosa.
Das Monorepo wird **ganzheitlich** gebaut/getestet.

## Befunde (Ist-Zustand)

- **A4 – kein monorepo-weites CI:** Einzige CI ist `kafka-platform/.github/workflows/ci.yml`
  (nur `kafka-platform`). `quad-quarkus` und `agentflow` haben gar keine.
- **§9 – Doku-Drift:** sechs Top-Level-Docs in `kafka-platform` nennen nicht existente Pakete
  (`com.agentplatform.*`, `com.platform.*`), Module und Testzahlen; `spec.md` ist ein KI-Artefakt
  (Traceback, doppelte Spec); `IMPLEMENTATION_COMPLETE.md` nennt `TracingProducerInterceptor.java`
  (existiert nicht), Testzahlen 41/84; `IMPL_STATUS.md` behauptet E-Mail-Responder/Vault/Alerting
  „implemented"; `README.md` sagt „359", `TODO.md` „270".
- **A3 – Root-Aggregator ohne Parent** (`pom.xml:3-19`) → keine zentrale Dependency-/Plugin-Verwaltung.
- **`TODO2.md`** (untracked, 918 Z.) ist ein realer Issue-Tracker außerhalb der Versionskontrolle.

## 1. Tests/Validierung zuerst

Es gibt hier keine Unit-Tests im engen Sinn — „Tests" sind **Doku-Checks**:

| # | Check | Erwartung |
|---|-------|-----------|
| 1 | `ci.yml` läuft für `quad-quarkus` + `agentflow` + Root | grün bei PR |
| 2 | Testzahlen im README == CI-Report | aus CI generiert/verlinkt |
| 3 | Paketnamen in Docs == `grep '^package '`-Realität | keine `com.agentplatform`/`com.platform` mehr |
| 4 | genannte Dateien/Klassen existieren | `TracingProducerInterceptor` existiert nicht → entfernen |
| 5 | `spec.md` entfernt/ersetzt | kein KI-Artefakt im Repo |

## 2. Änderungen (Green)

1. **CI ausbauen:** Workflows für `quad-quarkus` (`mvn -pl … test`), `agentflow` (`mvn -pl agentflow-app test`)
   und einen Root-Job (`mvn -T1C clean install`) ergänzen; Testzahlen in `$GITHUB_STEP_SUMMARY`.
2. **Doku konsolidieren:** ein kanonisches Statusdokument pro Modul; überflüssige/widersprüchliche
   entfernen (`IMPLEMENTATION_COMPLETE.md` vs. `IMPL_STATUS.md` zusammenführen).
   `spec.md` löschen (oder in `docs/history/` verschieben).
3. **Paket-/Datei-Referenzen korrigieren**; Testzahlen durch CI-verlinkte Angabe ersetzen.
4. **Claims mit Code abgleichen** (mit WP13): „implementiert" nur, wenn erreichbar.
5. **`TODO2.md` → getrackte Issues** (GitHub Issues oder `todo/`-WPs) überführen; die wichtigsten
   Befunde (leerer Workflow akzeptiert, KafkaTopicInitializer create-only, Nebenläufigkeit) sind
   bereits als `@Disabled`-Tests erfasst.
6. **Root-Parent (A3, optional):** prüfen, ob ein `dependencyManagement`/BOM im Root ohne Aufgeben
   der Modul-Eltern möglich ist — als Vorschlag dokumentieren, nicht überstürzt umbauen.

## 3. Verifikation

- CI-PR grün für alle drei Reaktoren.
- `grep -rn 'com\.agentplatform\|com\.platform' kafka-platform/*.md` → keine Treffer.
- `grep -rn 'TracingProducerInterceptor'` → keine Treffer (außer Changelog).

## 4. Risiken / Hinweise

- Doku-Änderungen sind umfangreich → in kleineren PRs pro Datei, damit Reviews möglich bleiben.
- CI-Laufzeit: Kafka-Tests brauchen Docker (siehe bestehende `ci.yml`).

## Definition of Done

- [ ] CI für alle Reaktoren vorhanden und grün.
- [ ] Testzahlen CI-generiert.
- [ ] Keine falschen Paket-/Datei-Referenzen.
- [ ] `spec.md` entfernt; Statusdoks konsolidiert.
- [ ] `TODO2.md`-Befunde getrackt.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. CI erweitert | |
| | | 2. Docs korrigiert | |
| | | 3. TODO2 überführt | |
