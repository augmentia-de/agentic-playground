# WP15 — Integrationstests härten

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `integration-tests` |
| **Review-Referenz** | §8.5, §B (README-Drift), §3.1 |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 1–2 Tage |

## Ziel

Die Integrationstests sind **flakiness-arm**, ihre README stimmt mit dem Code überein, und die
`@Disabled`-Befunde sind sichtbar dokumentiert.

## Befunde (Ist-Zustand)

- **README-Drift:** README sagt „58 Tests / 2 documented findings" (`:10,84-90`); real **104 `@Test`**
  und **3 `@Disabled`**-Befunde; die komplette **`syscapability`-Suite (46 Tests)** fehlt in der
  README-Tabelle.
- **Flakiness:** `support/WorkflowRun.java` pollt mit `Thread.sleep(400)`; `support/TestEnv.java`
  hat hartkodierte Ports; mögliche ungenutzte `uniqueKey`-Map.
- **Duplikation:** `stackMussLaufen`-Helfer 5–6× in `syscapability/*` kopiert.
- **Schwache Assertion:** `LastSystemtest.rueckstandIstSichtbar` prüft nur `isNotNegative()`.

## 1. Tests zuerst

Hier sind die Integrationstests selbst der Gegenstand.

| # | Aufgabe | Erwartung |
|---|---------|-----------|
| 1 | Zähl-Skript `scripts/count-tests.sh` | listet `@Test`/`@Disabled` pro Layer; README daraus generierbar |
| 2 | Awaitility statt `Thread.sleep` | deterministischere Wartezeiten |
| 3 | Ports aus `TestEnv`/Env statt hartkodiert | Suite in abweichender Umgebung lauffähig |
| 4 | `stackMussLaufen` in `support/` zentralisieren | eine Implementierung |
| 5 | `rueckstandIstSichtbar` schärfen | prüft echten Backlog-Wert |
| 6 | README-Tabelle regenerieren | enthält `syscapability`, 3 Befunde |

## 2. Änderungen (Green)

1. **README angleichen** (Zahlen + `syscapability`-Suite + 3 Befunde). Idealerweise aus
   `count-tests.sh` generieren (Bezug zu WP14-CI).
2. **`WorkflowRun`** auf Awaitility (`await().atMost(...).pollInterval(...)`) umstellen
   (Dependency ergänzen, falls nicht vorhanden).
3. **`TestEnv`:** Ports konfigurierbar machen; toten `uniqueKey` entfernen.
4. **Helper zentralisieren:** gemeinsame Basisklasse/Utility für Stack-Bereitschaft.
5. **Assertion schärfen** (echter Erwartungswert).
6. **Befund-Muster dokumentieren:** `@Disabled`-Befund + Begleittest als Hausstil in die README.

## 3. Verifikation

```bash
cd integration-tests && ./scripts/run-integration-tests.sh e2e
./scripts/count-tests.sh
```
- Suite mehrfach hintereinander laufen lassen → stabil grün (keine Sleep-Flakiness).
- In anderer Umgebung (abweichende Ports) → grün.

## 4. Risiken / Hinweise

- Awaitility-Umstellung ändert Timeouts → großzügig, aber mit klarer Fehlermeldung.
- `syscapability`-Suite braucht ggf. laufenden Stack (dokumentieren, welche Layer skippen).

## Definition of Done

- [ ] README == Realität (Zahlen, `syscapability`, 3 Befunde).
- [ ] Kein `Thread.sleep`-Polling mehr; Ports konfigurierbar.
- [ ] `stackMussLaufen` dedupliziert.
- [ ] Assertion geschärft.
- [ ] Zähl-Skript vorhanden (CI-tauglich).
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Zähl-Skript | |
| | | 2. Awaitility/Ports | |
| | | 3. README regeneriert | |
