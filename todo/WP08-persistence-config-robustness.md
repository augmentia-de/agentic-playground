# WP08 — Persistenz & Config-Robustheit

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `quad-quarkus/quad-quarkus` |
| **Review-Referenz** | B26, B27, B35, B36, §5.4 (JournalService) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 2 Tage |

## Ziel

Persistenz übersteht einzelne kaputte Datensätze und verliert keine Werte; nach außen werden keine
erfundenen Metriken/Ergebnisse gemeldet; JSON wird nicht per Substring geparst.

## Befunde (Ist-Zustand)

- **B26 – Ladung bricht bei einer Zeile ab:** `PersistenceStore.loadAllWorkflows`/`loadAllAgents`
  kapseln die gesamte `while (rs.next())`-Schleife in ein `try/catch` (`:94-192`) → eine defekte
  Zeile (`:175-178`) verwirft alle weiteren.
- **B27 – CSV-Arrays verlustbehaftet:** Arrays werden komma-verkettet (`:64-67`), `splitArr`
  (`:308-311`) kann Werte mit Komma nicht rekonstruieren; gleiches Muster in `security/AuditStore`.
- **B35 – erfundene Metriken / fragiler Parser:** `SystemController.getMetrics`/`getErrorMetrics`
  liefern Fake-Zahlen (`:159-166`); JSON-Parsing per `indexOf`/`substring` (`:277-292`).
- **B36 – Platzhalter-Ergebnis:** `AutomationService.lastAssistantText` liefert
  `"[session-… transcript]"` und persistiert das als Ergebnis (`:174`, genutzt `:143`).
- **§5.4 – `JournalService`-Default** `${QUAD_JOURNAL_PATH:logs/journal.jsonl}` wird nicht expandiert
  (`:26`).

## 1. Tests zuerst (Red)

Testort: `quad-quarkus/quad-quarkus/src/test/java/de/augmentia/quad/quarkus/…`
(vorhanden: `persistence/RunStoreTest`, `persistence/AgentStepExporterTest`,
`persistence/TelemetryStoreTest`).

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `PersistenceStoreTest#oneBadRow_othersStillLoad` | defekte Zeile übersprungen, Rest geladen |
| 2 | `PersistenceStoreTest#arrayWithComma_roundTrips` | Wert mit Komma bleibt erhalten |
| 3 | `PersistenceStoreTest#arrayEmptyAndSpecialChars` | `[]`, `"a,b"`, Sonderzeichen korrekt |
| 4 | `SystemControllerTest#metrics_areReal_orMarkedStub` | echte Zahlen oder explizit als Stub |
| 5 | `SystemControllerTest#extract_nestedJson` | verschachteltes JSON korrekt geparst |
| 6 | `AutomationServiceTest#resultText_isRealOrEmpty` | echtes Ergebnis oder leer, kein Platzhaltertext |
| 7 | `JournalServiceTest#defaultPathIsExpanded` | Default-Pfad ist ein echter Pfad |

**Abnahme (Red):** 1, 2, 4, 6, 7 fehlschlagen.

## 2. Änderungen (Green)

1. **`loadAll*`:** Zeilen-Schleife resilient machen (Fehler pro Zeile loggen + `continue`); DB-Zugriff
   weiterhin try-with-resources.
2. **Arrays:** JSON-Spalte statt CSV (oder ein Escaping-Format); Migration alter CSV-Werte tolerieren.
3. **`SystemController`:** echte Metriken aus vorhandenen Stores ODER klarer `stub`-Hinweis
   (Feld `"stub": true`); `extract` durch `ObjectMapper` (`JsonNode`) ersetzen.
4. **`AutomationService`:** Platzhalter durch echtes Ergebnis oder leeren Wert ersetzen.
5. **Config:** `${QUAD_JOURNAL_PATH:...}` in einen in Java ausgewerteten Default umwandeln (oder
   Property-Registrierung), sodass der Literal-Platzhalter nie als Pfad dient.

## 3. Verifikation

```bash
cd quad-quarkus && mvn -pl quad-quarkus test
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='*Persistence*,SystemController*,Automation*'
```
- DB mit manuell korruptem `nodes`-JSON befüllen → App startet mit restlichem Inventar.

## 4. Risiken / Hinweise

- Schema-/Formatwechsel bei Arrays → Upcaster/Migration vorsehen.
- Fake-Metriken könnten von der UI erwartet werden → UI-Anpassung mitplanen (siehe WP12).

## Definition of Done

- [ ] Tests 1–7 grün.
- [ ] Robuste Wiederherstellung bei Einzelzeilenfehlern.
- [ ] Verlustfreie Array-Persistenz.
- [ ] Keine Fake-Metriken/Platzhalterergebnisse nach außen.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
