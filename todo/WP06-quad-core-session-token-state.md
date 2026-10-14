# WP06 — quad-core: Session, Token, State

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `quad-quarkus/quad-core` |
| **Review-Referenz** | B15, B16, B21 (§5.3, §5.2) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 1–2 Tage |

## Ziel

Token-Austausch liefert den richtigen Token, Session-Snapshots verlieren keine Daten, und die
Conversation-Manager-Logik folgt nicht mehr einem toten `==`-Zweig.

## Befunde (Ist-Zustand)

- **B15 – Token-Cache-Fehler (sicherheitsrelevant):** `TokenExchangeClient` cacht nur nach
  `audience`, ignoriert `subjectToken`/`scopes` (`TokenExchangeClient.java:71`) → **falscher Token**
  bei gleicher Audience.
- **B16 – Snapshot verliert Summary:** `SessionStateSnapshot.toState()` lässt
  `MemorySnapshot.summary` weg (`:103-106`).
- **B21 – Referenzvergleich:** `SummarizingSlidingWindowConversationManager` vergleicht
  `toKeep == nonSystemMsgs` (`subList`-View) → nie wahr, falscher Pfad (`:70`).

## 1. Tests zuerst (Red)

Testort: `quad-quarkus/quad-core/src/test/java/de/augmentia/quad/core/…`

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `TokenExchangeClientTest#differentSubject_differentToken` | anderer `subjectToken` → anderer (nicht gecachter) Token |
| 2 | `TokenExchangeClientTest#differentScopes_differentToken` | andere `scopes` → eigener Cache-Eintrag |
| 3 | `TokenExchangeClientTest#sameKey_hitsCache` | identische Eingaben → Cache-Treffer |
| 4 | `SessionStateSnapshotTest#roundTrip_preservesSummary` | `toState`→`from` enthält Summary |
| 5 | `SummarizingSlidingWindowConversationManagerTest#keepsNonSystemMessages` | korrekte Auswahl der Nachrichten (kein toter Zweig) |

Testorte ggf. neu: `security/TokenExchangeClientTest.java`, `session/SessionStateSnapshotTest.java`,
`session/memory/SummarizingSlidingWindowConversationManagerTest.java`.

**Abnahme (Red):** 1, 2, 4, 5 fehlschlagen.

## 2. Änderungen (Green)

1. **Cache-Key:** zusammengesetzter Key aus `audience + subjectToken(+Hash) + scopes` (immutable
   Value-Object als Key, `equals/hashCode` korrekt). Sensible Token nicht im Klartext als Key-Logging.
2. **Snapshot:** `summary` mit serialisieren/deserialisieren (inkl. Null-/Leerfall).
3. **Conversation-Manager:** Bedingung auf inhaltliche Prüfung umstellen (z. B. `isEmpty`/`size`,
   Vergleich mit dem Original-Listenobjekt statt `subList`-View); toten Zweig entfernen.

## 3. Verifikation

```bash
cd quad-quarkus && mvn -pl quad-core test
cd quad-quarkus && mvn -pl quad-core test -Dtest='*TokenExchange*,*SessionState*,*ConversationManager*'
```

## 4. Risiken / Hinweise

- Cache-Key enthält Token → nicht loggen, ggf. Hash verwenden.
- Snapshot-Format-Änderung: Rückwärtskompatibilität zu alten Snapshots prüfen (Versionierung).

## Definition of Done

- [ ] Tests 1–5 grün.
- [ ] Token-Cache keyt korrekt (kein Cross-Subject-Leak).
- [ ] Summary überlebt Round-Trip.
- [ ] Kein toter Referenzvergleich mehr.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
