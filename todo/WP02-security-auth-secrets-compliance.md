# WP02 — Sicherheit: Auth, Secrets, Compliance

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Hoch |
| **Module** | `kafka-platform/agent-orchestrator`, `kafka-platform/quad-workflow-worker`, `agentflow/agentflow-ui` |
| **Review-Referenz** | B7, B8, B10, B13, B14, C3 (§5.2, §5.1, §8.6) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 2–3 Tage |

## Ziel

Vault-Anbindung funktioniert korrekt (oder wird entfernt), der JWT-Filter maskiert keine
Serverfehler mehr, HITL-Callbacks sind authentifiziert, GDPR-Endpunkte sind entweder echt oder
ehrlich als Stub markiert, und der UI-API-Key liegt nicht XSS-lesbar im `localStorage`.

## Befunde (Ist-Zustand)

- **B7 – `SecretResolver.isConnected()` invertiert:** `initialized && sealed` (`SecretResolver.java:191-192`).
- **B8 – `@Value` im Konstruktor:** `SecretResolver` baut `WebClient` aus noch-null Feldern (`:58-78`);
  `SchemaRegistryClient` gleich (`:33`), dessen `registerAll()` zusätzlich nur ein Log-No-op ist (`:36-38`).
- **B10 – JWT-Filter maskiert 500er als 401:** `chain.doFilter` innerhalb des `try`, dessen
  `catch (Exception)` 401 schreibt (`JwtAuthenticationFilter.java:91-97`); Filter nicht an
  `app.security.enabled` gekoppelt.
- **B13 – GDPR-Attrappen:** `deletePersonalData`/`anonymizePersonalData` melden Erfolg, löschen nichts
  (`GdprController.java:87-113,121-136`; `// TODO` `:105`).
- **B14 – Slack-HITL unauthentifiziert:** `HitlPortalController.slackCallback` (`:240-282`) prüft kein
  Signing-Secret.
- **C3 – UI-Secret in `localStorage`:** `tenant.interceptor.ts:8-9`, `pages/settings.ts:64-65`.

## 1. Tests zuerst (Red)

| # | Test (neu/erweitert) | Erwartung |
|---|----------------------|-----------|
| 1 | `SecretResolverTest#isConnected_falseWhenSealed` | sealed → `false` |
| 2 | `SecretResolverTest#isConnected_trueWhenInitializedAndUnsealed` | initialized, unsealed → `true` |
| 3 | `SecretResolverTest#webClientUsesConfiguredUrlAndToken` | konstruiert mit korrekter baseUrl + Header |
| 4 | `SchemaRegistryClientTest#registerAll_actuallyRegisters_orRemoved` | entweder echte Registrierung oder Klasse entfernt |
| 5 | `JwtAuthenticationFilterTest#downstreamException_isNotConvertedTo401` | Controller wirft → 500, nicht 401 |
| 6 | `JwtAuthenticationFilterTest#disabledSecurity_allowsRequest` | `app.security.enabled=false` → kein 401 |
| 7 | `HitlPortalControllerTest#slackCallback_rejectsMissingSignature` | fehlende/ungültige Signatur → 401/403 |
| 8 | `HitlPortalControllerTest#slackCallback_acceptsValidSignature` | gültig → verarbeitet |
| 9 | `GdprControllerTest#delete_actuallyRemoves_orReturns501` | echte Löschung **oder** ehrlicher Stub |
| 10 | Frontend: `tenant.interceptor.spec.ts#doesNotPersistApiKeyToLocalStorage` | Key nicht in `localStorage` |

Testorte:
- `kafka-platform/agent-orchestrator/src/test/java/de/augmentia/agentplatform/…` (MockMvc für Controller).
- `agentflow/agentflow-ui/src/app/core/tenant.interceptor.spec.ts` (Karma/Jest).

**Abnahme (Red):** Tests 1–3, 5–9 fehlschlagen wie beschrieben.

## 2. Änderungen (Green)

1. **`SecretResolver`:** `isConnected()` → `initialized && !sealed`; `@Value`-Felder durch
   Konstruktor-Parameter ersetzen (`WebClient` erst nach Injizierung bauen, z. B. lazy in `@PostConstruct`).
2. **`SchemaRegistryClient`:** entweder echte Registrierung implementieren **oder** Klasse + Doc-Claims
   entfernen (siehe WP13). Kein „Log-only" vorhalten.
3. **`JwtAuthenticationFilter`:** Authentifizierung im `try`, aber `chain.doFilter` **außerhalb**;
   Downstream-Ausnahmen propagieren. Filter nur aktiv bei `app.security.enabled=true`
   (`@ConditionalOnProperty` oder früher Return).
4. **`HitlPortalController.slackCallback`:** Slack-Signing-Secret prüfen
   (`X-Slack-Signature`/`X-Slack-Request-Timestamp`, HMAC, Replay-Fenster).
5. **`GdprController`:** echte Löschung/Anonymisierung implementieren **oder** Endpunkte mit
   `501 Not Implemented` + Doku-Hinweis markieren. Kein Fake-200.
6. **`agentflow-ui`:** API-Key/Tenant nur im Speicher (Service) halten; `localStorage`-Zugriffe für
   Secrets entfernen. Falls Persistenz gewünscht: dokumentiert als bewusste Dev-Einschränkung.

## 3. Verifikation

```bash
cd kafka-platform && mvn -pl agent-orchestrator test
cd agentflow && (cd agentflow-ui && npm test)
```
- Manuell: Vault-Mock starten, `isConnected` gegen sealed/unsealed testen.
- Manuell: Request mit gültigem JWT, Controller wirft → Status 500.

## 4. Risiken / Hinweise

- Slack-Signaturprüfung braucht das Signing-Secret → Config/Doku ergänzen.
- GDPR-Löschung berührt mehrere Stores (Run/Audit/Session) → transaktionale Strategie nötig.

## Definition of Done

- [ ] Tests 1–10 grün.
- [ ] Keine invertierte Health-/Statuslogik mehr.
- [ ] JWT-Filter maskiert keine 500er; Security-Toggle wirkt.
- [ ] Slack-Callback signaturgeprüft.
- [ ] GDPR-Endpunkte echt **oder** explizit 501/dokumentiert.
- [ ] Kein API-Key in `localStorage`.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
