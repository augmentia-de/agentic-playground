# WP02 — Security: auth, secrets, compliance

| | |
|---|---|
| **Status** | done |
| **Severity** | High |
| **Modules** | `kafka-platform/agent-orchestrator`, `agentflow/agentflow-ui` |
| **Review reference** | B7, B8, B10, B13, B14, C3 (§5.2, §5.1, §8.6) |
| **Depends on** | – |
| **Estimated effort** | 2–3 days |

## Goal

The Vault integration works correctly (or is removed), the JWT filter no longer masks server errors,
HITL callbacks are authenticated, the GDPR endpoints are either real or honestly marked as a stub,
and the UI API key is not stored in `localStorage` where it can be read by XSS.

## Findings (as-is)

- **B7 – `SecretResolver.isConnected()` inverted:** `initialized && sealed` (`SecretResolver.java:191-192`).
  A healthy (unsealed) Vault reported "not connected"; a sealed one reported "connected".
- **B8 – `@Value` fields read before injection:** `SecretResolver` built the `WebClient` in the
  constructor from still-`null` `vaultUrl`/`authToken` fields (`:58-78`); `SchemaRegistryClient`
  did the same (`:33`), and its `registerAll()` was only a log no-op (`:36-38`).
- **B8+ (found while testing) – the Vault client could not have worked even with correct fields:**
  1. **Jackson 3 codec vs Jackson 2 `JsonNode`.** The code did `bodyToMono(com.fasterxml.jackson
     .databind.JsonNode.class)`. On Spring Boot 4 the WebFlux codecs are Jackson 3
     (`tools.jackson`), which cannot construct the Jackson 2 `JsonNode` → every call failed with
     `Type definition error: [simple type, class com.fasterxml.jackson.databind.JsonNode]`.
  2. **`%2F` in the Vault path.** `.uri("/{path}", pathPrefix + "/" + secretPath)` encoded the
     slashes of the URI *variable*, so Vault was asked for
     `/secret%2Fdata%2Fagent-platform%2Fdb` instead of `/secret/data/agent-platform/db`.
- **B10 – JWT filter masks 500s as 401:** `chain.doFilter` sat inside the `try` whose `catch
  (Exception)` wrote a 401 (`JwtAuthenticationFilter.java:91-97`), so any downstream exception was
  rewritten as "unauthorized". The filter also ran (and rejected bad tokens) regardless of
  `app.security.enabled`.
- **B13 – GDPR dummies:** `deletePersonalData`/`anonymizePersonalData` returned `200` with a success
  message but deleted/anonymised nothing (`GdprController.java`).
- **B14 – Slack HITL unauthenticated:** `HitlPortalController.slackCallback` verified no Slack
  signing secret, so anyone could approve/reject HITL tickets.
- **C3 – UI secret in `localStorage`:** `tenant.interceptor.ts:8-9`, `pages/settings.ts:64-65` stored
  the API key in `localStorage`, readable by any XSS.

## 1. Tests first (Red)

| # | Test | Expectation |
|---|------|-------------|
| 1 | `SecretResolverTest#isConnectedFalseWhenSealed` | sealed → `false` (was `true`) |
| 2 | `SecretResolverTest#isConnectedTrueWhenInitializedAndUnsealed` | initialized + unsealed → `true` (was `false`) |
| 3 | `SecretResolverTest#webClientUsesConfiguredUrlAndToken` | request URL = configured base URL, `X-Vault-Token` header set (was `null` base URL) |
| 4 | `SchemaRegistryClientTest#listSubjectsUsesConfiguredBaseUrl` | `registerAll()` removed; the real `listSubjects()` uses the configured base URL |
| 5 | `JwtAuthenticationFilterTest#downstreamExceptionIsNotConvertedTo401` | controller throws → exception propagates, **not** 401 |
| 6 | `JwtAuthenticationFilterTest#disabledSecurityAllowsRequestWithBadToken` | `app.security.enabled=false` + bad token → no 401 |
| 7 | `HitlPortalControllerTest#missingSignatureRejected` / `#invalidSignatureRejected` | missing/invalid signature → 401, no side effect |
| 8 | `HitlPortalControllerTest#validSignatureProcessesAction` | valid signature → ticket resolved, HITL event published |
| 9 | `GdprControllerTest#deleteSessionIsNotImplemented` / `#deleteWorkflowIsNotImplemented` | `501 Not Implemented`, no false `200` |
| 10 | `tenant.interceptor.spec.ts#doesNotPersistApiKeyToLocalStorage` | key absent from `localStorage`, present in request header from memory |

Additional coverage added along the way:

- `SecretResolverTest`: `missingKeyReturnsNull`, `resolveAllParsesKeys`,
  `isConnectedFalseWhenNotInitialized`, `isConnectedFalseOnError`, `refreshAllClearsTheCache`.
- `SchemaRegistryClientTest#listSubjectsHandlesError`.
- `JwtAuthenticationFilterTest`: `enabledSecurityRejectsBadToken`, `validTokenPopulatesContext`,
  `publicPathSkipsValidation`.
- `SlackSignatureVerifierTest` (5): valid, wrong signature, tampered body, replay window, missing parts.
- `GdprControllerTest`: reject paths for delete/anonymize/export.
- `tenant.interceptor.spec.ts`: headers-from-store, no-headers-when-empty.

Test locations:
- `kafka-platform/agent-orchestrator/src/test/java/de/augmentia/agentplatform/…`
- `agentflow/agentflow-ui/src/app/core/tenant.interceptor.spec.ts` (Karma/Jasmine, ChromeHeadless).

## 2. Changes (Green)

1. **`SecretResolver`:** `isConnected()` → `initialized && !sealed`; all `@Value` fields replaced by
   constructor parameters so the `WebClient` (base URL + `X-Vault-Token`) is built **after**
   injection. The token now defaults to empty (`${app.vault.token:${VAULT_TOKEN:}}`) so a missing
   token no longer aborts startup. Additionally: the Vault path is now passed **literally** (URI
   variables encoded the slashes to `%2F`), and the response is read as text and parsed with an
   explicit Jackson 2 `ObjectMapper` (the WebFlux codecs are Jackson 3 and cannot build a Jackson 2
   `JsonNode`).
2. **`SchemaRegistryClient`:** constructor injection; the log-only `registerAll()` and its dead
   `compatibility-mode`/`auto-register` knobs were removed. Only the real `listSubjects()` remains
   (full class removal deferred to WP13).
3. **`JwtAuthenticationFilter`:** `chain.doFilter` moved **outside** the `try`, so downstream
   exceptions propagate. Added `app.security.enabled` (default `false`): when disabled, an invalid
   token is ignored (no 401) and the request continues; when enabled, an invalid token is a 401.
   The secret property gained an empty default. `TenantContext.clear()` on ignored tokens.
4. **`SlackSignatureVerifier` (new):** HMAC-SHA256 over `v0:{timestamp}:{body}`, constant-time
   compare, 5-minute replay window, fail-closed on any missing part. `HitlPortalController
   .slackCallback` now takes the **raw** body plus `X-Slack-Signature`/`X-Slack-Request-Timestamp`
   and rejects with 401 before any side effect. The Slack path is `permitAll()` in `SecurityConfig`
   (it authenticates via signature, and cannot present a JWT).
5. **`GdprController`:** delete/anonymise now return `501 Not Implemented` with a clear body; the
   fake `200` paths are gone. `workflowId` was added as an optional delete identifier to match the
   documented API.
6. **`agentflow-ui`:** new in-memory `ClientHeaderStore` (signals) replacing `localStorage` in
   `tenant.interceptor.ts` and `pages/settings.ts`. Values live only for the session; the UI text
   now says so.

## 3. Verification

```bash
mvn -pl kafka-platform/agent-orchestrator -am test        # backend
cd agentflow/agentflow-ui && npx ng test --watch=false --browsers=ChromeHeadless   # frontend
```

- Backend: **BUILD SUCCESS** — `agent-common` + `agent-orchestrator`; module total **249 tests,
  0 failures, 4 skipped**. New suites: `SecretResolverTest` 8, `JwtAuthenticationFilterTest` 5,
  `SlackSignatureVerifierTest` 5, `HitlPortalControllerTest` 4, `GdprControllerTest` 8,
  `SchemaRegistryClientTest` 2.
- Frontend: **3 specs SUCCESS** (`tenant.interceptor.spec.ts`); `npx ng build` green (full app
  type-check).

## 4. Risks / notes

- **Slack signature is fail-closed:** with no `app.hitl.slack.signing-secret` configured, all Slack
  callbacks are rejected. Operators must set the secret (add to config/docs).
- **GDPR deletion** touches several stores (execution, checkpoints, HITL tickets, DLQ) and needs a
  transactional strategy + FK handling; the honest `501` is deliberate. The export path is unchanged
  (real for `workflowId`).
- **Vault token** no longer fails startup when absent; a bad/absent token now fails at first use with
  a clear log line instead of a null-base-URL request.

## Definition of Done

- [x] Tests 1–10 green.
- [x] No inverted health/status logic remains.
- [x] JWT filter does not mask 500s; the security toggle takes effect.
- [x] Slack callback signature-verified.
- [x] GDPR endpoints real **or** explicitly 501/documented.
- [x] No API key in `localStorage`.
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-14 | @opencode | 1. Tests red | 6 new backend test classes (19 + 20 assertions), 1 frontend spec; the failing `SecretResolverTest` exposed two extra latent bugs (Jackson 3 codec vs Jackson 2 `JsonNode`, `%2F` path encoding) |
| 2026-10-14 | @opencode | 2. Implementation | `SecretResolver`, `SchemaRegistryClient`, `JwtAuthenticationFilter`, `SlackSignatureVerifier`+`HitlPortalController`, `SecurityConfig`, `GdprController`, `application.yml`, `ClientHeaderStore`+interceptor+settings |
| 2026-10-14 | @opencode | 3. Verification | backend `mvn -pl kafka-platform/agent-orchestrator -am test` → BUILD SUCCESS (249 tests, 0 failures); frontend `ng test` 3 SUCCESS + `ng build` green |
