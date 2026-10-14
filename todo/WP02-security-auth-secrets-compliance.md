# WP02 — Security: auth, secrets, compliance

| | |
|---|---|
| **Status** | open |
| **Severity** | High |
| **Modules** | `kafka-platform/agent-orchestrator`, `kafka-platform/quad-workflow-worker`, `agentflow/agentflow-ui` |
| **Review reference** | B7, B8, B10, B13, B14, C3 (§5.2, §5.1, §8.6) |
| **Depends on** | – |
| **Estimated effort** | 2–3 days |

## Goal

The Vault integration works correctly (or is removed), the JWT filter no longer masks server errors,
HITL callbacks are authenticated, the GDPR endpoints are either real or honestly marked as a stub,
and the UI API key is not stored in `localStorage` where it can be read by XSS.

## Findings (as-is)

- **B7 – `SecretResolver.isConnected()` inverted:** `initialized && sealed` (`SecretResolver.java:191-192`).
- **B8 – `@Value` in the constructor:** `SecretResolver` builds the `WebClient` from still-null fields
  (`:58-78`); `SchemaRegistryClient` likewise (`:33`), whose `registerAll()` is additionally only a
  log no-op (`:36-38`).
- **B10 – JWT filter masks 500s as 401:** `chain.doFilter` inside the `try` whose `catch (Exception)`
  writes a 401 (`JwtAuthenticationFilter.java:91-97`); the filter is not tied to
  `app.security.enabled`.
- **B13 – GDPR dummies:** `deletePersonalData`/`anonymizePersonalData` report success but delete
  nothing (`GdprController.java:87-113,121-136`; `// TODO` `:105`).
- **B14 – Slack HITL unauthenticated:** `HitlPortalController.slackCallback` (`:240-282`) checks no
  signing secret.
- **C3 – UI secret in `localStorage`:** `tenant.interceptor.ts:8-9`, `pages/settings.ts:64-65`.

## 1. Tests first (Red)

| # | Test (new/extended) | Expectation |
|---|---------------------|-------------|
| 1 | `SecretResolverTest#isConnected_falseWhenSealed` | sealed → `false` |
| 2 | `SecretResolverTest#isConnected_trueWhenInitializedAndUnsealed` | initialized, unsealed → `true` |
| 3 | `SecretResolverTest#webClientUsesConfiguredUrlAndToken` | constructed with correct baseUrl + header |
| 4 | `SchemaRegistryClientTest#registerAll_actuallyRegisters_orRemoved` | either real registration or class removed |
| 5 | `JwtAuthenticationFilterTest#downstreamException_isNotConvertedTo401` | controller throws → 500, not 401 |
| 6 | `JwtAuthenticationFilterTest#disabledSecurity_allowsRequest` | `app.security.enabled=false` → no 401 |
| 7 | `HitlPortalControllerTest#slackCallback_rejectsMissingSignature` | missing/invalid signature → 401/403 |
| 8 | `HitlPortalControllerTest#slackCallback_acceptsValidSignature` | valid → processed |
| 9 | `GdprControllerTest#delete_actuallyRemoves_orReturns501` | real deletion **or** honest stub |
| 10 | Frontend: `tenant.interceptor.spec.ts#doesNotPersistApiKeyToLocalStorage` | key not in `localStorage` |

Test locations:
- `kafka-platform/agent-orchestrator/src/test/java/de/augmentia/agentplatform/…` (MockMvc for controllers).
- `agentflow/agentflow-ui/src/app/core/tenant.interceptor.spec.ts` (Karma/Jest).

**Acceptance (Red):** tests 1–3, 5–9 fail as described.

## 2. Changes (Green)

1. **`SecretResolver`:** `isConnected()` → `initialized && !sealed`; replace `@Value` fields with
   constructor parameters (build the `WebClient` only after injection, e.g. lazily in `@PostConstruct`).
2. **`SchemaRegistryClient`:** either implement real registration **or** remove the class + doc claims
   (see WP13). Do not keep a "log-only" version.
3. **`JwtAuthenticationFilter`:** authenticate inside the `try`, but keep `chain.doFilter` **outside**;
   let downstream exceptions propagate. Only active when `app.security.enabled=true`
   (`@ConditionalOnProperty` or an early return).
4. **`HitlPortalController.slackCallback`:** verify the Slack signing secret
   (`X-Slack-Signature`/`X-Slack-Request-Timestamp`, HMAC, replay window).
5. **`GdprController`:** implement real deletion/anonymisation **or** mark the endpoints with
   `501 Not Implemented` + a doc note. No fake 200.
6. **`agentflow-ui`:** keep the API key/tenant only in memory (service); remove `localStorage` access
   for secrets. If persistence is desired, document it as a deliberate dev limitation.

## 3. Verification

```bash
cd kafka-platform && mvn -pl agent-orchestrator test
cd agentflow && (cd agentflow-ui && npm test)
```
- Manual: start the Vault mock, test `isConnected` against sealed/unsealed.
- Manual: request with a valid JWT, controller throws → status 500.

## 4. Risks / notes

- Slack signature verification needs the signing secret → add config/docs.
- GDPR deletion touches several stores (run/audit/session) → a transactional strategy is needed.

## Definition of Done

- [ ] Tests 1–10 green.
- [ ] No inverted health/status logic remains.
- [ ] JWT filter does not mask 500s; the security toggle takes effect.
- [ ] Slack callback signature-verified.
- [ ] GDPR endpoints real **or** explicitly 501/documented.
- [ ] No API key in `localStorage`.
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Tests red | |
| | | 2. Implementation | |
| | | 3. Verification | |
