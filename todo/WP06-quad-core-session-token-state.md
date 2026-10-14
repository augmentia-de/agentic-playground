# WP06 — quad-core: session, token, state

| | |
|---|---|
| **Status** | open |
| **Severity** | Medium |
| **Modules** | `quad-quarkus/quad-core` |
| **Review reference** | B15, B16, B21 (§5.3, §5.2) |
| **Depends on** | – |
| **Estimated effort** | 1–2 days |

## Goal

Token exchange returns the correct token, session snapshots lose no data, and the
conversation-manager logic no longer follows a dead `==` branch.

## Findings (as-is)

- **B15 – token cache bug (security-relevant):** `TokenExchangeClient` caches only by `audience`,
  ignoring `subjectToken`/`scopes` (`TokenExchangeClient.java:71`) → **wrong token** for the same
  audience.
- **B16 – snapshot loses summary:** `SessionStateSnapshot.toState()` omits `MemorySnapshot.summary`
  (`:103-106`).
- **B21 – reference comparison:** `SummarizingSlidingWindowConversationManager` compares
  `toKeep == nonSystemMsgs` (`subList` view) → never true, wrong branch (`:70`).

## 1. Tests first (Red)

Test location: `quad-quarkus/quad-core/src/test/java/de/augmentia/quad/core/…`

| # | Test | Expectation |
|---|------|-------------|
| 1 | `TokenExchangeClientTest#differentSubject_differentToken` | different `subjectToken` → different (uncached) token |
| 2 | `TokenExchangeClientTest#differentScopes_differentToken` | different `scopes` → own cache entry |
| 3 | `TokenExchangeClientTest#sameKey_hitsCache` | identical inputs → cache hit |
| 4 | `SessionStateSnapshotTest#roundTrip_preservesSummary` | `toState`→`from` contains the summary |
| 5 | `SummarizingSlidingWindowConversationManagerTest#keepsNonSystemMessages` | correct message selection (no dead branch) |

Test locations, possibly new: `security/TokenExchangeClientTest.java`,
`session/SessionStateSnapshotTest.java`,
`session/memory/SummarizingSlidingWindowConversationManagerTest.java`.

**Acceptance (Red):** 1, 2, 4, 5 fail.

## 2. Changes (Green)

1. **Cache key:** a composite key from `audience + subjectToken(+hash) + scopes` (immutable value
   object as key, correct `equals/hashCode`). Do not log sensitive tokens in plaintext as the key.
2. **Snapshot:** serialize/deserialize `summary` (including the null/empty case).
3. **Conversation manager:** replace the condition with a content check (e.g. `isEmpty`/`size`,
   compare against the original list object instead of the `subList` view); remove the dead branch.

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-core test
cd quad-quarkus && mvn -pl quad-core test -Dtest='*TokenExchange*,*SessionState*,*ConversationManager*'
```

## 4. Risks / notes

- The cache key contains the token → do not log it; use a hash if needed.
- Snapshot-format change: check backward compatibility with old snapshots (versioning).

## Definition of Done

- [ ] Tests 1–5 green.
- [ ] Token cache keys correctly (no cross-subject leak).
- [ ] Summary survives the round-trip.
- [ ] No dead reference comparison remains.
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Tests red | |
| | | 2. Implementation | |
| | | 3. Verification | |
