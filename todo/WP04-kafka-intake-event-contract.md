# WP04 — Kafka: intake/event contract

| | |
|---|---|
| **Status** | done |
| **Severity** | Critical |
| **Modules** | `kafka-platform/agent-orchestrator`, `kafka-platform/agent-common` |
| **Review reference** | B1, B9, §5.4 (IntakeListener no-op, Redis typo) |
| **Depends on** | – (WP03 done first) |
| **Estimated effort** | 1–2 days |

## Goal

Email (and generally foreign) events reach the intake in a form the orchestrator **understands** and
validates; malformed messages lead to clear errors instead of DLQ noise.

## Findings (as-is)

- **B1 – incompatible event shape:** `EmailInputTransformer` published `ControlEvents.EmailIntakeEvent`
  (`workflowId, triggerKey, emailBody, attachments, initiatorEmail, receivedAt`) to `TopicNames.INTAKE`
  (`EmailInputTransformer.java:58-59`, record `ControlEvents.java:59-66`). The orchestrator reads the
  same topic as a `WorkflowStartEvent` (`DagTopicListener.java:36`); the fields do not overlap →
  `workflowExecutionId == null` → `UUID.fromString(...)` throws (`WorkflowEngine.java:112`) → DLQ.
  The feature is therefore completely dead.
- **B9 – UUID before validation:** `WorkflowEngine.onResult` parsed the `UUID` before the null/blank
  check (`:139`) — and the other handlers (`onIntake`, `onLoop`, `onCancellation`, `onHitl`) did the
  same. Any foreign/malformed message threw and filled the DLQ.
  *Review note:* the report's "startup self-check" does **not** exist in this codebase — no
  `ApplicationRunner`/`@PostConstruct` publishes a synthetic result. The real risk is any foreign
  message; this is what the tests pin down.
- **§5.4 – `IntakeListener.handleIntake` was a no-op** (only acked); `trackEmailIntake` built a new
  `ObjectMapper` per message and stored the initiator under the *email's* random `workflowId`, which
  never matched the real execution id.
- **§5.4 – Redis key typo** `email:initator:` (missing second `i`, `EmailInitiatorStore.java`).
- **Bonus – dead response path:** `MailResponseHandler.lookupInitiatorEmail` was a placeholder that
  always returned `null`, so even a correct initiator mapping could never have produced a reply.

## 1. Tests first (Red)

| # | Test | Expectation |
|---|------|-------------|
| 1 | `EmailInputTransformerTest#acceptedEmailStartsWorkflow` | accepted email → `WorkflowStartService.start(...)` with email input + initiator stored under the **real** execution id |
| 2 | `EmailInputTransformerTest#duplicateStartIsAcknowledged` | duplicate start is acked, still stores the initiator |
| 3 | `EmailInputTransformerTest#wrongRecipientIsRejected` | other recipient → 400, no start |
| 4 | `EmailInputTransformerTest#startFailureReturns500` | missing definition/DB/broker → 500, no initiator stored |
| 5 | `EmailInputTransformerTest#triggerKeyIsNormalised` / `#missingTriggerFallsBackToDefault` | `[QUAD] My Task` → `my-task`; no tag → `default` |
| 6 | `EmailInputTransformerTest#inputParamsAreJsonSafe` | input map contains no `null` values |
| 7 | `WorkflowEngineCharacterizationTest#onResultWithMalformedTaskIdIsIgnored` | invalid `taskId` → skip, **no** DLQ row, **no** DLQ publish |
| 8 | `WorkflowEngineCharacterizationTest#onResultWithNullTaskIdIsIgnored` | missing `taskId` → skip, no DLQ |
| 9 | `WorkflowEngineCharacterizationTest#onIntakeWithInvalidExecutionIdIsIgnored` | invalid `workflowExecutionId` → skip, no throw |
| 10 | `WorkflowEngineCharacterizationTest#onIntakeWithNullExecutionIdIsIgnored` | missing `workflowExecutionId` → skip, no throw |
| 11 | `EmailInitiatorStoreTest#initiatorKeyIsSpelledCorrectly` | key is `email:initiator:{workflowId}` |
| 12 | `EmailInitiatorStoreTest#lookupUsesTheSameKey` | lookup reads the same key that store writes |
| 13 | `IntakeAndConfigListenerTest`, `ListenerAckModeContractTest` | updated: the removed no-op listener no longer contributes a manual-ack listener |

Tests 1–10 are the Red tests. The two Redis-key tests are also Red against the old `email:initator:`
spelling.

## 2. Changes (Green)

1. **Contract decision — option (B), one event type.** `EmailInputTransformer` no longer publishes a
   bespoke event. It calls the canonical `WorkflowStartService.start(name, version, inputParams, key)`
   — the same path the REST API uses. The trigger key from the subject (`[QUAD] <name>`, normalised)
   is the workflow name; the email travels as workflow input parameters
   (`task`, `emailBody`, `subject`, `initiatorEmail`, `attachments`, `triggerKey`). The initiator mapping
   is written keyed by the **real** execution id returned by the start service.
2. **`MailResponseHandler`** now reads the mapping through `EmailInitiatorStore.lookupInitiator(workflowId)`
   instead of returning `null` — the response half of the loop is no longer dead.
3. **Robust validation:** new `WorkflowEngine.parseUuidOrNull(raw, field)` returns `null` for
   missing/blank/malformed values; applied in `onIntake`, `onResult`, `onLoop`, `onCancellation`,
   `onHitl`. An unidentifiable `onResult` increments
   `orchestrator_stale_result_total{reason="invalid_task_id"}` and returns — it is never DLQ'd.
4. **`IntakeListener` removed** (it was a no-op plus superseded tracking, and a distraction in lag
   metrics).
5. **Redis key:** `email:initator:` → `email:initiator:`; misspelled identifiers
   (`INTAINER_PREFIX`, `buildIntainerKey`) renamed; migration note added to the class javadoc.
6. **`ControlEvents.EmailIntakeEvent` retained** in `agent-common`: it is a published type that other
   services may still bind to. It is no longer produced; removal is left to WP13.

## 3. Verification

```bash
mvn -pl kafka-platform/agent-orchestrator -am test
```

- Result: **BUILD SUCCESS** — `agent-common` + `agent-orchestrator` green; 1:14 min. New/updated
  suites: `EmailInputTransformerTest` 7, `EmailInitiatorStoreTest` 5,
  `WorkflowEngineCharacterizationTest` 27 (23 + 4), `IntakeAndConfigListenerTest` 6,
  `ListenerAckModeContractTest` 4 — 0 failures.
- The email path is exercised end-to-end at unit level (start call + input params + initiator key);
  `WorkflowStartService` itself is already covered by the existing `WorkflowEngineTestBase` suites.

## 4. Risks / notes

- **INTAKE consumers:** the wire contract for start events is unchanged (still `WorkflowStartEvent`).
  Only the *producer* of email starts changed, so the quad bridge and any external consumer are
  unaffected.
- **Redis key change is not backward-compatible.** The feature was dead, so no live mapping can exist
  in practice; the key is re-written on the next intake and the TTL is 7 days. Documented in the
  javadoc rather than migrated.

## Definition of Done

- [x] Email event really starts a workflow (tests 1–6).
- [x] No `IllegalArgumentException`/DLQ noise on invalid/missing IDs (tests 7–10).
- [x] No-op intake listener removed (test 13).
- [x] Redis key correct, migration documented (tests 11–12).
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-14 | @opencode | 1. Contract decision | option (B): reuse `WorkflowStartService`, one event type |
| 2026-10-14 | @opencode | 2. Tests written | `EmailInputTransformerTest`, `EmailInitiatorStoreTest`, 4 `WorkflowEngineCharacterizationTest` cases; `IntakeAndConfigListenerTest`/`ListenerAckModeContractTest` updated |
| 2026-10-14 | @opencode | 3. Implementation | `EmailInputTransformer`, `MailResponseHandler`, `WorkflowEngine`, `EmailInitiatorStore`; `IntakeListener` removed |
| 2026-10-14 | @opencode | 4. Verification | `mvn -pl kafka-platform/agent-orchestrator -am test` → BUILD SUCCESS (0 failures) |
