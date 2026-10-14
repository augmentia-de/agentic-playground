# WP04 — Kafka: intake/event contract

| | |
|---|---|
| **Status** | open |
| **Severity** | Critical |
| **Modules** | `kafka-platform/agent-orchestrator`, `kafka-platform/agent-common` |
| **Review reference** | B1, B9, §5.4 (IntakeListener no-op, Redis typo) |
| **Depends on** | – (WP03 recommended first) |
| **Estimated effort** | 1–2 days |

## Goal

Email (and generally foreign) events reach the intake in a form the orchestrator **understands** and
validates; malformed messages lead to clear errors instead of DLQ noise.

## Findings (as-is)

- **B1 – incompatible event shape:** `EmailInputTransformer` publishes `ControlEvents.EmailIntakeEvent`
  (`workflowId, triggerKey, emailBody, initiatorEmail, receivedAt`) to `TopicNames.INTAKE`
  (`EmailInputTransformer.java:58-59`, record `ControlEvents.java:59-66`). The orchestrator reads the
  same topic as a `WorkflowStartEvent` (`DagTopicListener.java:36`); the fields do not overlap →
  `workflowExecutionId == null` → `UUID.fromString(...)` throws (`WorkflowEngine.java:112`) → DLQ.
  The feature is therefore completely dead.
- **B9 – UUID before validation:** `WorkflowEngine.onResult` parses the `UUID` before the null/blank
  check (`:139`); the start self-check produces a synthetic event → DLQ noise on **every** boot.
- **§5.4 – `IntakeListener.handleIntake` is a no-op** (only acks, `:38-47`); builds a new
  `ObjectMapper` per message (`:59`).
- **§5.4 – Redis key typo** `email:initator:` (`EmailInitiatorStore.java:29`).

## 1. Tests first (Red)

Test location: `kafka-platform/agent-orchestrator/src/test/java/de/augmentia/agentplatform/orchestrator/…`
(harness: `IntakeAndConfigListenerTest`, `WorkflowEngineCharacterizationTest`, `WorkflowEngineTestBase`).

| # | Test | Expectation |
|---|------|-------------|
| 1 | `EmailIntakeContractTest#emailEvent_startsWorkflow` | `EmailIntakeEvent` on `INTAKE` → workflow starts with the correct input |
| 2 | `EmailIntakeContractTest#emailEvent_roundTripsThroughKafkaSerialization` | producer JSON → consumer object lossless |
| 3 | `WorkflowEngineTest#onIntake_missingWorkflowExecutionId_clearError` | missing ID → domain error/bad request, not `NumberFormatException` |
| 4 | `WorkflowEngineTest#onResult_invalidUuid_noDlqNoise` | invalid `taskId` → clear validation, no self-inflicted DLQ |
| 5 | `WorkflowEngineTest#startupSelfCheck_doesNotThrow` | boot without a DLQ entry |
| 6 | `IntakeListenerTest#handleIntake_doesRealWork_orRemoved` | real routing or the listener is removed |
| 7 | `EmailInitiatorStoreTest#keyUsesCorrectSpelling` | key `email:initiator:` (if public) incl. migration |

**Acceptance (Red):** tests 1, 3, 4, 5, 6 fail or pin the misbehaviour.

## 2. Changes (Green)

1. **Define the contract (one truth):** either
   - (A) the intake consumes its own `EmailIntakeEvent` in a separate listener and maps it to the
     workflow start, **or**
   - (B) `WorkflowStartEvent` gets a correlation field `initiatorEmail`/`source`, and
     `EmailInputTransformer` produces a complete `WorkflowStartEvent`.
   Recommendation: (B) — one event type, fewer topics.
2. **Robust validation:** call `UUID.fromString` only after a null/blank/format check; on error, send
   to the DLQ with a clear message **only** for genuine foreign errors, not for the self-check.
3. **Self-check:** synthetic event without the UUID requirement (own marker), or make the self-check
   switchable off.
4. **`IntakeListener`:** either real work or remove the listener + consumer group (otherwise confusing
   lag metrics). Register a single `ObjectMapper` bean.
5. **Redis key:** correct spelling; document a migration/compatibility note.

## 3. Verification

```bash
mvn -pl kafka-platform/agent-orchestrator -am test
cd kafka-platform && mvn -pl agent-orchestrator test
```
- End-to-end with Testcontainers: publish an email event → workflow runs to completion.
- Check the boot log: no DLQ entries from the self-check.

## 4. Risks / notes

- Existing consumers of the `INTAKE` topic (quad bridge) must not break →
  a compatibility test (`QuadUiPayloadCompatibilityTest` as a template).
- The Redis key change is not backward-compatible → migration or dual-read.

## Definition of Done

- [ ] Email event really starts a workflow (tests 1, 2).
- [ ] No `NumberFormatException` DLQ on invalid/missing IDs (tests 3–5).
- [ ] No-op intake listener removed or made meaningful (test 6).
- [ ] Redis key correct or migrated (test 7).
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Contract decision (A/B) | |
| | | 2. Tests red | |
| | | 3. Implementation | |
| | | 4. Verification | |
