# WP05 — quad-core: guardrails, PII, observability

| | |
|---|---|
| **Status** | open |
| **Severity** | Medium |
| **Modules** | `quad-quarkus/quad-core` |
| **Review reference** | B17, B18, B19, B20, B22 (§5.3, §5.2) |
| **Depends on** | – |
| **Estimated effort** | 2 days |

## Goal

Guardrails behave as documented, PII masking does not mutate shared state, tracing includes the tool
identity, and the hook pipeline no longer swallows errors.

## Findings (as-is)

- **B17 – guardrail actions ignored:** `GuardrailPlugin` does not handle `THROW`/`ESCALATE`;
  everything degrades to `Cancel` (`GuardrailPlugin.java:99-104`).
- **B18 – live list mutated:** `PiiAnonymizerHook` writes into the running list via `messages.set(...)`
  (`PiiAnonymizerHook.java:64`), unlike the non-mutating `PiiRedactionHook`.
- **B19 – PII regex case-sensitive:** `AgentBuilder.java:453-456` uses `[A-Z0-9…]` → lowercase
  (emails!) pass through unfiltered.
- **B20 – tool name discarded:** `OTelAgentTracer.addEvent` ignores `toolName` (`:74-79`).
- **B22 – exceptions swallowed:** `HookRegistry.wrap` swallows exceptions; `failurePolicy` is not
  `volatile` (`:163-172`).

## 1. Tests first (Red)

Test location: `quad-quarkus/quad-core/src/test/java/de/augmentia/quad/core/…`
(existing: `guardrails/GuardrailPluginTest`, `agent/AgentBuilderTest`, `hook/…`, `observability/…`).

| # | Test | Expectation |
|---|------|-------------|
| 1 | `GuardrailPluginTest#throwAction_throws` | `THROW` → exception propagates |
| 2 | `GuardrailPluginTest#escalateAction_escalates` | `ESCALATE` → escalation path/HITL |
| 3 | `GuardrailPluginTest#cancelAction_cancels` | `Cancel` stays as before |
| 4 | `PiiAnonymizerHookTest#doesNotMutateOriginalList` | original unchanged, only the result masked |
| 5 | `PiiAnonymizerHookTest#masksLowercaseEmail` | `user@example.com` is masked |
| 6 | `AgentBuilderTest#piiPatternCaseInsensitive` | lower/upper case treated the same |
| 7 | `OtelTracerTest#addEvent_includesToolName` | span event contains the tool name |
| 8 | `HookRegistryTest#wrap_propagatesOrLogsPerPolicy` | correct per `failurePolicy` (THROW/LOG) |
| 9 | `HookRegistryTest#failurePolicyVisibleAcrossThreads` | `volatile`/immutable → visibility |

**Acceptance (Red):** 1, 2, 4, 5, 7, 8 fail.

## 2. Changes (Green)

1. **`GuardrailPlugin`:** complete action handling (`THROW`, `ESCALATE`, `Cancel`, optionally `RETRY`);
   unknown action → `IllegalArgumentException` instead of a silent `Cancel`.
2. **`PiiAnonymizerHook`:** copy instead of mutate (same semantics as `PiiRedactionHook`); if needed,
   merge both hooks / document clearly which one applies when.
3. **PII pattern:** `CASE_INSENSITIVE` (`(?i)`) or character classes `[A-Za-z0-9]`.
4. **`OTelAgentTracer`:** use `toolName` as an attribute/event field.
5. **`HookRegistry`:** make `failurePolicy` `volatile` or an immutable field; `wrap` logs with context
   and respects the policy (no silent swallowing).

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-core test
cd quad-quarkus && mvn -pl quad-core test -Dtest='Guardrail*,Pii*,*Tracer*,HookRegistry*'
```

## 4. Risks / notes

- Changing guardrail semantics can affect existing workflows → record the behaviour per action in docs.
- `PiiAnonymizerHook` vs. `PiiRedactionHook`: clarify the dual concept (merge in WP13 if appropriate).

## Definition of Done

- [ ] Tests 1–9 green.
- [ ] Guardrail actions behave as documented.
- [ ] PII masked case-insensitively and without side effects.
- [ ] Tool name present in tracing.
- [ ] Hook errors handled in a policy-conformant way.
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Tests red | |
| | | 2. Implementation | |
| | | 3. Verification | |
