# WP05 — quad-core: guardrails, PII, observability

| | |
|---|---|
| **Status** | done |
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

Location: `quad-quarkus/quad-core/src/test/java/de/augmentia/quad/core/…`
(new `gdpr/PiiAnonymizerHookTest`, `agent/runtime/OtelTracerTest`, `hook/pipeline/HookRegistryTest`;
extended `guardrails/GuardrailPluginTest`, `agent/AgentBuilderTest`).

| # | Test | Expectation | Red before change |
|---|------|-------------|-------------------|
| 1 | `GuardrailPluginTest#throwAction_throws` | `THROW` → exception propagates | ✅ fails |
| 2 | `GuardrailPluginTest#escalateAction_escalates` | `ESCALATE` → escalation signal | ✅ fails |
| 3 | `GuardrailPluginTest#cancelAction_cancels` | `CANCEL` returns `HookResult.Cancel` w/ reason | ✅ fails |
| 4 | `PiiAnonymizerHookTest#doesNotMutateOriginalList` | original unchanged, masked copy in result | ✅ fails |
| 5 | `PiiAnonymizerHookTest#masksLowercaseEmail` | `user@example.com` is masked | ✅ fails |
| 6 | `AgentBuilderTest#piiPatternCaseInsensitive` | lower/upper case treated the same | ✅ fails |
| 7 | `OtelTracerTest#addEvent_includesToolName` | span event carries the tool name | ✅ fails |
| 8 | `HookRegistryTest#wrap_propagatesOrLogsPerPolicy` | `ISOLATE` skips, `CHAIN_ABORT` propagates | ✅ fails |
| 9 | `HookRegistryTest#failurePolicyVisibleAcrossThreads` | `failurePolicy` is `volatile` | ✅ fails |

Extra guards:

- `HookRegistryTest#guardrailControlFlowIsNeverSwallowed` — `GuardrailException` propagates even
  under `ISOLATE`.
- `PiiAnonymizerHookTest#leavesUntouchedMessagesAsContinue`, `#throwActionCancelsWithoutMutating`,
  `OtelTracerTest#addEvent_withoutToolName_hasNoAttributes`.

**Red evidence:** with the fixes reverted, the five classes reported 3 + 2 + 1 + 3 + 1 failures
(`GuardrailPluginTest`, `PiiAnonymizerHookTest`, `OtelTracerTest`, `HookRegistryTest`,
`AgentBuilderTest`).

> **Note on test 5.** `PiiAnonymizerHook.EMAIL_PATTERN` already used `[a-zA-Z0-9…]`, so lowercase
> e-mail was matched before this WP as well. It is kept as a regression guard; the actual B19 case is
> the `AgentBuilder` default guardrail (test 6), whose `[A-Z0-9…]` form really did let lowercase
> through.

## 2. Changes (Green)

1. **`GuardrailPlugin` + `BlockAction`:** the block action is now honoured —
   `THROW` → `GuardrailException`, `ESCALATE` → pauses the agent and throws a new
   `GuardrailEscalationException` (routing signal for HITL), `CANCEL` → `HookResult.Cancel` with the
   guardrail reason, `FALLBACK` → `HookResult.Cancel` with the fallback message. A new
   `BlockAction.CANCEL` completes the enum.
2. **`PiiAnonymizerHook`:** non-mutating. It builds a masked copy and returns
   `HookResult.Modify<>(maskedMessages)`; the running transcript keeps the original content.
3. **`HookRegistry` + `HookContexts`:** `triggerBeforeModelCall` now returns a typed
   `BeforeModelCallUpdate` (messages + tools + cancel reason). A `Modify` value carrying
   `List<ChatMessage>` replaces the messages sent to the model; `List<ToolSpecification>` still
   filters tools. `AgentRuntime` uses the (possibly anonymized) messages for the request only.
4. **`HookRegistry.wrap`:** rethrows guardrail control-flow exceptions unconditionally, logs every
   failure with hook name + policy, `ISOLATE` → skip, `CHAIN_ABORT` → propagate. `failurePolicy` is
   now `volatile`.
5. **`AgentBuilder`:** the default PII guardrail uses a named, `CASE_INSENSITIVE` `Pattern` and
   `find()` instead of `String.matches`.
6. **`OTelAgentTracer.addEvent`:** attaches `tool.name` as a span-event attribute (via a
   package-private, unit-testable overload).

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-core test
# → BUILD SUCCESS — 526 tests, 0 failures

cd quad-quarkus && mvn -pl quad-core test \
  -Dtest='Guardrail*,Pii*,*Tracer*,HookRegistry*,AgentBuilder*'
# → BUILD SUCCESS — 26 tests, 0 failures
```

Downstream compiled and tested: `quad-quarkus`, `quad-examples`, `agentflow-app` (reactor `-am`).

## 4. Risks / notes

- Guardrail semantics per action are now recorded in the `BlockAction`/`GuardrailPlugin` Javadoc.
  `THROW`/`ESCALATE` deliberately bypass the hook failure policy — callers must be ready to catch
  them.
- `PiiAnonymizerHook` (request anonymization) vs. `PiiRedactionHook` (audit-copy redaction) is still
  a dual concept; a potential merge is left to WP13.

## Definition of Done

- [x] Tests 1–9 green.
- [x] Guardrail actions behave as documented.
- [x] PII masked case-insensitively and without side effects.
- [x] Tool name present in tracing.
- [x] Hook errors handled in a policy-conformant way.
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-14 | | 1. Tests red | 5 classes, verified red by reverting the fixes (3+2+1+3+1 failures) |
| 2026-10-14 | | 2. Implementation | guardrail actions, non-mutating PII, typed before-model-call update, wrap policy + volatile, case-insensitive default PII, tool name on span events |
| 2026-10-14 | | 3. Verification | `quad-core` 526 tests green; `quad-quarkus`/`quad-examples`/`agentflow-app` reactor green |
