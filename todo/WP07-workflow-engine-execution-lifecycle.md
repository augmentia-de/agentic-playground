# WP07 — Workflow engine: execution & lifecycle

| | |
|---|---|
| **Status** | open |
| **Severity** | Medium |
| **Modules** | `quad-quarkus/quad-workflow-engine`, `quad-quarkus/quad-quarkus` |
| **Review reference** | B23, B24, B25, B28, B29, B30 (§5.2, §5.3) |
| **Depends on** | – |
| **Estimated effort** | 2–3 days |

## Goal

The engine leaks no runs, loses no asynchronous results, produces valid JSON, does not block in a
busy-wait, and cannot execute a node twice. Sandbox processes do not store without bound.

## Findings (as-is)

- **B23 – run leak:** `WorkflowEngine.execute` calls `removeRun` only on the success path
  (`WorkflowEngine.java:98`); the error path (`:115-129`) leaves the `runOutputs` entry in place.
- **B24 – silent no-op:** `WorkflowSupport.completeDeferred` writes nothing and returns when the run
  is not registered (`:169-176`); the caller reports success.
- **B25 – unescaped JSON:** event payloads via string concatenation (`:57-58,110,127`).
- **B28 – busy-wait/infinite loop:** `AsyncNodeExecutor` polls with `Thread.sleep(50)`, with
  `timeout==0` an infinite loop (`:78-100`); `WorkflowScheduler` polls likewise (`:55-64`).
- **B29 – double execution:** `AsyncNodeExecutor` additionally calls `runner.runNode(...)` after
  `runChildNode(...)==null` (`:61-65`).
- **B30 – unbounded stderr:** only stdout is throttled, stderr grows without bound
  (`SandboxPoolClient.java:132-160`).

## 1. Tests first (Red)

Test location: `quad-quarkus/quad-workflow-engine/src/test/java/de/augmentia/quad/workflow/engine/…`
(existing: `WorkflowEngineContainerlessTest`, `internal/WorkflowSupportTimeoutTest`,
`node/PrefixSourceResolverTest`, `LoopNodeExecutor*Test`).

| # | Test | Expectation |
|---|------|-------------|
| 1 | `WorkflowEngineLifecycleTest#failedRun_isRemovedFromRegistry` | failure → `runOutputs` empty |
| 2 | `WorkflowEngineLifecycleTest#successRun_isRemoved` | success → empty (existing) |
| 3 | `WorkflowSupportTest#completeDeferred_missingRun_fails` | missing run → exception/callback, no silent success |
| 4 | `WorkflowEngineEventJsonTest#specialChars_produceValidJson` | name/error with `"`/`\n` → valid JSON (Jackson parse) |
| 5 | `AsyncNodeExecutorTest#noTimeout_doesNotHangForever` | `timeout==0` does not block forever |
| 6 | `AsyncNodeExecutorTest#childExecutedExactlyOnce` | node runs exactly once |
| 7 | `SandboxOutputTest#stderrBounded` | stderr flood → output bounded (no OOM) |

**Acceptance (Red):** 1, 3, 4, 6, 7 fail.

## 2. Changes (Green)

1. **Lifecycle:** call `removeRun` in `finally`; give registry entries a TTL/cleanup.
2. **`completeDeferred`:** missing run → clear error (exception/`Result.failure`), the caller
   propagates; `DynamicWorkflowResource.complete` must no longer blindly report "ok".
3. **JSON:** use the central `ObjectMapper` (already present) for event payloads; no string concatenation.
4. **Async:** notify via the existing `monitor` object instead of polling; define `timeout==0`
   (infinite only with a real signal, or hard-bounded).
5. **Double execution:** a single dispatch decision; remove or make the fallback idempotent.
6. **Sandbox:** route stderr through the same ring buffer with an identical cap.

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-workflow-engine test
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='*Workflow*'
```
- Long-running test: many failing runs → heap stable.
- Sandbox with a `yes >&2`-like command → no OOM.

## 4. Risks / notes

- The `monitor` rework touches several executors → synchronise carefully (lost wakeup).
- The JSON switch can affect existing event consumers → compatibility test.

## Definition of Done

- [ ] No run leak (tests 1, 2).
- [ ] `completeDeferred` fails loudly instead of silently (test 3).
- [ ] Events are valid JSON (test 4).
- [ ] No busy-wait/infinite loop (test 5), no double execution (test 6).
- [ ] stderr bounded (test 7).
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Tests red | |
| | | 2. Implementation | |
| | | 3. Verification | |
