# WP11 — agentflow: determinism & request context

| | |
|---|---|
| **Status** | open |
| **Severity** | High |
| **Modules** | `agentflow/agentflow-app` |
| **Review reference** | B12 (§5.2), §6 (Declarative: replay/pruning/loop) |
| **Depends on** | – |
| **Estimated effort** | 2–3 days |

## Goal

Temporal workflow code is deterministic (no `HashMap` ordering influence, stable pruning, sensible
loop bounds), and per-request headers/tenant take effect on **all** activity paths.

## Findings (as-is)

- **B12 – request context missing:** only `runAgentGraph` sets `RequestContext`
  (`AgentFlowActivitiesImpl.java:35,59`); `runQuadAgent`/`runRichAgent`/`runAuthorizedAgent`
  (`:78,84,95`) do not → headers/tenant silently ignored; potential ThreadLocal leak.
- **§6 – Declarative workflow:**
  - `HashSet`/`HashMap` in workflow state (`DeclarativeAgentWorkflow.java:53,71`) — ordering can
    influence inputs/replay.
  - **Non-transitive pruning** (`:138-142`): only direct non-targets, descendants neither skipped nor
    executed → silent omission from `outputs`.
  - **Loop default** `Math.max(1, maxIterations())` (`:149-171`) → unset runs exactly once; no
    repository-wide iteration limit.

## 1. Tests first (Red)

Test location: `agentflow/agentflow-app/src/test/java/de/augmentia/agentflow/app/workflow/declarative/…`
(existing: `DeclarativeAgentWorkflowTest`, `ConditionEvaluatorTest`, `DeclarativePlannerTest`)
and `…/config/AgentFlowLlmHeaderPropertiesTest`, `LlmConfigBridgeTest`.

| # | Test | Expectation |
|---|------|-------------|
| 1 | `AgentFlowActivitiesImplTest#runQuadAgent_setsRequestContext` | headers/tenant take effect |
| 2 | `AgentFlowActivitiesImplTest#runRichAgent_setsRequestContext` | ditto |
| 3 | `AgentFlowActivitiesImplTest#runAuthorizedAgent_setsRequestContext` | ditto |
| 4 | `AgentFlowActivitiesImplTest#contextIsClearedAfterRun` | no ThreadLocal leak |
| 5 | `DeclarativeAgentWorkflowTest#deterministicContextOrdering` | same input → same activity input (sorted) |
| 6 | `DeclarativeAgentWorkflowTest#pruningIsTransitive` | pruned descendants are marked as `skipped` |
| 7 | `DeclarativeAgentWorkflowTest#loopRespectsHardCap` | excessive iterations → hard-capped + notice |
| 8 | `DeclarativeAgentWorkflowTest#unsetIterations_definedBehavior` | `0` → documented behaviour (not silently 1×) |

**Acceptance (Red):** 1–3, 5, 6, 8 fail.

## 2. Changes (Green)

1. **Central request context:** a shared `withRequestContext(runId, action)` helper (or aspect) used by
   **all four** activities; guaranteed clear in `finally`.
2. **Determinism:** `TreeMap`/`LinkedHashMap` + explicit sorting in `context(...)`/output composition;
   use `WorkflowReplayer` or a repeat test in the test.
3. **Pruning:** propagate transitively to descendants; list pruned descendants explicitly as `skipped`
   in `outputs`/status → do not silently omit.
4. **Loop:** define the unset behaviour clearly (e.g. error or a documented default), with a
   repository-wide `maxIterations` limit and a log/event on reaching it.

## 3. Verification

```bash
cd agentflow && mvn -pl agentflow-app test -Dtest='Declarative*,AgentFlowActivities*'
```
- Temporal replay test: replay recorded history → no non-determinism errors.

## 4. Risks / notes

- Introducing an aspect/helper into activities: mind the Temporal activity interfaces (no state across
  replays).
- The pruning change affects which nodes appear in `outputs` → check existing workflows.

## Definition of Done

- [ ] All activities set/clear the context (tests 1–4).
- [ ] Replay-deterministic (test 5).
- [ ] Pruning transitive and visible (test 6).
- [ ] Loop bounds defined (tests 7, 8).
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Tests red | |
| | | 2. Implementation | |
| | | 3. Replay test | |
