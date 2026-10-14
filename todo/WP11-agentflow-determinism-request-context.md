# WP11 — agentflow: determinism & request context

| | |
|---|---|
| **Status** | done |
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

Location: `agentflow/agentflow-app/src/test/java/de/augmentia/agentflow/app/…`
(new `activities/AgentFlowActivitiesImplTest`, extended `workflow/declarative/DeclarativeAgentWorkflowTest`).

| # | Test | Expectation | Red before change |
|---|------|-------------|-------------------|
| 1 | `AgentFlowActivitiesImplTest#runQuadAgent_setsRequestContext` | headers/tenant take effect | ✅ fails (`null`) |
| 2 | `AgentFlowActivitiesImplTest#runRichAgent_setsRequestContext` | ditto | ✅ fails (`null`) |
| 3 | `AgentFlowActivitiesImplTest#runAuthorizedAgent_setsRequestContext` | ditto | ✅ fails (`null`) |
| 4 | `AgentFlowActivitiesImplTest#contextIsClearedAfterRun` | no ThreadLocal leak | ✅ fails (`null`) |
| 5 | `DeclarativeAgentWorkflowTest#deterministicContextOrdering` | same input → same activity input (sorted) | ✅ fails (unsorted) |
| 6 | `DeclarativeAgentWorkflowTest#pruningIsTransitive` | pruned descendants are marked as `skipped` | ❌ **passes already** — see note |
| 7 | `DeclarativeAgentWorkflowTest#loopRespectsHardCap` | excessive iterations → hard-capped + notice | ✅ fails (250 vs. 100) |
| 8 | `DeclarativeAgentWorkflowTest#unsetIterations_definedBehavior` | `0` → documented behaviour (not silently 1×) | ✅ fails (1 vs. 100) |

Extra guards added:

- `AgentFlowActivitiesImplTest#contextIsClearedWhenBodyThrows` — context is cleared even when the
  activity body throws (the `finally`).
- `DeclarativeAgentWorkflowTest#repeatedRunsProduceIdenticalActivitySequence` — two runs of the same
  plan produce the identical activity sequence (repeat/replay determinism without needing
  `WorkflowReplayer`).

**Red evidence:** reverting the three fixes (context set, sorting, loop bound) gave
`AgentFlowActivitiesImplTest` 4 failures and `DeclarativeAgentWorkflowTest` 3 failures;
`loopRespectsHardCap` reported `expected: 100L but was: 250L`.

### Note on test 6 (verified partial false positive)

The Review claimed pruned branch **descendants are "neither skipped nor executed"**. That is
**not** reproducible: the existing dependency-based `shouldSkip` already skips any node with a
skipped direct dependency, so it propagates transitively through the dependency graph. With the new
`pruneCascade` deliberately disabled, `pruningIsTransitive` still passes — descendants never ran.
`pruneCascade` is therefore a *robustness/visibility* change (it makes the "can never run" set
explicit and covers nodes the loop has not yet revisited, e.g. when an `end` node completes before
the pruned branch was marked), not a run-bug fix. Test 6 is kept as a regression guard.

## 2. Changes (Green)

1. **Central request context** (`AgentFlowActivitiesImpl`):
   - One private `withRequestContext(Function<String,String>)` helper used by **all four** activities
     (`runAgentGraph`, `runQuadAgent`, `runRichAgent`, `runAuthorizedAgent`). It sets
     `RequestContext` (Spring-AI dynamic headers) and always clears it in `finally` — no ThreadLocal
     leak on the reused activity thread.
   - The run id is also bound to the LangChain4j path as the quad-core **session id**
     (`AgentSessionState.create(runId)`), so the quad/LangChain4j agents get per-run headers too
     (previously a random per-agent UUID). `runId()` is a small overridable seam
     (`Activity.getExecutionContext().getInfo().getWorkflowId()`).
   - The agent bodies (`runSimpleAgent`, `runCapabilities`) are package-private seams so tests can
     substitute a fake LLM body.

2. **Determinism** (`DeclarativeAgentWorkflow.Impl`):
   - All workflow state uses insertion-ordered collections (`LinkedHashMap`/`LinkedHashSet`).
   - `context(...)` sorts the dependency list (`Comparator.naturalOrder()`) before composing the
     activity input, so a replay sees a stable ordering regardless of JSON order.

3. **Pruning** (`DeclarativeAgentWorkflow.Impl`):
   - New `pruneCascade(...)`: after a `decision` prunes the non-selected targets, it extends the set
     transitively to every node whose dependencies **all** lie in the pruned closure. `end` nodes are
     exempt (they must still close the workflow) and subset joins (`any`/`n_of_m`) keep running while
     a non-pruned predecessor exists — mirroring `shouldSkip`.

4. **Loop bounds** (`DeclarativeAgentWorkflow.Impl`):
   - Repository-wide hard cap `MAX_LOOP_ITERATIONS = 100`; a declared `maxIterations` is clamped to it.
   - A non-positive/absent `maxIterations` now means "run until the exit condition, bounded by the
     hard cap" (documented behaviour) — never silently exactly one iteration.
   - Reaching the cap without meeting the exit condition, or declaring a value above the cap, logs a
     `Workflow` warning.

## 3. Verification

```bash
mvn -pl agentflow/agentflow-app -am test \
  -Dtest='AgentFlowActivitiesImplTest,DeclarativeAgentWorkflowTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
# → BUILD SUCCESS — 5 + 10 tests, 0 failures

mvn -pl agentflow/agentflow-app -am test       # whole reactor incl. quad-core, examples
# → BUILD SUCCESS
```

Replay/repeat determinism is covered by `repeatedRunsProduceIdenticalActivitySequence` and by the
`TestWorkflowEnvironment` (the workflow is replayed on every activity/await cut).

## 4. Risks / notes

- Introducing an aspect/helper into activities: the helper is a plain method on the activity impl —
  no state survives replays, and the activity interface is unchanged.
- The loop default change is a behaviour change for documents that omit `maxIterations`: they now
  iterate up to the hard cap instead of once. Callers should set an explicit bound where the previous
  "once" semantics were intended.
- `context(...)` sorting changes the composed activity input order for multi-dependency nodes (only
  ordering, same content). No stored-workflow compatibility guarantee is given for in-flight v2 runs.

## Definition of Done

- [x] All activities set/clear the context (tests 1–4).
- [x] Replay-deterministic (test 5 + repeat test).
- [x] Pruning transitive and visible (test 6; false-positive nuance documented above).
- [x] Loop bounds defined (tests 7, 8).
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-14 | | 1. Tests red | 5 new tests + 8 assertions; verified red by reverting the fixes (4 + 3 failures) |
| 2026-10-14 | | 2. Implementation | context helper for 4 activities; sorted deps; `pruneCascade`; loop cap 100 + warnings |
| 2026-10-14 | | 3. Replay test | repeat test + full `agentflow-app -am` reactor green |
