# WP10 — agentflow: consolidate generator & join

| | |
|---|---|
| **Status** | open |
| **Severity** | High |
| **Modules** | `agentflow/agentflow-app`, `quad-quarkus/quad-quarkus` |
| **Review reference** | A1, B6, B11 (§3.2, §5.2) |
| **Depends on** | – (test locations independent of WP01) |
| **Estimated effort** | 3–4 days |

## Goal

The two `WorkflowGeneratorService` implementations are **unified** (no diverging copy-paste branch).
The agentflow generator again produces **join nodes** on fork/parallel, and the plan is no longer
truncated by a missing token limit.

## Findings (as-is)

- **A1/B6 – diverged copy:** `agentflow/agentflow-app/src/main/java/de/augmentia/agentflow/app/workflow/generate/WorkflowGeneratorService.java`
  is a near-verbatim copy of
  `quad-quarkus/quad-quarkus/src/main/java/de/augmentia/quad/quarkus/workflow/WorkflowGeneratorService.java`
  (identical private method names), but **17 vs. 29** private methods and **no** join logic
  (`grep 'join'` → 0 hits vs. 11 in the original, including `:465 addJoinNodes(...)`, `:548`).
  Consequence: parallel forks without a merge.
- **B11 – plan without a token limit:** `max-output-tokens: 0` (`application.yml:50`, default `:0` in
  `:110`); `chatParametersJson` omits the field (`:183-184`) → the provider truncates long plans →
  `parseSteps` fails. The Quad original deliberately defaults large (`:249-254`).

## 1. Tests first (Red)

Test locations (both exist):
- `quad-quarkus/quad-quarkus/src/test/java/de/augmentia/quad/quarkus/workflow/WorkflowGeneratorJoinTest.java`
- `agentflow/agentflow-app/src/test/java/de/augmentia/agentflow/app/workflow/generate/WorkflowGeneratorServiceTest.java`

| # | Test | Expectation |
|---|------|-------------|
| 1 | `WorkflowGeneratorServiceTest#forkMerge_containsJoinNode` (agentflow) | fork→merge plan contains a join node |
| 2 | `WorkflowGeneratorJoinTest#sameInput_sameNodeSet` | both generators produce an identical node set (parity test) |
| 3 | `WorkflowGeneratorServiceTest#parallelBranches_downstreamResolves` | downstream `dependsOn` resolves |
| 4 | `WorkflowGeneratorServiceTest#truncatedPlan_actionableError` | truncation → clear error (not a parser error) |
| 5 | `WorkflowGeneratorServiceTest#defaultTokenLimit_applied` | default > 0, visible in the request |
| 6 | `WorkflowGeneratorParsingTest#cyclicPlan_rejected` | cycle detected (existing/parity) |

**Acceptance (Red):** 1, 2, 3, 5 fail.

## 2. Changes (Green)

1. **Consolidate (preferred):** move the shared generator into a reusable module
   (e.g. `quad-workflow-core` or a new `quad-workflow-generate`) used by both apps.
   Fallback if consolidation is too large: port the join logic exactly **and** keep the parity test (2)
   as a guard against renewed divergence.
2. **Join logic:** take `addJoinNodes`/`uniqueJoinId`/`addJoinNode` from the original
   (`:465,548-601`), including timeout derivation.
3. **Token limit:** default to a sensible value (e.g. 32768); adjust `application.yml:50`; throw a
   clear error on `finish_reason == length`.
4. **Keep a permanent parity/regression test** so the copies do not drift apart again.

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='WorkflowGenerator*'
cd agentflow && mvn -pl agentflow-app test -Dtest='WorkflowGenerator*'
```
- Manual: generate a fork prompt, inspect the produced graph in the editor (join present).
- End-to-end: agentflow generates → executes (if UI/engine available).

## 4. Risks / notes

- Consolidation affects build order/POMs → root-reactor test (`mvn -T1C clean install`).
- Prompt texts may differ → choose deliberately during the merge and document it.

## Definition of Done

- [ ] No diverging generator remains (shared source **or** parity guard green).
- [ ] Fork→merge produces a join (tests 1, 3).
- [ ] No token truncation without a clear error (tests 4, 5).
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Decision consolidation vs. port | |
| | | 2. Tests red | |
| | | 3. Implementation | |
| | | 4. Verification | |
