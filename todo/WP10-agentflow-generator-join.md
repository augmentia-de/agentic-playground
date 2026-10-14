# WP10 — agentflow: consolidate generator & join

| | |
|---|---|
| **Status** | done |
| **Severity** | High |
| **Modules** | `quad-quarkus/quad-workflow-core`, `quad-quarkus/quad-quarkus`, `agentflow/agentflow-app` |
| **Review reference** | A1, B6, B11 (§3.2, §5.2) |
| **Depends on** | – (test locations independent of WP01) |
| **Estimated effort** | 3–4 days |

## Goal

The two `WorkflowGeneratorService` implementations no longer carry diverging copy-paste code, and
the agentflow plan is no longer truncated by a missing token limit.

## Findings (as-is)

- **A1/B6 – diverged copy (confirmed):** `agentflow/agentflow-app/.../workflow/generate/WorkflowGeneratorService.java`
  was a near-verbatim copy of `quad-quarkus/quad-quarkus/.../workflow/WorkflowGeneratorService.java`
  for the LLM-free half (the `GeneratedStep`/`WorkbookResult` records plus `parseSteps`,
  `validateSteps`, `hasCycle`, `stripFences`). The two copies could drift silently; the parsing is
  exactly the part that decides whether a generated plan is accepted.
- **A1/B6 – "no join logic → parallel forks without a merge" (verified FALSE POSITIVE for agentflow):**
  the two generators diverge because they target **different execution engines**:
  - quad builds a nodes/edges `WorkflowDef`; its engine needs explicit `join` nodes at merge points
    (`addJoinNodes`) — kept as-is.
  - agentflow builds a v2 `quad.workflow` document whose interpreter (`DeclarativeAgentWorkflow`)
    runs a **non-join** node only when **all** its `dependsOn` are done
    (`depsResolved`, line "return resolved == deps.size()") and feeds **all** predecessor outputs into
    the step context (`context()`, "Previous results"). The `join` type is only needed for the
    `any` / `n_of_m` join policies (`joinNeed`). A fork→merge plan already synchronises on the merge
    step's `dependsOn` — no deadlock. The generator prompt documents exactly this contract
    ("that step MUST declare `dependsOn` listing ALL predecessor branches (merge point)").
  Conclusion: porting quad's join nodes into the agentflow document would add nodes that the engine
  does not need. Not done deliberately; documented here.
- **B11 – plan without a token limit (confirmed):** `max-output-tokens: 0` (`application.yml:50`,
  default `:0` in the constructor); `chatParametersJson` omitted the field when 0 → the provider
  truncated long plans → `parseSteps` failed with a bare "Invalid JSON". The quad original defaults
  large (`plannerMaxOutputTokens()` → 32768).

## 1. Tests first (Red)

| # | Test | Expectation |
|---|------|-------------|
| 1 | `WorkflowPlanParserTest` (new, `quad-workflow-core`) | the shared parser accepts a fork→merge plan, rejects unknown deps / cycles / duplicates / missing steps / bad JSON, and normalises fences — the **single-source parity guard** |
| 2 | ~~`WorkflowGeneratorJoinTest#sameInput_sameNodeSet`~~ | **N/A** — see the false-positive finding; parity is now structural (one parser), not a cross-module comparison |
| 3 | `WorkflowGeneratorServiceTest#forkMergePlanParses` (via shared parser) | downstream `dependsOn` resolves |
| 4 | `WorkflowGeneratorServiceTest#truncatedPlanGivesActionableError` | truncation → clear error naming `max-output-tokens`, not a bare parser error |
| 5 | `WorkflowGeneratorServiceTest#defaultTokenLimitApplied` / `#explicitTokenLimitHonoured` | default > 0 (32768), visible in the request |
| 6 | `WorkflowGeneratorParsingTest#*` (quad, existing) | cycle/dep validation still detected (now via the shared parser) |

**Acceptance (Red):** 1, 4, 5 fail before the change (no shared parser; `maxOutputTokens` stayed 0 and
was omitted; truncation produced "Invalid JSON").

## 2. Changes (Green)

1. **Consolidate (shared source):** new `quad-quarkus/quad-workflow-core/.../workflow/generate/WorkflowPlanParser.java`
   holds the `GeneratedStep`/`WorkbookResult` records and `parseSteps(mapper, json)`,
   `validateSteps(...)`, `stripFences(...)`. Both generators import these types and their
   `parseSteps`/`validateSteps`/`stripFences` are now one-line delegates. The duplicated records and
   the private `hasCycle` copy were removed from both apps. Only the target-specific graph building
   (`toWorkflowDef` + `addJoinNodes` vs. `toV2Document`) stays in each application.
2. **Token limit (B11):** agentflow constructor normalises `<= 0` to `DEFAULT_MAX_OUTPUT_TOKENS`
   (32768); `chatParametersJson()` now always sends `maxOutputTokens`; `application.yml` default is
   `32768`. `parsePlannerReply` turns a reply that ends mid-JSON (unbalanced braces) into
   `IllegalStateException` naming the property, instead of a bare "Invalid JSON".

## 3. Verification

```bash
mvn -pl quad-quarkus/quad-quarkus -am test -Dtest='WorkflowPlanParserTest,WorkflowGenerator*'   # quad + shared parser
mvn -pl agentflow/agentflow-app -am test -Dtest='WorkflowGenerator*'                            # agentflow
```

- quad: **BUILD SUCCESS** — `WorkflowPlanParserTest` 10, `WorkflowGeneratorJoinTest` 11,
  `WorkflowGeneratorParsingTest` 35, `WorkflowGeneratorGenerateTest` 13; 0 failures.
- agentflow: **BUILD SUCCESS** — `WorkflowGeneratorServiceTest` 10 (6 existing + 4 new
  token/truncation tests); 0 failures.

## 4. Risks / notes

- Consolidation moves types into `quad-workflow-core`; both apps already depend on it, so no new
  build edges. Test references were updated (`WorkflowGeneratorService.GeneratedStep` →
  `WorkflowPlanParser.GeneratedStep`).
- The engines stay different by design — the join divergence is intentional and now documented.

## Definition of Done

- [x] No diverging parser remains (single shared source; guard test green).
- [x] Fork→merge resolves in the agentflow plan (via `dependsOn`); the join-node sub-claim is a
      verified false positive and documented with engine references.
- [x] No token truncation without a clear error (tests 4, 5).
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-14 | @opencode | 1. Decision consolidation vs. port | Consolidate the shared parser; the join claim is a verified false positive for the v2 engine |
| 2026-10-14 | @opencode | 2. Tests red | `WorkflowPlanParserTest` + agentflow token/truncation tests |
| 2026-10-14 | @opencode | 3. Implementation | `WorkflowPlanParser` shared; both generators delegate; token default 32768 + truncation error |
| 2026-10-14 | @opencode | 4. Verification | quad BUILD SUCCESS (`WorkflowPlanParserTest` 10, `WorkflowGenerator*` 59); agentflow BUILD SUCCESS (`WorkflowGeneratorServiceTest` 10) |
