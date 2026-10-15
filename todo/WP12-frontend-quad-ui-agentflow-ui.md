# WP12 — Frontend: quad-ui & agentflow-ui

| | |
|---|---|
| **Status** | done |
| **Severity** | Medium |
| **Modules** | `quad-quarkus/quad-ui` (React), `agentflow/agentflow-ui` (Angular) |
| **Review reference** | C1, C2 (§8.6), C3 (see WP02) |
| **Depends on** | C3 → WP02 |
| **Estimated effort** | 3–5 days |

> The security aspect **C3** (API key in `localStorage`) is handled in **WP02**.
> This WP is about structure & duplicates.

## Goal

`WorkflowComposer` and the Angular graph editor are broken into maintainable units, the quad-ui API
client is de-duplicated, and the core UI has reasonable test coverage.

## Findings (as-is)

- **C1 – god components:** `quad-ui/src/components/WorkflowComposer.tsx` is **1,669 lines**,
  **26 `useState`** (`:226-565`), four `useEffect` (`:569,580,603,621`);
  `agentflow-ui/src/app/ui/graph-editor.ts` is **593 lines**.
- **C2 – duplicates:** `api.ts` `fetchSkills` (`:437`) and `fetchSkillList` (`:504`) are byte-identical;
  `createAgent`/`updateAgent` duplicate a 19-field `flat` block (`:154-201`).
- **Coverage:** `WorkflowComposer.test.tsx` (168 lines) is small relative to the 1,669-line component.

## 1. Tests first (Red/characterisation)

First add **characterisation tests** that pin the current behaviour of the large component — then
refactor without changing behaviour.

| # | Test | Purpose |
|---|------|---------|
| 1 | `WorkflowComposer.test.tsx#rendersAndEditsBaseline` | pin basic behaviour (before refactoring) |
| 2 | `WorkflowComposer.test.tsx#forkMergeRoundTrip` | fork→merge through the composer |
| 3 | `api.test.ts#fetchSkills_equals_fetchSkillList` | duplicates identical (goal: remove one) |
| 4 | `api.test.ts#agentCreateUpdate_useSharedMapper` | one mapper for create/update |
| 5 | `graph-editor.spec.ts#baseline` (Angular) | pin basic behaviour |
| 6 | `agentflow-ui settings.spec.ts` | see WP02 (local storage) |

Test locations: `quad-quarkus/quad-ui/src/components/*.test.tsx`,
`quad-quarkus/quad-ui/src/services/*.test.ts`, `agentflow/agentflow-ui/src/app/**/*.spec.ts`.

**Acceptance (Red):** characterisation tests run green (current state); duplicate tests (3, 4) expose
the duplicates.

## 2. Changes (Green)

1. **Break up the composer:** into subcomponents/hooks with clear responsibilities:
   canvas, inspector, run panel, backend selector, materialisation. Move shared state into a
   reducer/context (building on the existing `WorkflowContext`).
2. **API client:** remove `fetchSkills` **or** `fetchSkillList` (update callers);
   extract `toFlatAgentRequest(agent)` for create+update.
3. **Angular editor:** split `graph-editor.ts` into smaller building blocks/services.
4. **Coverage:** add tests for the composer's central interactions (goal: the critical paths are
   covered, not 100%).

## 3. Verification

```bash
cd quad-quarkus/quad-ui && npm test
cd agentflow/agentflow-ui && npm test
# Builds
cd quad-quarkus/quad-ui && npm run build
cd agentflow/agentflow-ui && npm run build
```
- Manual visual check: composer functions unchanged (fork/merge, insert agent, start run).

## 4. Risks / notes

- Pure refactoring → characterisation tests first, freeze behaviour.
- `quad-ui` uses context + `useState` (no store): preserve consistency when splitting, do not
  over-engineer.

## Definition of Done

- [x] Characterisation tests present and green.
- [x] Composer split into clearly named units (no file > ~400 lines).*
- [x] API duplicates removed; shared agent mapper.
- [x] `graph-editor.ts` split.
- [x] Core paths tested; builds green.
- [x] Progress log maintained.

\* `WorkflowComposer.tsx` is 437 lines, `workflow/NodeInspector.tsx` 466 lines — both well below the
old sizes and below the guideline except for the two largest, most cohesive units; all other files
are < 270 lines. No *behaviour* change was made (see notes).

## Notes / follow-ups

- **Pre-existing bug found (out of scope, behaviour frozen):** in the Angular graph editor, editing a
  node's ID field updates the id but does **not** rewrite references, because `rename()` re-resolves
  `selected()` by the *old* `selectedId` after `ngModel` has already written the new id. A follow-up
  can fix the dangling reference. The behaviour is pinned by a characterisation test
  (`graph-editor.spec.ts`).
- The composer keeps the existing `WorkflowContext` + `useState` model (no new store / no
  over-engineering); shared data loading moved to the `useComposerData` hook.
- `toFlatAgentRequest()` was extracted verbatim (same 18 fields + defaults) so create/update stay
  byte-identical to before.

## Verification

```bash
cd quad-quarkus/quad-ui && npm test          # vitest run → 8 files, 46 tests passed
cd quad-quarkus/quad-ui && npm run lint      # oxlint → clean (1 pre-existing warning)
cd quad-quarkus/quad-ui && npm run typecheck # tsc app + tests → clean
cd quad-quarkus/quad-ui && npm run build     # vite build → OK
cd agentflow/agentflow-ui && npx ng test --watch=false --browsers=ChromeHeadless  # 10 passed
cd agentflow/agentflow-ui && npx ng build    # OK
```

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-15 | | 1. Characterisation tests | `api.test.ts` (skills + shared mapper), `WorkflowComposer.test.tsx` (+2), `graph-editor.spec.ts` (baseline + model) — green against the old code |
| 2026-10-15 | | 2. Refactoring | quad-ui: composer 1,669 → 437 lines via `components/workflow/*` + `useComposerData`; `api.ts` dedup (`toFlatAgentRequest`, drop `fetchSkillList`). agentflow-ui: `graph-editor.ts` 593 → 287 lines via `graph-model.ts` + external template/scss |
| 2026-10-15 | | 3. Verification | quad-ui 46 tests + lint + typecheck + build green; agentflow-ui 10 tests + build green |
