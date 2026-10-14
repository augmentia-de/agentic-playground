# WP12 — Frontend: quad-ui & agentflow-ui

| | |
|---|---|
| **Status** | open |
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

- [ ] Characterisation tests present and green.
- [ ] Composer split into clearly named units (no file > ~400 lines).
- [ ] API duplicates removed; shared agent mapper.
- [ ] `graph-editor.ts` split.
- [ ] Core paths tested; builds green.
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Characterisation tests | |
| | | 2. Refactoring | |
| | | 3. Verification | |
