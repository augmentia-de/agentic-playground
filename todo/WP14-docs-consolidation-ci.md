# WP14 — Consolidate docs & CI

| | |
|---|---|
| **Status** | open |
| **Severity** | Medium |
| **Modules** | `kafka-platform` (docs), root, `.github` |
| **Review reference** | A3, A4, §9 |
| **Depends on** | – (mirror the doc parts after WP13) |
| **Estimated effort** | 2 days |

## Goal

Documentation describes the **current state**; test counts come from CI, not from prose.
The monorepo is built/tested **as a whole**.

## Findings (as-is)

- **A4 – no monorepo-wide CI:** the only CI is `kafka-platform/.github/workflows/ci.yml`
  (`kafka-platform` only). `quad-quarkus` and `agentflow` have none.
- **§9 – doc drift:** six top-level docs in `kafka-platform` mention non-existent packages
  (`com.agentplatform.*`, `com.platform.*`), modules and test counts; `spec.md` is an AI artefact
  (traceback, duplicated spec); `IMPLEMENTATION_COMPLETE.md` mentions
  `TracingProducerInterceptor.java` (does not exist), test counts 41/84; `IMPL_STATUS.md` claims the
  email responder/Vault/alerting are "implemented"; `README.md` says "359", `TODO.md` "270".
- **A3 – root aggregator without a parent** (`pom.xml:3-19`) → no central dependency/plugin management.
- **`TODO2.md`** (untracked, 918 lines) is a real issue tracker outside version control.

## 1. Tests/validation first

There are no unit tests in the narrow sense here — the "tests" are **doc checks**:

| # | Check | Expectation |
|---|-------|-------------|
| 1 | `ci.yml` runs for `quad-quarkus` + `agentflow` + root | green on PR |
| 2 | Test counts in the README == CI report | generated/linked from CI |
| 3 | Package names in docs == `grep '^package '` reality | no `com.agentplatform`/`com.platform` |
| 4 | Referenced files/classes exist | `TracingProducerInterceptor` does not exist → remove |
| 5 | `spec.md` removed/replaced | no AI artefact in the repo |

## 2. Changes (Green)

1. **Extend CI:** add workflows for `quad-quarkus` (`mvn -pl … test`), `agentflow`
   (`mvn -pl agentflow-app test`) and a root job (`mvn -T1C clean install`); write the test counts into
   `$GITHUB_STEP_SUMMARY`.
2. **Consolidate docs:** one canonical status document per module; remove redundant/contradictory ones
   (merge `IMPLEMENTATION_COMPLETE.md` vs. `IMPL_STATUS.md`). Delete `spec.md` (or move it to
   `docs/history/`).
3. **Fix package/file references**; replace test counts with a CI-linked figure.
4. **Align claims with code** (with WP13): "implemented" only when reachable.
5. **Convert `TODO2.md` → tracked issues** (GitHub Issues or `todo/` WPs); the most important findings
   (empty workflow accepted, KafkaTopicInitializer create-only, concurrency) are already captured as
   `@Disabled` tests.
6. **Root parent (A3, optional):** check whether a `dependencyManagement`/BOM at the root is possible
   without abandoning the module parents — document as a proposal, do not rush the refactor.

## 3. Verification

- CI PR green for all three reactors.
- `grep -rn 'com\.agentplatform\|com\.platform' kafka-platform/*.md` → no hits.
- `grep -rn 'TracingProducerInterceptor'` → no hits (except the changelog).

## 4. Risks / notes

- Doc changes are extensive → smaller PRs per file so reviews stay possible.
- CI runtime: Kafka tests need Docker (see the existing `ci.yml`).

## Definition of Done

- [ ] CI present for all reactors and green.
- [ ] Test counts CI-generated.
- [ ] No wrong package/file references.
- [ ] `spec.md` removed; status docs consolidated.
- [ ] `TODO2.md` findings tracked.
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. CI extended | |
| | | 2. Docs corrected | |
| | | 3. TODO2 migrated | |
