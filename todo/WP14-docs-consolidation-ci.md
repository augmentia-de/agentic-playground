# WP14 — Consolidate docs & CI

| | |
|---|---|
| **Status** | done |
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

## Result

- **CI:** new root `.github/workflows/ci.yml` (active — GitHub only reads `.github/workflows/`
  at the repo root; the previous `kafka-platform/.github/workflows/ci.yml` was inert and has been
  removed). Jobs: `quad-quarkus`, `kafka-platform` (Docker/Testcontainers), `agentflow`,
  `quad-ui`, `agentflow-ui`, `integration-tests`. Per-job counts are written to the run
  summary by `.github/scripts/summarise-tests.sh`.
- **Docs:** `spec.md` (AI artefact) and `IMPLEMENTATION_COMPLETE.md` (superseded by
  `IMPL_STATUS.md`) deleted; `IMPL_STATUS.md` is the single canonical status doc and now
  carries a historical-log banner. All `com.agentplatform.*` / `com.platform.*` references
  replaced with `de.augmentia.agentplatform.*`; the non-existent `TracingProducerInterceptor`
  replaced by the real `TraceHeaders` / `OrchestratorEventPublisher`. Claims for the classes
  removed in WP13 (`PrefixCache`, `SlackNotifier`, `PagerDutyAlertSender`, `VaultConfig`,
  `SchemaRegistryClient`) are marked removed. Hard-coded test counts removed (README/TODO);
  counts now come from CI.
- **TODO2:** moved from the untracked repo root into version control as
  `todo/TODO2-quad-tooling-engine.md`, with a status/provenance header.
- **A3:** the aggregator-without-parent decision is already documented in `pom.xml` and now also
  in the root `README.md` as a proposal.

## Verification

```bash
# no wrong packages in the kafka top-level docs
grep -rn 'com\.agentplatform\|com\.platform' kafka-platform/*.md      # -> no hits
# no claim of the non-existent tracing class (only the review/spec describe its absence)
grep -rn 'TracingProducerInterceptor' --include=*.md .                # -> Review.md, WP14 only
# no hard-coded test counts in the kafka docs
grep -rnE '\b(359|270|244|101)\b' kafka-platform/*.md                 # -> no hits
# CI reactor selections resolve (quad-core is pulled via -am)
mvn -B -ntp validate -DskipTests -pl kafka-platform/agent-common,kafka-platform/agent-orchestrator,kafka-platform/agent-worker,kafka-platform/quad-workflow-worker -am
```

## Definition of Done

- [x] CI present for all reactors and green.
- [x] Test counts CI-generated.
- [x] No wrong package/file references.
- [x] `spec.md` removed; status docs consolidated.
- [x] `TODO2.md` findings tracked.
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-15 | | 1. CI extended | Root `.github/workflows/ci.yml` (6 jobs) + `.github/scripts/summarise-tests.sh`; inert `kafka-platform/.github/workflows/ci.yml` removed; YAML validated |
| 2026-10-15 | | 2. Docs corrected | `spec.md` + `IMPLEMENTATION_COMPLETE.md` deleted; packages, tracing class, WP13-removed claims and test counts fixed |
| 2026-10-15 | | 3. TODO2 migrated | `TODO2.md` → tracked `todo/TODO2-quad-tooling-engine.md` |
