# WP15 — Harden integration tests

| | |
|---|---|
| **Status** | done |
| **Severity** | Medium |
| **Modules** | `integration-tests` |
| **Review reference** | §8.5, §B (README drift), §3.1 |
| **Depends on** | – |
| **Estimated effort** | 1–2 days |

## Goal

The integration tests are **low-flakiness**, their README matches the code, and the `@Disabled`
findings are visibly documented.

## Findings (as-is)

- **README drift:** the README says "58 tests / 2 documented findings" (`:10,84-90`); reality is
  **104 `@Test`** and **3 `@Disabled`** findings; the entire **`syscapability` suite (46 tests)** is
  missing from the README table.
- **Flakiness:** `support/WorkflowRun.java` polls with `Thread.sleep(400)`; `support/TestEnv.java`
  has hard-coded ports; a possibly unused `uniqueKey` map.
- **Duplication:** the `stackMussLaufen` helper is copied 5–6× in `syscapability/*`.
- **Weak assertion:** `LastSystemTest.rueckstandIstSichtbar` only checks `isNotNegative()`.

## 1. Tests first

Here the integration tests themselves are the subject.

| # | Task | Expectation |
|---|------|-------------|
| 1 | counting script `scripts/count-tests.sh` | lists `@Test`/`@Disabled` per layer; README generatable from it |
| 2 | Awaitility instead of `Thread.sleep` | more deterministic waits |
| 3 | Ports from `TestEnv`/env instead of hard-coded | suite runnable in a different environment |
| 4 | centralise `stackMussLaufen` in `support/` | one implementation |
| 5 | sharpen `rueckstandIstSichtbar` | checks the real backlog value |
| 6 | regenerate the README table | includes `syscapability`, 3 findings |

## 2. Changes (Green)

1. **Align the README** (numbers + `syscapability` suite + 3 findings). Ideally generated from
   `count-tests.sh` (related to WP14 CI).
2. **`WorkflowRun`:** switch to Awaitility (`await().atMost(...).pollInterval(...)`) (add the
   dependency if missing).
3. **`TestEnv`:** make ports configurable; remove the dead `uniqueKey`.
4. **Centralise helpers:** a shared base class/utility for stack readiness.
5. **Sharpen the assertion** (real expected value).
6. **Document the finding pattern:** the `@Disabled` finding + companion test as a house style in the README.

## 3. Verification

```bash
cd integration-tests && ./scripts/run-integration-tests.sh e2e
./scripts/count-tests.sh
```
- Run the suite several times in a row → stably green (no sleep flakiness).
- In a different environment (different ports) → green.

## 4. Risks / notes

- The Awaitility switch changes timeouts → generous, but with a clear error message.
- The `syscapability` suite may need a running stack (document which layers skip).

## Result

- **Counting script:** new `integration-tests/scripts/count-tests.sh` counts `@Test`/`@Disabled`
  per class and layer. It reports **104 / 3** (the README said 58 / 2). The README table sits
  between `<!-- test-counts:start -->` markers and `--check` fails when README and code drift.
  CI-ready (`--check` is a one-line check).
- **Awaitility instead of `Thread.sleep`:** added `org.awaitility:awaitility` (test scope). The
  polling in `support/WorkflowRun.awaitStatus` and in `CrossPlatformWorkflowTest` now polls until
  the condition holds or a generous cap expires (last-seen status is still returned on timeout).
  No direct `Thread.sleep` remains in the test path.
- **Ports/endpoints configurable:** `TestEnv` reads system property → environment variable →
  default (`it.worker.url` and `it.orchestrator.log` are now also passed by the pom, and the
  runner passes the orchestrator log path). The unused `IDEMPOTENCY` map was removed.
- **Deduplicated:** the five `syscapability` copies of `stackMussLaufen` (plus
  `PlatformEndToEndTest`) now call `support/Stack.assumeUp()`; the orchestrator-only class calls
  `Stack.assumeOrchestratorUp()`.
- **Sharpened:** `LastSystemTest.rueckstandIstSichtbar` no longer asserts `endOffsets >= 0`
  (always true). It measures the topic growth and requires at least one new message per started run.
- **README:** regenerated from the script; the `syscapability` suite (46 tests) is now listed and
  the third finding (duplicate-key race, `NebenlaeufigkeitSystemTest`) is documented.

## Verification

```bash
cd integration-tests
./scripts/count-tests.sh --check     # README numbers == sources  -> green
mvn -o test                          # default profile (L0 only): 18 green
```

Against a real, isolated stack (own Kafka/Postgres/Redis containers; orchestrator on `:28080`,
worker on `:28081`, mock mode) the whole non-`quad` suite was run:

```bash
mvn -o test -Pe2e -Dtest='!KafkaContractTest' \
  -Dit.orchestrator.url=http://localhost:28080 \
  -Dit.worker.url=http://localhost:28081 \
  -Dit.kafka.bootstrap=localhost:9092 \
  -Dit.orchestrator.log=/tmp/opencode/wp15-it/orch.log
```

Result: **green** — L0 18, L1 14, L3 `PlatformEndToEndTest` 8, and the whole `syscapability`
suite 46 (45 green + the 1 `@Disabled` finding). `CrossPlatformWorkflowTest` reports 0 tests
(its `@BeforeAll` skips because `quad` was not running — the intended behaviour).
`LastSystemTest.rueckstandIstSichtbar` passed with the new growth assertion, i.e. Awaitility
polling and the sharpened check work against a live broker.

`KafkaContractTest` (L2) was excluded here: it fails on this ad-hoc broker for **pre-existing**
reasons (broker-created topic drift + a leftover intake message). The identical 3 failures occur
on the unmodified code (`git stash` + rerun), so they are **not** a WP15 regression; they need a
broker provisioned exactly like the documented dev stack (see README, Finding 1).

## Definition of Done

- [x] README == reality (numbers, `syscapability`, 3 findings).
- [x] No more `Thread.sleep` polling; ports configurable.
- [x] `stackMussLaufen` de-duplicated.
- [x] Assertion sharpened.
- [x] Counting script present (CI-ready).
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-15 | | 1. Counting script | `scripts/count-tests.sh` (+`--check`); reports 104 / 3 |
| 2026-10-15 | | 2. Awaitility/ports | Awaitility dep; `WorkflowRun`/`CrossPlatform` poll instead of `Thread.sleep`; env fallback + log path |
| 2026-10-15 | | 3. Dedupe + assert | `support/Stack`; backlog assertion measures growth; README regenerated with 3 findings |
