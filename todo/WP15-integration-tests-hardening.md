# WP15 — Harden integration tests

| | |
|---|---|
| **Status** | open |
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
- **Weak assertion:** `LastSystemtest.rueckstandIstSichtbar` only checks `isNotNegative()`.

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

## Definition of Done

- [ ] README == reality (numbers, `syscapability`, 3 findings).
- [ ] No more `Thread.sleep` polling; ports configurable.
- [ ] `stackMussLaufen` de-duplicated.
- [ ] Assertion sharpened.
- [ ] Counting script present (CI-ready).
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Counting script | |
| | | 2. Awaitility/ports | |
| | | 3. README regenerated | |
