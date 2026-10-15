# WP16 — All tests must match Surefire includes

| | |
|---|---|
| **Status** | done |
| **Severity** | Medium |
| **Modules** | `integration-tests` |
| **Review reference** | — (found while running the WP15 test campaign) |
| **Depends on** | WP15 |
| **Estimated effort** | 0.5 day |

## Goal

The command the README calls "everything" (`mvn -o test -Pe2e`) must actually execute
every test class that `count-tests.sh` counts. Today it silently drops the whole
`syscapability` layer.

## Finding (as-is)

Maven Surefire only collects classes whose **file name** matches one of its default
includes:

```
Test*.java   *Test.java   *Tests.java   *TestCase.java
```

The six `syscapability` classes were named `…Systemtest` (lowercase `t`) — no match. They
were therefore **never started** by `mvn -o test -Pe2e`; they only ran when explicitly
selected with `-Dtest=…`.

Consequences observed:

- `mvn -o test -Pe2e` on a live stack executed **58** tests (L0 18, L1 14, L2 11, L3 8+7),
  not the 104 the README table announces. The `syscapability` layer (46 tests) was absent.
- `count-tests.sh` counts `@Test` lines in the **source**, so it cannot see this gap. Its
  `--check` drift gate stayed green while the documented command ran half the suite. The
  README even credits the generator with "gaining" the 46 tests — a number that was never
  backed by an execution path.
- This is the same failure mode the WP13/WP15 DoD forbids: a documented claim without a
  reachable call path.

Side observation (out of scope): `quad-quarkus/quad-core/src/test/java/de/augmentia/quad/core/e2e/scenarios/CodeActScenario.java`
is a real LLM scenario test that also matches no Surefire include (and has no `@Disabled`
guard). It sits in another module and is not part of this fix; it is left as an explicitly
manual scenario.

## 1. Tests first (Red)

Make the gap machine-checkable instead of relying on reading the surefire output. Extend
`integration-tests/scripts/count-tests.sh` with an include check: every source file that
contains an `@Test` line must have a file name that matches a Surefire default include,
otherwise `--check` exits non-zero and names the offenders.

Red run (before the rename):

```bash
cd integration-tests && ./scripts/count-tests.sh --check
# count-tests: AusdrucksfaehigkeitSystemtest.java enthaelt @Test, matcht aber kein Surefire-Default-Include
# ... (6 lines, one per class)
# FEHLER: mindestens eine @Test-Klasse wird von Surefire nie eingesammelt.
# EXIT=1
```

## 2. Change (Green)

- `git mv` the six files `…Systemtest.java` → `…SystemTest.java` and rename the classes
  (so they match `*Test.java`, the conventional and Documentation-friendly form).
- The `TestEnv` Javadoc and the `integration-tests/pom.xml` comment that name
  `SicherheitSystemtest` are updated.
- The README table is regenerated and the `syscapability/*Systemtest` glob becomes
  `syscapability/*SystemTest`.

## 3. Verification

```bash
cd integration-tests
./scripts/count-tests.sh --check     # green: table current AND all classes Surefire-collectable
mvn -o test                          # default profile (L0): 18 green
```

Against a real isolated stack (own Kafka/Postgres/Redis; orchestrator `:28080`, worker
`:28081`, quad `:28086`):

```bash
mvn -o test -Pe2e \
  -Dit.orchestrator.url=http://localhost:28080 \
  -Dit.quad.url=http://localhost:28086 \
  -Dit.worker.url=http://localhost:28081 \
  -Dit.kafka.bootstrap=localhost:9092 \
  -Dit.orchestrator.log=/tmp/opencode/run/it/orch.log
```

Result: **the plain `-Pe2e` command now collects 104 tests** (was 58). The only failures are
the three known, pre-existing `KafkaContractTest` (L2) environment failures — the same that
occur on the unmodified code (broker provisioned ad-hoc, not like the documented dev stack;
see README Finding 1). Every `syscapability` class runs and passes.

## 4. Risks / notes

- Renaming test classes is behaviour-neutral; the assertions, tags and counts are unchanged.
- The new `count-tests.sh` gate is stack-free and deterministic, so CI catches any future
  class that is counted but never collectable.

## Result

- **Red:** extended `count-tests.sh` with a Surefire-include check → `--check` fails and
  names all six `…Systemtest` classes.
- **Green:** the six classes are now `…SystemTest`; `--check` passes.
- **Proof:** `mvn -o test -Pe2e` went from **58 → 104** collected tests against the live
  stack; only the three pre-existing L2 environment failures remain.
- **Docs:** README table + glob, the new check's description, `pom.xml`/`TestEnv` references
  and the WP15 class-name references are updated.

## Definition of Done

- [x] Deterministic, stack-free regression test for the defect (`count-tests.sh --check`).
- [x] `mvn -o test -Pe2e` executes all 104 tests (was 58).
- [x] No stale `Systemtest` references in code, pom, or docs.
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-15 | | 1. Red | `count-tests.sh --check` fails, names the 6 `…Systemtest` classes |
| 2026-10-15 | | 2. Rename | six classes → `…SystemTest`; `--check` green |
| 2026-10-15 | | 3. Verify | plain `-Pe2e` collects 104 (was 58); syscapability green |
