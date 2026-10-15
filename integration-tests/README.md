# Integration Tests — kafka-platform × quad

Tests against the **finished artifacts**: packaged JARs, a real Kafka broker, a real Postgres,
real HTTP calls. No in-process access, no mock interfaces in the test path — only the
worker itself runs in mock mode, because without an LLM key nothing would run deterministically.

## Quickstart

```bash
# alles: Stack hochfahren, alle Tests, Stack wieder runter
./scripts/run-integration-tests.sh e2e

# nur L0+L1+L2 (Broker + Orchestrator genügen)
./scripts/run-integration-tests.sh

# Stack dauerhaft hochfahren, in einer anderen Shell testen
./scripts/run-integration-tests.sh --start
mvn -o test -Pe2e
./scripts/run-integration-tests.sh --stop
```

The runner sets the bridge's `--quarkus.base-url` to the quad port. Without that, the
proxy path of the Unified API fails with 502 — see
[configuration pitfalls](#configuration-pitfalls).

### Prerequisites

| Component | Expected | Variable |
|---|---|---|
| Kafka | `localhost:9092` | `KAFKA_BOOTSTRAP_SERVERS` |
| Postgres | `jdbc:postgresql://localhost:5432/ap` | `POSTGRES_URL` |
| Orchestrator | Port `18080` | `ORCH_PORT` |
| Worker | Port `18081` | `WORKER_PORT` |
| quad | Port `18086` | `QUAD_PORT` |

The ports deliberately sit next to the defaults (`8080`, `8086`, `8092`) so that a running
developer stack does not get hit by mistake. The runner does **not** start Kafka and Postgres — they
are the shared infrastructure that the developer stack also uses.

Fresh is preferred: **a dedicated broker**. The reason is in [Finding 1](#finding-1) — the
observations depend on whether a topic already existed beforehand.

## Structure

Four layers. Each builds on the previous one and checks a different class of errors.

| Layer | Test | Environment | Checks |
|---|---|---|---|
| **L0** | `l0contract/PlatformContractTest` | none | Topic names, partitioning, task/result event JSON, DAG validator, event roundtrip |
| **L1** | `l1api/OrchestratorApiTest` | Orchestrator | Health, create/reject definitions, start, idempotency, 404, checkpoints, HITL, metrics |
| **L2** | `l2kafka/KafkaContractTest` | + Broker | Topics against specification, intake/task/result events, message flow |
| **L3** | `l3e2e/PlatformEndToEndTest` | + Worker | real runs: end, HITL gate, approval, cancellation, task and result events |
| **L3** | `l3e2e/CrossPlatformWorkflowTest` | + quad | quad definition → bridge → kafka execution, both backend paths |
| **Sys** | `syscapability/*Systemtest` | + Worker | capability questions: concurrency, failures, load, security, expressiveness |

If a dependency is missing, the layer skips via AssertJ's `assumeThat` (centralised in
`support/Stack`) — the build stays green and says what was missing. L0 always runs.

## Profiles

```bash
mvn -o test              # L0 (Standard: alles außer Netzwerk)
mvn -o test -Pintegration   # L0 + L1 + L2
mvn -o test -Pe2e            # alles
```

Controlled via tags: `requires-orchestrator`, `requires-kafka`, `requires-e2e`.

**Careful when reading the summary.** If the environment is not running, Surefire reports
`Tests run: 18, Skipped: 0` — even though L1 and L2 were definitely involved. They skip in
`@BeforeAll` via an assumption and therefore count as **0 tests**, not as "skipped".
The report shows this as an empty class:

```
 18 tests   de.augmentia.it.l0contract.PlatformContractTest
  0 tests   de.augmentia.it.l1api.OrchestratorApiTest      ← skipped by assumption
  0 tests   de.augmentia.it.l2kafka.KafkaContractTest      ← skipped by assumption
```

So anyone who sees "18 green" should double-check whether the environment was up. The runner
therefore starts it itself instead of relying on that.

## Current Status

<!-- test-counts:start -->
| Layer | Test class | @Test | @Disabled |
|-------|------------|------:|----------:|
| L0 Contract | `de/augmentia/it/l0contract/PlatformContractTest.java` | 18 | 0 |
| L1 API | `de/augmentia/it/l1api/OrchestratorApiTest.java` | 14 | 0 |
| L2 Kafka | `de/augmentia/it/l2kafka/KafkaContractTest.java` | 11 | 1 |
| L3 E2E | `de/augmentia/it/l3e2e/CrossPlatformWorkflowTest.java` | 7 | 1 |
| L3 E2E | `de/augmentia/it/l3e2e/PlatformEndToEndTest.java` | 8 | 0 |
| Syscapability | `de/augmentia/it/syscapability/AusdrucksfaehigkeitSystemtest.java` | 9 | 0 |
| Syscapability | `de/augmentia/it/syscapability/FaehigkeitenSystemtest.java` | 8 | 0 |
| Syscapability | `de/augmentia/it/syscapability/LastSystemtest.java` | 4 | 0 |
| Syscapability | `de/augmentia/it/syscapability/NebenlaeufigkeitSystemtest.java` | 9 | 1 |
| Syscapability | `de/augmentia/it/syscapability/SicherheitSystemtest.java` | 8 | 0 |
| Syscapability | `de/augmentia/it/syscapability/StoerfaelleSystemtest.java` | 8 | 0 |
| **Total** |  | **104** | **3** |
<!-- test-counts:end -->

**104 tests, 3 `@Disabled` findings.** The table is generated from the sources by
`./scripts/count-tests.sh`; `./scripts/count-tests.sh --check` fails when README and code
drift apart. It used to be maintained by hand and named 58 tests / 2 findings.
L1–L3 and Syscapability each skip when the environment is not up (see Profiles).

---

# Findings

Three real bugs in the production code. All three are formulated as a `@Disabled` test — the
assertion is the correct expectation and becomes active as soon as the bug is fixed. For each,
there is also a green test that records the current state: the finding is thus not only noted,
but measurable. As soon as the behavior changes, the green as-is test fails **first** and
shows the change.

## Finding 1 — whoever publishes first defines the topic

`KafkaTopicInitializer` is **create-if-missing**:

```java
Set<String> existing = adminClient.listTopics().names().get();
List<NewTopic> missing = specs.stream()
        .filter(spec -> !existing.contains(spec.name()))   // ← vorhandene werden übergangen
        .map(KafkaTopicInitializer::toNewTopic)
        .toList();
```

A topic that already exists keeps its broker settings — without a hint, without a log.
Observed on a broker that had already been used before:

| Topic | Spec | Broker |
|---|---|---|
| `agent.tasks.*` (10 Topics) | 12 | **12** |
| `agent.control.timeouts` | 6 | **6** |
| `agent.execution.dlq` | 6 | **6** |
| `agent.control.sweep` | 1 | 1 |
| `agent.execution.intake` | 12 | **1** |
| `agent.execution.results` | 12 | **1** |
| `agent.control.cancellation` | 6 | **1** |
| `agent.control.hitl` | 6 | **1** |
| `agent.control.loop` | 6 | **1** |
| `agent.config.updates` | compact | **delete** |

The pattern is unambiguous: `agent.tasks.*`, `timeouts` and `dlq` did not exist yet on the first
start and received the specification. Everything else had already been touched by someone
beforehand — the broker created it with one partition and `cleanup.policy=delete`, and the
initializer accepted that without comment.

**Why this is more than cosmetic.** Two consequences that weigh differently:

- **Parallelism.** `INTAKE` and `RESULTS` are supposed to have 12 partitions. With one partition,
  the entire platform runs serialized. This does not stand out, because the tests keep running —
  it just gets slower.
- **Compaction.** `config.updates` is defined as `compact` so that only the latest configuration
  per `agent_type` is retained. With `delete`, the topic instead grows until retention kicks in.
  The cases are not the same, but the data volume is bounded.

The test checks this topic by topic against `KafkaTopics.all()` — not hand-picked per topic, because
the list changes with every run. That is why the error message always names the complete
set of deviations.

**Fix direction:** The initializer should reconcile instead of only creating — `createPartitions` for
too few partitions, `alterConfigs` for `cleanup.policy` and `retention.ms`. Both are
idempotent admin operations.

Tests: `brokerMatchesTopicSpecification` (`@Disabled`), `driftIsLimitedToTopicsCreatedByTheBroker`,
`workerTopicsAreFullyPartitioned`, `configUpdatesCompactionComesFromTheTopicCreator`.

## Finding 2 — quad reports an empty workflow as `completed`

`POST /api/ui/workflows` with `nodes: []` is accepted. The workflow can be started, and
the run ends with status `completed`.

This makes `completed` meaningless — simply nothing ran. A workflow without nodes should be
rejected on creation, or at the latest on start. Anyone who checks for `completed` to signal
success gets a false yes here.

Tests: `quadRejectsEmptyWorkflow` (`@Disabled`), `emptyWorkflowRunsAndReportsCompleted`.

## Finding 3 — idempotency under contention returns HTTP 500

`WorkflowStartService.start` is `@Transactional` and catches the `DuplicateKeyException` from
`executions.create` in order to resolve the existing run in the same `catch`. In PostgreSQL a
constraint violation aborts the transaction (SQLSTATE `25P02`, *current transaction is
aborted*), so every further statement fails — the resolution in the `catch` can never succeed,
and the caller gets HTTP 500 instead of the existing run's id.

The path is reachable as soon as two starts with the same key arrive at the same time.
`StartIdempotencyGuard.reserve` already handles that race before the insert, which makes the
`catch` legacy from before the guard — and unreachable in the intended flow.

**Fix direction:** a separate transaction (`REQUIRES_NEW`) for the lookup, or drop the legacy
`catch` entirely.

Tests: `idempotenzUnterLastGibtKeinenDbFehler` (`@Disabled`), `idempotenzBeiNacheinander`,
`nachGleichstandGehtEsWeiter` (green — record that the ordered path keeps working).

---

# Configuration Pitfalls

Not bugs, but encountered during setup — recorded here so that nobody has to search for them again.

**The bridge's `quarkus.base-url`.** The orchestrator runs with
`quarkus.base-url=http://localhost:8086`. Quad runs on `8086` by default, in the test on
`18086`. Without an adjustment, `POST /api/v1/unified/workflows/from-quad` with
`X-Backend: quarkus` returns a `502` — the error message at least names the unreachable address.
The runner sets the argument, which is why the test is green. For real operation,
`quarkus.base-url` belongs in the configuration instead of pointing at the default.

**Worker port.** In `quad-workflow-worker/src/main/resources/application.yml`,
`server.port: 8092` is hard-coded, without `${...:-Default}` — so it is **not** overridable via the
environment. The runner sets `--server.port`. If the worker really is ever supposed to run on a
different port, that is one config line, not a system property.

**quad data directory.** quad creates its SQLite file as `data/quad.db`, resolved against
the process's **working directory**, not against the jar. The runner therefore starts quad
in `$RUN_DIR/quad`. Started from the module directory, Flyway does not find the file and
quad does not start at all.

**The legacy worker no longer listens on `agent.tasks.*`.** `agent-worker` has been switched to
`legacy\.agent\.tasks\..*`, because the canonical worker (`quad-workflow-worker`)
has taken over these topics. In the test the runner therefore starts
`quad-workflow-worker` — the `agent-worker` would be silent and the L3 tests would hang.

## Noted, Not Fixed as a Test

- **`TaskEvent.header` is not required for validation.** The field may be `null`
  (`TaskEvent.java`). For correlation and timeout monitoring, however, the header is the only
  way to assign a task to a run. L0 records today's contract; the question of whether that is
  intended belongs in the product decision.
- **The HITL approval path takes time.** After the retry of a paused run, the
  remaining steps continue asynchronously. The test waits for `COMPLETED` and must **not** have
  `PAUSED_HITL` in the wait list there — otherwise it returns immediately, before the approval
  has been processed.
