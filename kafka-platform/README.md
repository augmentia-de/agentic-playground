# Deterministic Kafka Workflow Platform

Event-driven workflow platform based on `SPEC2.md`.

Important: this repository pulls in **no LLM/Spring AI library**. `agent-common`, `agent-orchestrator` and `agent-worker` are deterministic and contain no model calls at all. The only module that talks to a model endpoint is `quad-workflow-worker`, and it does so over plain HTTP against an OpenAI-compatible API, degrading to a mock when no key is configured. Real agents can also live in another service and be integrated over the same Kafka task/result contract.

## Modules

- `agent-common`: shared domain model, Kafka topic/event contracts, deterministic routing evaluator, workflow validator.
- `agent-orchestrator`: Spring Boot service that owns routing state, checkpoints, HITL, retries, cancellation, sweeps, and the REST API.
- `agent-worker`: Spring Boot service with deterministic mock agents behind the `AgentExecutor` seam.
- `quad-workflow-worker`: Spring Boot worker for LLM agents over the same Kafka task/result contract, backed by `quad-core` and `quad-tool-builtin`. It talks to an OpenAI-compatible endpoint (`OPENAI_BASE_URL`, defaults to OpenRouter) and falls back to a mock `ChatModel` when no API key is set. It pulls no LLM/Spring AI library — `agent-common`, `agent-orchestrator` and `agent-worker` remain deterministic and LLM-free.
- `integration-tests` (`../integration-tests`): a separate project, not part of this
  reactor. Run it from its own directory with `mvn -o test -Pe2e` (see below).

## Stack

- Java 21
- Spring Boot 4.0.8
- Spring Kafka
- PostgreSQL 16 + Flyway
- Redis 7.2 for start idempotency
- Kafka KRaft

## Build

```bash
mvn verify
```

That runs the default reactor: 359 tests across the four modules, 5 of which are
key-gated and skipped unless `OPENAI_API_KEY` is set.

End-to-end tests live in `../integration-tests`, a separate project — they drive the
two Boot applications from the outside and need their fat jars, which are unusable as
libraries. They are therefore not in this reactor. Run them from their own directory:

```bash
cd ../integration-tests
mvn -o test              # L0, no network
mvn -o test -Pintegration
mvn -o test -Pe2e        # everything
```

## Run infrastructure

```bash
docker compose up -d kafka postgres redis
```

## Run locally

```bash
java -jar agent-orchestrator/target/agent-orchestrator-1.0.0-SNAPSHOT.jar
java -jar agent-worker/target/agent-worker-1.0.0-SNAPSHOT.jar
```

The orchestrator creates all Kafka topics on startup and seeds the workflow definitions from `agent-orchestrator/src/main/resources/workflows`.

## Run with containers

A prerequisite is `deps.tar.gz`: the worker depends on `agent-common` (this
repo) as well as `quad-core` and `quad-tool-builtin` (quad-quarkus) — all
locally built artifacts with no public repository. It is generated with:

```bash
mvn -DskipTests install                    # agent-common
(cd ../quad-quarkus && mvn -DskipTests install)
./scripts/build-deps-tarball.sh
```

The script checks whether the installed artifacts are newer than the source code
and otherwise aborts with a clear hint. An older `agent-common` would otherwise
only show up in the image as a cryptic compiler error message.

```bash
mvn -DskipTests package
docker compose up -d --build
```

## Seeded workflows

| Workflow | Demonstrates |
| --- | --- |
| `reference-code-quality` | STEP, DECISION, LOOP, FORK, AND JOIN, HITL approval, END |
| `xor-race` | FORK + XOR JOIN, loser pruning |
| `n-of-quorum` | FORK + N_OF JOIN, quorum pruning |
| `guard-skip` | STEP guard, SKIPPED propagation |
| `error-taxonomy` | retryable failure, fatal failure, NEEDS_HITL, branch pruning, cancel/skip |

## Start a workflow

```bash
curl -X POST http://localhost:8080/api/v1/workflows/start \
  -H 'Content-Type: application/json' \
  -d '{
    "workflowName": "reference-code-quality",
    "inputParams": {"researchConfidence": 0.72},
    "idempotencyKey": "demo-001"
  }'
```

Poll:

```bash
curl http://localhost:8080/api/v1/workflows/{workflowExecutionId}
```

## HITL

List open tickets:

```bash
curl 'http://localhost:8080/api/v1/hitl/tickets?status=OPEN'
```

Approve/retry, skip, or abort:

```bash
curl -X POST http://localhost:8080/api/v1/hitl/tickets/{ticketId}/retry \
  -H 'Content-Type: application/json' \
  -d '{"userId":"operator"}'

curl -X POST http://localhost:8080/api/v1/hitl/tickets/{ticketId}/skip \
  -H 'Content-Type: application/json' \
  -d '{"userId":"operator"}'

curl -X POST http://localhost:8080/api/v1/hitl/tickets/{ticketId}/abort \
  -H 'Content-Type: application/json' \
  -d '{"userId":"operator"}'
```

For an approval gate, `retry` resumes/dispatches the waiting step.

## Mock behavior

The deterministic mock agent derives output from a SHA-256 hash of workflow, task, step, iteration, attempt, and prompt. It supports test hooks in `inputParams`:

```json
{
  "researchConfidence": 0.72,
  "runLint": false,
  "mode": "retryable",
  "failAttempts": {"b.retryable.task": 1},
  "forceStatus": {"b.fatal.task": "FAILED_FATAL"},
  "errorMessage": {"b.fatal.task": "boom"},
  "resultDataOverrides": {"prepare": {"custom": true}}
}
```

## Kafka topics

Created from `KafkaTopics` / `TopicNames` per SPEC2 §4.1:

- `agent.execution.intake`
- `agent.tasks.<agent_type>`
- `agent.execution.results`
- `agent.control.loop`
- `agent.control.sweep`
- `agent.control.cancellation`
- `agent.control.hitl`
- `agent.control.timeouts`
- `agent.execution.dlq`
- `agent.config.updates`

All workflow-scoped topics are keyed by `workflow_id`. Sweep uses `sweep_id`; timeouts use `task_id`.

## Deterministic routing

`RoutingEvaluator` is pure:

- no wall clock
- no randomness
- no HTTP
- no DB writes
- sorted by `(level, stepId)`
- DECISION persists `decision_branch`
- guard-false becomes `SKIPPED`
- unchosen DECISION branches become `SKIPPED`
- XOR/N_OF losers become `PRUNED`
- replaying the same persisted state produces the same routing actions

## Optional external LLM worker: `ai-agent-service`

This repository stays LLM-free. The separate `ai-agent-service` project can act as a real worker over the same Kafka contract:

```text
agent-orchestrator -> agent.tasks.<agentType> -> ai-agent-service -> agent.execution.results -> agent-orchestrator
```

Run only one worker implementation per agent type. To replace the deterministic mock worker:

1. Stop `agent-worker`.
2. Start `ai-agent-service` with a valid LLM key and workflow mode enabled:

```bash
export OPENAI_API_KEY=...
export AGENT_KAFKA_ENABLED=false
export AGENT_KAFKA_WORKFLOW_ENABLED=true
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export AGENT_KAFKA_WORKFLOW_GROUP_ID=ai-agent-service-workflow-worker
export AGENT_KAFKA_WORKFLOW_AUTO_OFFSET_RESET=latest
java -jar ai-agent-service/target/ai-agent-service-1.0.0-SNAPSHOT.jar
```

3. Map orchestrator `agentType` values to ai-agent-service agent IDs:

```properties
agent.kafka.workflow.default-agent-id=general-workflow-agent
agent.kafka.workflow.agent-id-mapping.researcher=fakten-checker
agent.kafka.workflow.agent-id-mapping.coder=code-assistant
agent.kafka.workflow.agent-id-mapping.reviewer=review-agent
```

The worker sends JSON task context to the selected agent and expects a JSON object back. Useful result fields are:

```json
{
  "response": "agent answer",
  "confidence": 0.87,
  "status": "COMPLETED",
  "errorMessage": null
}
```

`status` may be `COMPLETED`, `FAILED_RETRYABLE`, `FAILED_FATAL`, or `NEEDS_HITL`. Without an explicit status, successful responses are reported as `COMPLETED`.

## Verified status

Verified end-to-end with a real LLM for the first time (2026-09-29). One workflow
(`guard-skip`) ran over Kafka through the orchestrator and quad-workflow-worker
to `COMPLETED`: 2 real LLM calls, 55 s wall clock, the guard skipped one step,
idempotency confirmed via the key.

Fixed along the way — the platform **did not start before**:

| Finding | Effect |
|---|---|
| `@SpringBootApplication` scans only `com.agentplatform.orchestrator` | Startup abort: `EmailInitiatorStore` (package `com.agentplatform.bridge`) was never registered as a bean |
| `KafkaProducer` required a Jackson 2 `ObjectMapper` | Startup abort (the project uses Jackson 3) |
| `ProducerInterceptor` hard-required a `Tracer` | Startup abort; tracing is now optional |
| `interceptor.classes` pointed to a non-existent class | **every** Kafka publish failed |
| Kafka creates `ProducerInterceptor` reflectively with a no-arg constructor | Publish still impossible; header creation moves into the publisher |
| `task-topic-pattern: agent\\.tasks\\..*` in YAML | Backslashes are literal in YAML → the wildcard matched **0 topics**, the worker stayed idle |
| The worker knew 3 of 10 `agentType`s | every seeded workflow failed with "Unknown agentType" |
| The worker required `DataSource` even in YAML mode | Startup abort despite the promise "can start without a database" |
| DECISION/guard conditions of the delivered workflows used `context[...].resultData.feld` | the intermediate level does not exist in the context → all branches fell through to `defaultTarget` |
| `OrchestratorLeaseService` bound `java.time.Instant` untyped | lease always `false` — the sweep ran on every replica |
| Timestamps partly from the clock, partly from the database | `updated_at` was compared against a cut from the clock |

In the subsequent live test of the path from the UI
(`POST /api/v1/unified/workflows/from-quad`), four more bugs came to light, all
of which only appeared under real HTTP/container operation and that no unit test
could have caught:

| Finding | Effect |
|---|---|
| The canvas knows no `end` node, the orchestrator requires one (R6) | **every** workflow from the UI was rejected with `R6: no END node` |
| `UnifiedApiGateway` imported Jackson 2, Spring Boot 4 converts with Jackson 3 | every call to `/from-quad` ended with HTTP 500 `Cannot construct instance of JsonNode` |
| `IntakeListener`/`ConfigUpdateConsumer` called `acknowledge()` under `AckMode.RECORD` | `IllegalStateException` per record, ten retries, the intake message never processed normally |
| `AutoScalingService` cast `bootstrap.servers` to `String`, the Kafka client returns `List` | auto-scaling failed every 30 s with `ClassCastException` — never evaluated |

The path from the UI is then verified end-to-end: UI payload →
`WorkflowDefinitionBridge` → validation → `RoutingEvaluator` → worker →
`COMPLETED`, including `SKIPPED` on the unchosen branch and the synthetically
added `__end`.

Details and regression tests for each point are in the test classes
of the respective modules.

## Not yet production-complete

- Confluent Schema Registry serialization (so far only JSON schema registration runs)
- JWT/OIDC API security
- Vault secrets and Kafka TLS/SASL
- ~~multi-instance orchestrator lease~~ — the lease is wired up (`SweepReaperService`), covered by `SweepReaperLeaseTest`
- Prometheus metrics/dashboards (metrics produced, dashboards untested)
- HITL UI
- production hardening of the external `ai-agent-service` worker (dedicated agent definitions, schema registry, real LLM E2E)
