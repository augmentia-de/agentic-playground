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

Voraussetzung ist das `deps.tar.gz`: der Worker haengt an `agent-common` (dieses
Repo) sowie `quad-core` und `quad-tool-builtin` (quad-quarkus) — allesamt lokal
gebaute Artefakte ohneoeffentliches Repository. Es wird erzeugt mit:

```bash
mvn -DskipTests install                    # agent-common
(cd ../quad-quarkus && mvn -DskipTests install)
./scripts/build-deps-tarball.sh
```

Das Skript prueft, ob die installierten Artefakte juenger als der Quellcode sind
und bricht sonst mit einem klaren Hinweis ab. Ein aelteres `agent-common` zeigt
sich im Image sonst nur als kryptische Kompilierfehlermeldung.

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

## Verifizierter Stand

Erstmals Ende-zu-Ende mit echtem LLM geprüft (2026-09-29). Ein Workflow
(`guard-skip`) lief über Kafka durch Orchestrator und quad-workflow-worker
bis `COMPLETED`: 2 echte LLM-Aufrufe, 55 s Wanduhr, Guard hat einen Schritt
übersprungen, Idempotenz über den Schlüssel bestätigt.

Dabei behoben — die Plattform **startete vorher nicht**:

| Befund | Wirkung |
|---|---|
| `@SpringBootApplication` scant nur `com.agentplatform.orchestrator` | Startabbruch: `EmailInitiatorStore` (Paket `com.agentplatform.bridge`) wurde nie als Bean registriert |
| `KafkaProducer` verlangte einen Jackson-2-`ObjectMapper` | Startabbruch (das Projekt nutzt Jackson 3) |
| `ProducerInterceptor` verlangte hart einen `Tracer` | Startabbruch; Tracing ist jetzt optional |
| `interceptor.classes` verwies auf eine nicht existierende Klasse | **jeder** Kafka-Publish scheiterte |
| Kafka erzeugt `ProducerInterceptor` reflektiv mit No-Arg-Konstruktor | Publish weiterhin unmöglich; Header-Erzeugung wandert in den Publisher |
| `task-topic-pattern: agent\\.tasks\\..*` in YAML | Backslashes sind in YAML literal → Wildcard matchte **0 Topics**, Worker blieb idle |
| Worker kannte 3 von 10 `agentType`s | jeder geseedete Workflow scheiterte mit „Unbekannter AgentTyp" |
| Worker verlangte `DataSource` auch im YAML-Modus | Startabbruch trotz Zusage „ohne Datenbank startfähig" |
| DECISION-/Guard-Bedingungen der ausgelieferten Workflows nutzten `context[...].resultData.feld` | Zwischenlevel existiert im Kontext nicht → alle Zweige fielen auf `defaultTarget` |
| `OrchestratorLeaseService` band `java.time.Instant` untypisiert | Lease immer `false` — Sweep lief auf jeder Replica |
| Zeitstempel teils aus der Uhr, teils aus der Datenbank | `updated_at` wurde gegen einen Schnitt aus der Uhr verglichen |

Beim anschließenden Live-Test des Wegs aus der Oberfläche
(`POST /api/v1/unified/workflows/from-quad`) kamen vier weitere Fehler dazu, die
alle erst unter realem HTTP-/Container-Betrieb auftraten und kein Unit-Test
sehen konnte:

| Befund | Wirkung |
|---|---|
| Canvas kennt keinen `end`-Knoten, der Orchestrator verlangt ihn (R6) | **jeder** Workflow aus der Oberfläche wurde mit `R6: no END node` abgewiesen |
| `UnifiedApiGateway` importierte Jackson 2, Spring Boot 4 konvertiert mit Jackson 3 | jeder Aufruf von `/from-quad` endete mit HTTP 500 `Cannot construct instance of JsonNode` |
| `IntakeListener`/`ConfigUpdateConsumer` riefen `acknowledge()` unter `AckMode.RECORD` | `IllegalStateException` pro Datensatz, zehn Wiederholungen, Eintrittsmeldung nie regulär verarbeitet |
| `AutoScalingService` castete `bootstrap.servers` auf `String`, der Kafka-Client liefert `List` | Auto-Scaling scheiterte alle 30 s an `ClassCastException` — nie ausgewertet |

Der Weg aus der Oberfläche ist danach Ende-zu-Ende geprüft: UI-Payload →
`WorkflowDefinitionBridge` → Validierung → `RoutingEvaluator` → Worker →
`COMPLETED`, inklusive `SKIPPED` am nicht gewählten Zweig und dem synthetisch
ergänzten `__end`.

Einzelheiten und Regressionstests zu jedem Punkt stehen in den Testklassen
der jeweiligen Module.

## Not yet production-complete

- Confluent Schema Registry serialization (es läuft bisher nur die JSON-Schema-Registrierung)
- JWT/OIDC API security
- Vault secrets and Kafka TLS/SASL
- ~~multi-instance orchestrator lease~~ — Lease ist verdrahtet (`SweepReaperService`), mit `SweepReaperLeaseTest` abgesichert
- Prometheus metrics/dashboards (Metriken erzeugt, Dashboards ungetestet)
- HITL UI
- production hardening of the external `ai-agent-service` worker (dedicated agent definitions, schema registry, real LLM E2E)
