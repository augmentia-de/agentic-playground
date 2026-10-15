# Agentic Platform — from a quad agent to a durable workflow platform

A monorepo with **one shared agent basis** (`quad-core`) and **three stages** of workflow
orchestration:

```
   Stage 1                Stage 2                       Stage 3
   quad-quarkus           kafka-platform                agentflow
   synchronous/in-process asynchronous/event-driven    durable
   (Quarkus)              (Kafka)                       (Temporal)
        │                      │                             │
        └──────────────────────┴─────────────────────────────┘
                               │
                    shared basis: quad-core
              (LangChain4j agent, ReAct, hooks, tools, MCP,
               guardrails, PII, HITL, observability)
```

The agents themselves do **not** change across the stages — only *who* orchestrates the
execution and *where* durability (retries, checkpoints, replay) lives.

---

## The shared basis: `quad-core`

`quad-core` (inside `quad-quarkus/`) is a **framework-neutral Java framework for LLM agents
built on LangChain4j**. It is the constant across all three stages: an agent is always assembled
the same way, regardless of whether it runs in Quarkus, in a Kafka worker or in a Temporal
activity.

```java
Agent agent = AgentBuilder.defaultAgent()
        .withModel(ModelFactory.createOpenAiFromEnv())   // OPENAI_*
        .withSystemPrompt("You are a research assistant.")
        .withToolObjects(new ReadFileTool(), new WebSearchTool())
        .withGuardrail(promptInjectionGuardrail())
        .withHook(piiHook())                              // GDPR/PII masking
        .withHook(new AuditTrailHook(store))
        .withEventPublisher(publisher)                    // metrics/tracing/logging
        .build();

String answer = agent.run(task, new AgentSessionState());
```

What `quad-core` provides (excerpt):

| Building block | Meaning |
|---|---|
| `AgentBuilder` / `AgentRuntime` | Agent assembly + ReAct loop (reason → act → observe) |
| **Hook pipeline** | six interception points (`before/after Agent/Model/Tool`) → guardrails, HITL, telemetry, resilience, GDPR as pluggable modules |
| Tools & MCP | built-ins (`readFile`, `grepSearch`, `webSearch`, …), `@Tool` methods, `SubAgentTool`, MCP bridge |
| Guardrails / PII / audit | `GuardrailPlugin`, `PiiAnonymizerHook`, `AuditTrailHook` |
| HITL | `HITLPlugin` + `CheckpointService` (SSE/Kafka/console channels) |
| Observability | typed `AgentEvent`s, `MetricsHook` (Micrometer), `TracingHook` (OTel), `ProvenanceTracker` |
| Model | `ModelFactory` (OpenAI-compatible, `OPENAI_*`/`QUAD_LLM_*`) |

**Who depends on `quad-core`:**

| Module | quad dependency |
|---|---|
| `quad-quarkus/quad-quarkus` | `quad-core` (direct, in-process) |
| `kafka-platform/quad-workflow-worker` | `de.augmentia.quad:quad-core` + `quad-tool-builtin` |
| `agentflow/agentflow-app` | `quad-core` + `quad-tool-builtin` + `quad-tool-mcp` |

> The root aggregator resolves `quad-core` **within the reactor**, so the Kafka worker never
> pulls a stale jar from `~/.m2` (see [Build & test](#build--test)).

---

## The path across three stages

### Stage 1 — `quad-quarkus`: synchronous, in-process (Quarkus + LangChain4j)

The agent runs **in the same process** as the application. Quarkus serves REST/SSE and the
React UI (`quad-ui`); the workflow engine (`quad.workflow` v2) executes DAGs with seven node
executors (`agent`, `decision`, `loop`, `fork`, `join`, `hitl`, `end`). Asynchrony only appears
where you ask for it — via messaging channels (Kafka/AMQP/email).

```
Client ──REST/SSE──► quad-quarkus (Quarkus)
                        │  AgentBuilder + AgentRuntime + hook pipeline   (quad-core)
                        │  workflow engine (quad.workflow v2)
                        └─ JDBC stores (SQLite/Postgres): runs, memory, audit, skills, checkpoints
```

- **Fits:** interactive, agent-centric applications with a UI, short runs, direct control over
  prompt/tools.
- **Limit:** orchestration and durability live inside the application process; scaling and load
  balancing a single run across instances is manual work.
- **Run:** `cd quad-quarkus && mvn quarkus:dev -Dquarkus.http.port=8083` (UI: `quad-ui`).
- Details: [`quad-quarkus/README.md`](quad-quarkus/README.md).

### Stage 2 — `kafka-platform`: asynchronous, event-driven (Kafka)

The **orchestration is decoupled from the agent**. A deterministic orchestrator owns the routing
state; the actual agent work travels as a **task over Kafka** and comes back as a **result**. The
same `quad-core` agent now lives in `quad-workflow-worker` — the other modules (`agent-common`,
`agent-orchestrator`, `agent-worker`) deliberately stay **LLM-free and deterministic**.

```
        quad workflow/agent  ──/unified/…──►  bridge  ──►  workflow-/agent_definitions (Postgres)
                                                            │
 Orchestrator (Spring Boot) ◄──intake───────────────────────┘
   │  RoutingEvaluator (pure, deterministic, replayable)
   │  checkpoints · retries · HITL tickets · sweep · Redis idempotency
   ▼
 agent.tasks.<agentType>   (key = workflow_id)
   │
   ▼
 Worker:  ┌ agent-worker            (deterministic mock)
          └ quad-workflow-worker    (quad-core + real LLM, mock fallback)
   │
   ▼
 agent.execution.results ──► orchestrator ──► next step … ──► END
```

- **New vs. stage 1:** durability lives in **Postgres** (checkpoints), **Redis** (start
  idempotency) and Kafka (retention/replay); load is spread over partitions
  (`key = workflow_id`) and worker instances; HITL becomes decoupled tickets.
- **Bridge (quad → kafka):** `POST /api/v1/unified/agents/convert` (agents first!) and
  `POST /api/v1/unified/workflows/from-quad` (workflow). Details:
  [`kafka-platform/examples/quad-to-kafka/README.md`](kafka-platform/examples/quad-to-kafka/README.md).
- **Fits:** high throughput, parallelism, loose coupling, multiple worker fleets.
- **Limit:** orchestration logic (retries, timers, state machine) is **hand-built** and must be
  kept deterministic/replayable.
- **Run:** `cd kafka-platform && mvn verify`, then `docker compose up -d kafka postgres redis`.
- Details: [`kafka-platform/README.md`](kafka-platform/README.md).

### Stage 3 — `agentflow`: durable (Temporal + Spring AI)

Here **Temporal** replaces the hand-built Kafka orchestration. A workflow is deterministic code;
everything non-deterministic (LLM/tool calls) runs in **activities**. The same `quad-core` agents
are built and executed inside an activity; there is additionally a Spring AI (LangGraph4j) path
and **declarative `quad.workflow` v2 documents** that a generic runner executes across all node
types (`agent`, `decision`, `loop`, `fork`, `join`, `hitl`, `subworkflow`, `end`).

```
Client ──REST──► agentflow-app (Spring Boot)
                    │  starts workflow  ──►  Temporal server (:7233)
                    │                              │  event history = durability
                    │   ┌──────────────────────────┘  replay · retries · timers · signals
                    │   ▼
                    │  workflow (deterministic): plan/DAG, branching, HITL wait
                    │      │  activity
                    │      ▼
                    └─  activity: quad-core agent / Spring AI / tools / MCP  (non-deterministic)
```

- **New vs. stage 2:** durability, retries, timers and HITL **signals** are **platform features**
  instead of DIY. The event history enables deterministic replay; a worker restart is
  uneventful. Declarative v2 definitions are validated (exactly one `end`, acyclic, resolvable
  references) and published **fail-closed**.
- **Fits:** long-running, failure-tolerant business processes with human approvals and a
  reproducible audit trail.
- **Run:** `cd agentflow && ./start-agentflow.sh` (app `:8095`, Temporal UI `:8233`, Angular UI `:4200`).
- Details: [`agentflow/README.md`](agentflow/README.md).

---

## What changes per stage (and what does not)

| Aspect | Stage 1 · quad-quarkus | Stage 2 · kafka-platform | Stage 3 · agentflow |
|---|---|---|---|
| **Agent basis** | `quad-core` (LangChain4j) | `quad-core` (in `quad-workflow-worker`) | `quad-core` (+ Spring AI) |
| **Orchestration** | in-process (workflow engine) | Kafka choreography + deterministic evaluator | **Temporal** |
| **Durability** | DB stores/checkpoints in-process | Postgres checkpoints, Redis idempotency, sweep | **Temporal history (replay)** |
| **Trigger** | REST/SSE, messaging channels | Kafka topics | REST/SSE + Temporal signals |
| **Model call** | in-process | in the Kafka worker | inside Temporal activities |
| **HITL** | checkpoints via SSE | HITL tickets (Kafka) | Temporal signals |
| **UI** | React (`quad-ui`) | REST API (+ bridge from `quad-ui`) | Angular (`agentflow-ui`) |
| **LLM-free?** | no | core yes (`agent-common`/`orchestrator`/`worker`), LLM only in `quad-workflow-worker` | no (LLM encapsulated in activities) |

**Guideline across all stages:** *keep durable/graph boundaries explicit, never retry
non-idempotent effects without a key, keep checkpoints slim* — the agent stays the same, only
the orchestration moves outward.

---

## Project structure

```text
.
├── pom.xml               # Maven aggregator (quad-quarkus, kafka-platform, agentflow)
├── README.md             # this file
├── .gitignore            # consolidated from all modules
├── quad-quarkus/         # QUAD – agent framework (Quarkus + LangChain4j) + React UI
├── kafka-platform/       # event-driven workflow platform (Kafka + quad-core worker)
├── agentflow/            # durable agent workflows (LangGraph4j + Spring AI + Temporal) + Angular UI
└── integration-tests/    # end-to-end tests across the platform (separate reactor)
```

The root level tracks only `README.md`, `pom.xml` and `.gitignore`; local helper files (Makefile,
`start-*.sh`, notes, `.code-workspace`) are ignored.

---

## Build & test

The root aggregator resolves `quad-core` from the **reactor**, so that
`kafka-platform/quad-workflow-worker` does not pull a stale jar from `~/.m2`:

```bash
# everything (correct order, incl. tests)
mvn -T1C clean install

# individual reactors
cd quad-quarkus    && mvn clean install          # quad-core + Quarkus + UI
cd kafka-platform  && mvn verify                 # orchestrator/worker/quad-workflow-worker
cd agentflow       && mvn clean package          # app + UI (quad-core from ~/.m2)

# integration project (separate reactor, separate ports)
cd integration-tests && ./scripts/run-integration-tests.sh e2e   # L0–L3
```

> **Test counts are not hard-coded in the docs.** CI
> (`.github/workflows/ci.yml`) builds every reactor and publishes the exact
> per-module Surefire counts in the run's job summary.

> **Stale-jar trap:** after changes to `quad-core`, always run `mvn … clean package` or
> `mvn install` in the consumers — without `clean`, the old `quad-core` is bundled into the
> fat jar.

**Frontends**

```bash
cd quad-quarkus/quad-ui    && npm install && npm run build
cd agentflow/agentflow-ui  && npm install && npm run build
```

---

## Continuous integration

`.github/workflows/ci.yml` runs on every push/PR against `main`/`master` and covers the
whole monorepo: the three Java reactors (`quad-quarkus`, `kafka-platform`, `agentflow`),
the standalone `integration-tests` reactor (L0 contract by default) and both front-ends
(`quad-ui`, `agentflow-ui`). `kafka-platform` runs its Testcontainers-based suite under
the Docker socket. Each job appends the per-module test counts to the run summary via
`.github/scripts/summarise-tests.sh` — test numbers are reported, never hard-coded.

## Aggregator vs. parent (A3)

The root `pom.xml` intentionally aggregates without being a parent: `quad-quarkus` and
`kafka-platform`/`agentflow` use different parent POMs and group IDs. A common parent
would force one of those chains to be abandoned. This is recorded here as a **proposal
for a later consolidation**, not a task: the current setup builds correctly, and CI
enforces it.

---

## Ports (local stack)

| Component | Port |
|---|---|
| quad-quarkus (Quarkus) | `8083` (dev) / `8086` (default) |
| quad-ui (React, Vite) | `5173` |
| agent-orchestrator (Kafka) | `8080` |
| quad-workflow-worker | `8092` |
| Kafka / Postgres / Redis | `9092` / `5432` / `6379` |
| agentflow-app | `8095` |
| Temporal (gRPC / UI) | `7233` / `8233` |
| agentflow-ui (Angular) | `4200` |

---

## Further reading

| Topic | Document |
|---|---|
| Agent framework in depth (hooks, guardrails, HITL, PII, security, deployment) | [`quad-quarkus/README.md`](quad-quarkus/README.md) |
| Kafka platform (topics, routing, HITL tickets, mock behavior) | [`kafka-platform/README.md`](kafka-platform/README.md) |
| Run a quad workflow on Kafka (bridge, conversion) | [`kafka-platform/examples/quad-to-kafka/README.md`](kafka-platform/examples/quad-to-kafka/README.md) |
| agentflow (Temporal, declarative v2 workflows, per-agent guardrails) | [`agentflow/README.md`](agentflow/README.md) |
| End-to-end layers L0–L3 + documented findings | [`integration-tests/README.md`](integration-tests/README.md) |

---

## Notes

- **Fresh history:** this monorepo was created by merging the formerly separate repositories
  `quad`, `kafka-platform` and `agentflow` with **fresh history**; the old repositories remain as
  history/backup.
- **Secrets:** only `*.example` templates are tracked; real `.env` files are ignored.
