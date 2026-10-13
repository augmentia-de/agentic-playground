# Java Agent Stack vs. QUAD — Comparison, Advantages, Replaceability, Evolution Path

As of: 2026-10-10. Scope: `quad-core`, `quad-workflow-*`, `quad-quarkus`, `kafka-platform`.
Purpose: To compare the proposed 3-layer Java architecture (Durable Execution / Graph Engine /
Framework) against what we have **today** — with a complete inventory of the
advantages, what can be replaced, and an evolution path.

---

## 0. Brief Summary

- We **already have the three layers**, but as **one proprietary, horizontally integrated
  implementation**: the kafka orchestrator is the "durable/saga" layer, `WorkflowEngine` +
  `AgentRuntime` the "graph/agentic" layer, `quad-core` (LangChain4j) the framework layer.
- The **biggest strategic lever** is **not** "replacing everything with Temporal/LangGraph4j",
  but **standardizing the definition** (Open Workflow Specification / v2) and replacing **exactly
  one layer** with a standard — where we genuinely lack capabilities
  (**durable replay across restarts**, **long-running timers**).
- Replacement candidates with clear benefit: **Temporal** (durable orchestration around the
  kafka orchestrator) and **LangGraph4j** (graph/HITL interrupts, if we move to Spring AI).
  **Do not** replace: our ReAct runtime with bounded context, skills, sub-agents,
  guardrails, MCP, and the cross-cutting persistence — that is the actual value.

---

## 1. The Reference Architecture (3 Layers)

```
Durable Execution (Temporal Java / Dapr Workflow)   ← State, Event-Sourcing, Saga, Timeouts
        │  managed/calls
Graph/Agentic Engine (LangGraph4j / Spring AI Graph) ← State-Nodes, Edges, HITL, Guardrails
        │  uses
Framework/Tooling (Spring AI / LangChain4j)          ← ChatClient, MCP-Tools, Schema-JSON
```

The mapping is **not** congruent with our cut: our framework core
(`quad-core`) already contains parts of the graph and durable layer (hooks, saga, HITL,
checkpoints). That is exactly the point where we have to decide.

---

## 2. Our System Today (Inventory, with References)

| Layer (reference) | Present in ours | Reference |
|---|---|---|
| Framework: LLM/abstraction | LangChain4j `ChatModel`, `ToolSpecification`, `AiMessage`; `ModelFactory`, `TieredChatModel`, `StructuredOutputConfig` | `quad-core/config/*`, `quad-quarkus-core` |
| Framework: Tools/MCP | `@Tool`/`@Param`, `QuadToolRegistry`, `ReflectiveToolMethod`, `quad-tool-builtin`, `quad-tool-mcp`, `McpManagerService` | `quad-core/tool/*` |
| Framework: Agent runtime | `Agent`, `AgentBuilder`, `AgentConfig`, `Agents`, `AgentRuntime` (ReAct, bounded context, `StuckDetector`, `OutputForcer`) | `quad-core/agent/*` |
| Graph: Workflow engine | `WorkflowEngine`, `NodeExecutorRegistry`, executors: Agent, Async (deferred), Conditional, Fork, Join, Loop, NestedWorkflow; `InputBuilder` (templates), `JsonPathResolver` | `quad-workflow-engine/*` |
| Graph: DSL | `AgentWorkflowBuilder`, `WorkflowNode` (AgentStep/Loop/Conditional/Nested), `AgentWorkflowConfig` | `quad-workflow-core/workflow/*` |
| Graph: HITL | `HitlFacade`, `HitlService`, `/runs/{id}/continue`, `/api/ui/dynamic/complete` (Deferred), UI overlay | `quad-core`, `quad-quarkus/hitl` |
| Durable: Event/State | `WorkflowRunState`/`Snapshot`/`NodeResult`; new: `run/event/*` (event-sourcing foundation); `run_checkpoints`; Kafka DLQ, idempotency | `quad-workflow-core`, `kafka-platform` |
| Durable: Orchestration | kafka orchestrator (DAG, retries, HITL, cancel, checkpoints), `workflow_definitions` (JSONB), `workflow_executions`, `workflow_checkpoints` | `kafka-platform/agent-orchestrator` |
| Durable: Saga | `SagaAgent`, `CompensatingAction`, `SagaAuditRecord`, `SagaAgentInterceptor`, `IdempotencyStore` | `quad-core/saga`, `runtime` |
| Durable: Scheduling | `CronExpression`, `ScheduledTask`, `Schedule`, `Automation` | `quad-workflow-core/schedule`, UI |
| Framework: Skills | `SkillRegistry`, `SkillParser`, `AgentSkillsPlugin`, JIT `load_skill`/`skill_search` | `quad-core/capability/skill` |
| Framework: Sub-agents | `SubAgentTool`, `DynamicSubAgentTool`, `ChannelAgentFactory` | `quad-core/tool`, `quad-quarkus` |
| Cross-cutting | Hooks/guardrails, GDPR/PII, permissions, provenance, OTel/Micrometer, journal | `quad-core/{hook,guardrails,gdpr,security,observability,events}` |
| Definition/Transfer | v1 schema + validator (old); new v2 schema, upcaster, PublishCompiler, SemanticValidator, HITL normalizer; `workflow_documents` (V25) | `quad-workflow-core/transfer`, `docs/WORKFLOW_FORMAT_PROPOSAL.md` |

---

## 3. Comparison per Layer

### 3.1 Durable Execution (Temporal / Dapr)

| Criterion | Temporal Java SDK | Dapr Workflow | QUAD today |
|---|---|---|---|
| Durable replay across restart | **Yes** (deterministic replay of the workflow logic) | Yes (Durable Task, replay) | **Partial**: snapshots/events + idempotency + Kafka redelivery; **no** code replay |
| Long-running (hours/days) | **Yes**, timers/signals first-class | Yes | Schedules/Cron + `TimerChannel`; no durable timer signals |
| Saga/compensation | **Yes** | Yes | `SagaAgent`/`CompensatingAction` present |
| HITL | Signals + `Workflow.await` | external events | Kafka `PAUSED_HITL` + tickets + continue endpoint |
| Operation | own server/cluster | Sidecar + Durable Task | **already present** (Kafka/Postgres), no new service |
| Language/binding | Java SDK + `temporal-spring-ai` | `dapr-spring-boot-starter` | Spring **and** Quarkus capable (framework-neutral core) |
| Observability | Web UI, history | Dapr Dashboard | OTel/Prometheus/Grafana, custom run views |

**Verdict:** Temporal/Dapr are clearly stronger at **durability/replay/timers**. Our advantage is
**zero additional infrastructure** and Kafka-native, multi-tenant event processing.

### 3.2 Graph/Agentic Engine (LangGraph4j / Spring AI Graph)

| Criterion | LangGraph4j | Spring AI Agentic Patterns | QUAD |
|---|---|---|---|
| Graph model | `StateGraph`, nodes/edges, conditional edges, **reducers/channels** | Orchestrator-Worker, routing, chains | `WorkflowEngine` + `NodeExecutor` (typed), `AgentWorkflowBuilder` |
| HITL | **`interruptBefore` / `InterruptionMetadata` / `updateState`** (mechanistic) | `AskUserQuestionTool` | Checkpoint + continue + deferred completion |
| Checkpointing | **`CheckpointSaver`** (in-memory/JDBC) | — | `run_checkpoints`, `workflow_state` |
| LLM loop | builds on LangChain4j **or** Spring AI | Spring AI ChatClient | own `AgentRuntime` (ReAct, bounded, stuck detection, output forcing) |
| Parallel/loop/sub-graph | yes | yes | Fork/Join/Loop/Nested (own executors) |
| Maturity/ecosystem | small, active (bsorrentino) | Spring-official (Tzonov series) | proprietary, in use |

**Verdict:** LangGraph4j provides **standardized, compact graph primitives including reducers and
interrupts**. Our advantage is **production depth** (skills, sub-agents, MCP, guardrails,
persistence, multi-runtime). The swap is especially worthwhile if we are moving to Spring AI anyway.

### 3.3 Framework/Tooling (Spring AI / LangChain4j)

| Criterion | Spring AI | LangChain4j | QUAD |
|---|---|---|---|
| LLM/embedding/vector | broad, Spring-first | broad, framework-neutral | LangChain4j directly |
| Tools | `@Tool`, MCP, Advisors | `@Tool`, MCP | `@Tool`/`@Param`, reflective binding, MCP module |
| Agentic patterns | **Skills, subagents/task, TodoWrite, AskUserQuestion** | AiServices, Tools | Skills (JIT), SubAgentTools, HITL |
| Durable integration | **`temporal-spring-ai`** | — | own kafka worker |
| Runtime | Spring Boot | neutral | Quarkus **and** Spring (worker) |

**Verdict:** Spring AI brings **patterns + Temporal binding**; LangChain4j remains the
more neutral core. Our tool/skill/MCP portfolio is already at enterprise level.

---

## 4. Complete Inventory: Building Block — Advantage — Replaceable?

| Building block (Java world) | Core advantage | Our equivalent | Replace/Augment? | Benefit for us |
|---|---|---|---|---|
| **Temporal Java SDK** | Deterministic replay, timers, saga, versioning, UI | kafka orchestrator + `workflow_state`/checkpoints | **Augment**: Temporal **around** long/failure-safe workflows; Kafka remains transport | Highest durability jump; survives restarts/network outages without data loss |
| **Dapr Workflow** | Durable Task, CNCF, sidecar | as above | **Alternative** to Temporal if sidecar/CNCF is desired | Lower barrier to entry, but sidecar operation. `dapr-spring-boot-starter` ≥ 1.19 targets **Spring Boot 4.0** (fits our kafka-platform 4.0.8); 3.5 ⇒ 1.18.x |
| **LangGraph4j** | `StateGraph`, reducer/channels, **interrupts/HITL**, checkpoint saver | `WorkflowEngine`/`AgentWorkflowBuilder` | **Augment/replace** the graph layer (especially with Spring AI) | Standard graph API, less in-house development, clean HITL. **MIT**, 1.9.3 (Oct 2026), 1.8.x **LTS**, Java 17+, broad saver/studio/OTel ecosystem |
| **Spring AI Agentic Patterns** | Skills, subagent/task, TodoWrite, routing | Skills/subagents (own) | **Adopt ideas** (TodoWrite, task isolation) | Context isolation, less "agent bloat". **Note:** the patterns come from the community module `org.springaicommunity:spring-ai-agent-utils` 0.4.x and require Spring AI **2.0 milestones** |
| **Spring AI (framework)** | Spring-first, advisors, vector, observability | LangChain4j + Quarkus | **Optional** for the kafka worker (Spring) | Consistency in the Spring reactor |
| **LangChain4j** | Framework-neutral, broad provider support | already in use | **Keep** | Basis of `quad-core` |
| **Open Workflow Specification (CNCF)** | Vendor-neutral DSL + JSON schema + Java SDK | v1/v2 schema (own) | **Adopt** (v2 → OWS) | Interoperability, tooling; `docs/WORKFLOW_FORMAT_PROPOSAL.md` |
| **Quarkus Flow** | OWS-compliant, agentic via LangChain4j | quad-workflow-engine + Quarkus app | **Evaluate/partially replace** | Ready-made OWS runtime instead of in-house development |
| **BPMN/DMN** | Business modeling, decision tables | proprietary nodes | **Only as import/export projection** | Business-domain tooling, not the internal model |
| **Embabel (Spring AI)** | Agent framework (Rod Johnson), goal-oriented | `AgentRuntime` | **Watch** | Alternative agent orchestration |
| **MCP** | Standardized tool boundary | `quad-tool-mcp` | **Keep/expand** | Tool ecosystem without rebuilding |

---

## 5. What Can Concretely Be Replaced — and What Cannot

**Replace/augment (clear benefit):**
1. **Durable layer** — kafka orchestrator internals (retries, checkpoints, timeouts, saga) are
   candidates for **Temporal** (or Dapr). Kafka remains as **transport** (`agent.tasks.*`,
   results, DLQ); Temporal takes over the **workflow history/replay**.
2. **Graph layer** — our `WorkflowEngine` + executors by **LangGraph4j `StateGraph`**, if
   we move to Spring AI. Gain: reducers, interrupts, checkpoint saver instead of in-house development.
3. **Definition** — `{nodes,edges}`/v1 by **OWS** (a v2 proposal already exists).
4. **HITL mechanics** — custom continue logic by **LangGraph4j interrupts** or
   **Temporal signals**.
5. **Context patterns** — **TodoWrite/subagent isolation** from Spring AI into our sub-agents.

**Do not replace (genuine estate):**
- `AgentRuntime` (bounded context per round, stuck detector, output forcer, cache-prefix chain).
- Tool/skill/MCP portfolio incl. JIT `load_skill` and dynamic tool activation.
- Cross-cutting: guardrails, GDPR/PII, permissions, provenance, audit, OTel.
- Framework neutrality (Quarkus **and** Spring) and the two-reactor separation.

---

## 6. The Three "Limits in Java" — Concretely in Our Case

| Limit (from the assessment) | In our case | Recommendation |
|---|---|---|
| **State/blob size during checkpointing** | `workflow_state.step_results`, `node_states_json`, `run_checkpoints` (JSONB/TEXT) | Only IDs + slim JSONs in the state; large artifacts in the object store/vector DB; `workflow_documents` (v2) as a slim canonical document |
| **Dynamic MCP tool schemas vs. Java types** | `ReflectiveToolMethod` (JSON schema from the signature), `quad-tool-mcp` (dynamic) | Jackson `JsonNode`/records for MCP; schema drift via the v2 semantic validator |
| **Idempotency of external tool/MCP calls** | `IdempotencyStore`, `@Idempotent`, `ToolExecutor`, Kafka DLQ | Idempotency key per effect; saga compensation; explicitly document "at-least-once + idempotent" |

---

## 7. Evolution Path (Stages)

### Stage 0 — Today
In-house development, two runtimes, rich in capabilities; definition proprietary; durability = Kafka + checkpoints.

### Stage 1 — Standardize the definition (in progress)
v2 schema + upcaster/publish/SemanticValidator + `workflow_documents` (see
`docs/WORKFLOW_FORMAT_PROPOSAL.md`). **Effort: small, risk: small, benefit: high**
(interoperability, a single source-of-truth format).

### Stage 2 — Unify graph/HITL
Either (a) keep `WorkflowEngine` and **retrofit interrupt/reducer semantics**, or
(b) introduce **LangGraph4j** (especially with a Spring AI decision). **Medium effort.**

### Stage 3 — Add durable execution
**Temporal Java SDK** (or Dapr) as a **wrapper** for long/failure-safe workflows.
Node/agent execution becomes an **activity**; Kafka remains transport; HITL via signals.
**Highest benefit, medium operational effort** (new cluster) — only for workflows that must
run over a long time/across restarts.

### Stage 4 — Consolidate framework/observability
Deliberately choose the framework layer: **keep LangChain4j in the neutral core**; for the
**Spring reactor** Spring AI + `temporal-spring-ai`; MCP as the only tool boundary; OTel/Micrometer
throughout. **Benefit: consistency, fewer special paths.**

### Stage 5 — Interop
OWS↔BPMN/DMN as an import/export projection; A2A/MCP for external agents/tools.

**Decision order (recommended):** complete Stage 1 → Stage 2 (graph/HITL) →
Stage 3 only with genuine long-running need → consolidate Stage 4.

---

## 8. Decision Matrix

| If … | then … |
|---|---|
| Workflows run over hours/days and must survive restarts | Add **Temporal** (or Dapr) as a durability layer |
| We want less in-house development in graph/HITL | **LangGraph4j** (with Spring AI) or retrofit interrupt semantics |
| Interoperability with other runtimes/tooling is needed | Push **OWS** (v2) through as the canonical format |
| The Spring reactor should become consistent | **Spring AI** + `temporal-spring-ai` in the worker |
| Everything runs stably enough in the kafka orchestrator | **Replace nothing**, just Stage 1 + 2 |

---

## 9. Sources (2026)

- LangGraph4j (Java): GitHub `langgraph4j/langgraph4j`; "Human-in-the-Loop at ease" (B. Sorrentino, dev.to/2025) — `StateGraph`, `CheckpointSaver`, `InterruptionMetadata`, `updateState`, `interruptBefore`, LangChain4j/Spring AI integration.
- Spring AI Agentic Patterns (C. Tzolov, spring.io): #3 TodoWrite (2026‑01‑20), #4 **Subagent Orchestration / Task** (2026‑01‑27), Agent Skills (2026‑01‑13), AskUserQuestionTool (2026‑01‑16).
- Temporal: "Spring AI integration" (`io.temporal:temporal-spring-ai`, SDK ≥1.35, Spring Boot 3.x); "Build resilient Agentic AI with Temporal"; OpenAI Agents SDK + Temporal.
- Dapr: "Workflows with Java Spring Boot" (Diagrid, Dapr 1.16); Dapr vs. Temporal (2026).
- Open Workflow Specification (CNCF, v1.0.3) + Quarkus Flow (OWS-compliant, LangChain4j) — see `docs/WORKFLOW_FORMAT_PROPOSAL.md`.
- Repo evidence: see §2.

---

## 10. Verification, Corrections & Risks (Review 2026-10-10)

### 10.1 Point-by-Point Verification

| # | Claim in the document | Status | Evidence / correction |
|---|---|---|---|
| 1 | LangGraph4j = Java port with StateGraph/interrupts | **confirmed** | GitHub `langgraph4j`; `StateGraph`, `InterruptionMetadata`, `updateState`, `interruptBefore` |
| 2 | "small, active" (ecosystem) | **corrected (too weak)** | **1.9.3 (01.10.2026)**, **1.8.x LTS**, **MIT**, Java 17+, savers for Postgres/MySQL/Oracle/DynamoDB/Cockroach/Redis/Hazelcast/SQLite, **DSL**, **AG-UI**, **Studio** (Quarkus+Spring), **OTel hook** |
| 3 | Checkpointing/reducer present | **confirmed** | checkpoints, breakpoints (pause/resume), channels/reducer |
| 4 | HITL via interrupts | **confirmed** | `approvalOn(...)`, `InterruptionMetadata`, `updateState(...APPROVED/REJECTED)` |
| 5 | Spring AI delivers subagents/task "officially" | **corrected** | comes from the **community** `org.springaicommunity:spring-ai-agent-utils` **0.4.x**, requires Spring AI **2.0-M2/M5** (still milestone) |
| 6 | Temporal + Spring AI | **confirmed** | `io.temporal:temporal-spring-ai`; handles determinism for durable tools |
| 7 | Temporal requires deterministic workflow code | **confirmed, refined** | LLM/tool/API/DB **must be activities**; `Date.now`/random = frequent source of errors |
| 8 | Dapr Workflow Java | **confirmed, refined** | `dapr-spring-boot-starter` ≥1.19 ⇒ **Spring Boot 4.0** (fits us); 3.5 ⇒ 1.18.x |
| 9 | Our 3 layers "present" | **confirmed** | kafka orchestrator (durable), `WorkflowEngine`+7 executors, `AgentRuntime` (LangChain4j) |
| 10 | Saga present | **refined** | `@SagaAgent` (annotation) + `SagaAgentInterceptor` + `CompensatingAction`/`SagaAuditRecord` (no `class SagaAgent`) |
| 11 | Idempotency | **confirmed** | `IdempotencyStore`, `@Idempotent`, Kafka DLQ |
| 12 | HITL/channels/deferred | **confirmed** | `HITLPlugin/-Provider/-Authority`, `HitlFacade`, `Queue-/TimerChannel`, `AsyncNodeExecutor` |
| 13 | Context guards | **confirmed** | `StuckDetector`, `OutputForcer`, bounded `SummarizingSlidingWindowConversationManager` |
| 14 | Skills JIT | **confirmed** | `SkillLoadTool`, `SkillSearchTool`, `SkillActivationHook` |
| 15 | MCP module | **refined** | the MCP client lives in `quad-core/tool` (`McpToolClient`, `SseMcpProxy`, `McpToolMethod`) **plus** the module `quad-tool-mcp` |
| 16 | Persistence tables | **confirmed** | `workflow_state`, `run_checkpoints`, `workflow_documents`, `sessions`, `skills`, `memories` |
| 17 | Versions | **confirmed** | LangChain4j **1.15.0**, kafka-platform **Spring Boot 4.0.8**, Java 21 |
| 18 | "Zero additional infrastructure" in our case | **confirmed, qualified** | true operationally; in exchange, **no** deterministic replay/timers |

### 10.2 What Can Be Done Better (Concrete Improvements)

1. **Draw a durability boundary instead of doubling the stack.** Delegate exactly **one** class of
   workflows (long-running, restart-surviving, long timers) to Temporal/Dapr; short/kafka-native
   workflows stay with the orchestrator. Otherwise **two orchestrators** (split-brain).
2. **Steal the interrupt/reducer semantics first, not the dependency.** LangGraph4j concepts
   (channels/reducer, `interruptBefore`, `updateState`) are small and can be rebuilt in
   `WorkflowEngine`/`HitlFacade` → high benefit, low risk, **without** a framework switch.
3. **Decouple the definition (Stage 1) before swapping the engine.** First OWS/v2 as the
   truth; then the graph/durable swap is an adapter problem instead of a fundamental decision.
4. **Secure checkpoint serialization.** JSON + schema only, **no** native Java deserialization
   (RCE class), sign/validate checkpoints; apply PII/GDPR rules to checkpoints.
5. **Payload/state discipline.** Only IDs + slim JSONs in `workflow_state`/`run_checkpoints`;
   artifacts in the object store/vector DB; size caps + monitoring.
6. **Adopt context isolation** from the Spring AI patterns (task subagent, TodoWrite) — our
   `SubAgentTool` is the basis; we are missing a "todo" tool.
7. **Differentiate retry budgets** (LLM expensive vs. metadata cheap) — set explicitly on activities.

### 10.3 Risks (with Countermeasures)

| Risk | Why critical | Countermeasure |
|---|---|---|
| **Two orchestrators (Kafka + Temporal/Dapr)** | Double retries/HITL, inconsistent history, split-brain | One ownership boundary per workflow type; Kafka remains transport, Temporal only orchestration |
| **Determinism/replay errors** | `Date.now`, random, direct calls break replay (known Temporal pain point) | LLM/tool/IO strictly as activities; deterministic workflow logic; versioning discipline |
| **Checkpoint deserialization (RCE)** | The LangGraph CVE cluster (SQLi→RCE) shows the class | JSON-only, no Java native deserialization, signature, schema validation |
| **PII/GDPR in checkpoints** | Raw prompts/tool payloads in the DB | Apply our PII anonymization/audit to checkpoints too; retention |
| **Operational complexity** | Temporal server (PG/Cassandra) or Dapr sidecars/scheduler (known start-job bugs) | Managed/cloud, or Dapr only where a sidecar is acceptable; operating concept beforehand |
| **Ecosystem churn** | Spring AI 2.0 = milestones; `spring-ai-agent-utils` 0.4.x; LangGraph4j 2.0 announced | Keep the core (LangChain4j) neutral; encapsulate standards behind `agentRef`/interface |
| **Framework lock-in (Spring AI)** | Would endanger the Quarkus branch | Spring AI only in the **Spring reactor** (worker); `quad-core` stays framework-neutral |
| **LLM costs from retries** | Activities repeat expensive calls | Idempotency + retry budgets + "best-effort" per activity |
| **Multi-tenant/data residency** | Temporal namespaces/checkpoints = new data storage | Tenant mapping, namespace strategy, GDPR impact assessment |
| **Regression risk when swapping the engine** | 491+ tests, two runtimes, running pipelines | Staged migration, adapters, parallel operation with dry runs |

### 10.4 Adjusted Recommendation

- **Immediately (small/low-risk):** complete Stage 1 (OWS/v2); **retrofit interrupt/reducer
  semantics**; harden **checkpoint security**; payload discipline; **todo tool** + subagent isolation.
- **Conditional (medium):** LangGraph4j **or** keeping the engine — decide only after Stage 1
  and the framework choice (Quarkus-first vs. Spring-first).
- **Only with hard need (medium/high):** Temporal/Dapr as a **thin** durability capsule with a strict
  owner boundary — not as a second orchestrator.

**Net correction to the first draft:** LangGraph4j and Dapr are **more mature** than depicted in the
first draft (MIT/LTS and SB4 support respectively); the Spring AI *patterns*, by contrast, are
**less** official (community + 2.0 milestones). The main risk factor remains **not** the technology,
but the **duplication of orchestration** and **checkpoint security**.
