# AgentFlow — Ideas

**Status:** Concept (2026-10-10). Basis: `agentflow/` (`agentflow-core`, `agentflow-app`).
Stack: **Spring Boot 4.0.8 · Spring AI 2.0.1 · LangGraph4j 1.9.4 · Temporal 1.40.0 · quad-core**.
Goal of this document: what the app can do today, which complex use cases build on it, and
how a UI for **observe, config, logs, HITL and task start** could look.

---

## 0. What the app can do today (real, not wishful thinking)

| Capability | Implementation | Status |
|---|---|---|
| Graph reasoning (plan → execute → review, conditional retry) | `AgentFlowGraph` (`StateGraph`, `addConditionalEdges`) | ✅ |
| Durable orchestration (replay/retry/timers) | Temporal `AgentFlowWorkflow` + activity | ✅ |
| quad-core agent as durable activity | `QuadAgentWorkflow` + `AgentFlowActivities.runQuadAgent` (`Agents.simple()`) | ✅ |
| **Rich quad agent** (tools/skills/guardrails/PII/audit) | `QuadCapabilities` + `RichAgentWorkflow`, `/agentflow/runs/quad-rich` | ✅ |
| **Built-in tools** (workspace-confined) | readFile, writeFile, listDirectory, grepSearch, findFiles, webSearch, webfetch | ✅ |
| **Skills (JIT)** | `AgentBuilder.withSkills` → `load_skill` / `skill_search` | ✅ |
| **Guardrails** | `GuardrailPlugin` via `withGuardrail` (prompt injection) | ✅ |
| **PII anonymization** | `PiiAnonymizerHook` (EMAIL/PHONE/CREDIT_CARD → `[REDACTED]`) | ✅ |
| **Audit trail** | `AuditTrailHook` (in-memory) | ✅ |
| **RBAC (role-based tools)** | `QuadSecurity` + quad `RoleBasedToolGuard` (`/runs/authorized`) | ✅ |
| **Saga/compensation (durable)** | Temporal `SagaWorkflow` + quad `SagaAgentInterceptor` (`/runs/saga`, `/runs/quad-saga`) | ✅ |
| **Structured Output** | `withStructuredOutput(ContentPlan)` + `/runs/structured` | ✅ |
| **Provenance** | quad `ProvenanceTracker` + `/runs/provenance` | ✅ |
| **Long-term Memory** | quad `PersistentMemoryStore` (in-memory) + `/runs/memory` | ✅ |
| **MCP tools** | `SseMcpProxy`/`McpToolMethod` + `/runs/mcp` (SSE URL configurable) | ✅ |
| **Sub-agents** | quad `SubAgentTool` via `withSubAgent` + `/runs/subagents` | ✅ |
| **OTel/metrics** | quad `MetricsHook` (Micrometer) + `TracingHook` (OTel) + `/runs/metrics`, `/actuator/prometheus` | ✅ |
| **Capability search/RAG** | quad `CapabilitySearch`/`CapabilityIndex` (HNSW or PgVector) + `/runs/capability-search`, `/capabilities/search` | ✅ |
| **Business cases** | module `agentflow-examples`: 5 cases under `/examples/**` | ✅ |
| **Declarative workflows/agents (v2)** | `quad.workflow` v2 JSON (`workflows/*.json`, incl. `agents[]`) via `quad-workflow-core` (publish compiler + semantic validator) → `/agentflow/workflows`, `/runs/declarative` | ✅ |
| Human-in-the-Loop | `interruptBefore(REVIEWER)` + `HitlWorkflow` (`@SignalMethod decide`) | ✅ |
| Persistent checkpoints (worker-restart-safe) | `SQLiteSaverV2`, `graphId=agentflow` | ✅ |
| LLM (OpenAI-compatible) + configurable headers | `spring.ai.openai.*` (single source) + `LlmConfigBridge` + `LlmHeaderConfig` (headers only when configured) | ✅ |
| REST start/signal | `/runs*` (incl. `structured`, `provenance`, `memory`, `mcp`, `subagents`, `metrics`, `capability-search`, `declarative`), `/capabilities`, `/capabilities/search`, `/workflows`, `/workflows/{id}`, `/info`, `/quad/capabilities`, `/security/roles`, `/examples**` | ✅ |
| Tests | Graph (3), exceptions (15), examples (8, incl. durable workflows via `TestWorkflowEnvironment`) | ✅ |
| **UI** | module `agentflow-ui` (Angular 20), see `UI.md` | ✅ |
| Run listing / HITL queue / config API | `GET /agentflow/runs(/{id})`, `/agentflow/settings`, workflow editor (`/workflows/validate`, `PUT`) | ✅ |

**Honest limits today:** no UI, no run list, no live stream, no config CRUD,
state lives in activity logic (not as a first-class run object), HITL only pauses the one node.

---

## 1. Capability map (target picture)

```
Temporal (Durability)      LangGraph4j (Reasoning)        Spring AI (Model/Tools)
  Workflow / Signals         StateGraph / Reducers          ChatClient / MCP
  Timers / Replay            Interrupts / Checkpoints       Structured Output
        │                           │                              │
        └─────────────── AgentFlow Core (quad-core reuse) ────────┘
                    Agents · Tools · Skills · Guardrails
```

---

## 1.1 Deep quad usage — catalog

**Already wired** (`QuadCapabilities` / `RichAgentWorkflow`, `/agentflow/runs/quad-rich`)

| Building block | quad-core class | Effect |
|---|---|---|
| Built-in Tools | `ReadFile/WriteTool/LsTool/GrepTool/FindTool/WebSearchTool/WebFetchTool` | file/web tools, confined to the workspace |
| Tool-Registry | `QuadToolRegistry` + `ReflectiveToolMethod` | tools registered under catalog names |
| Skills (JIT) | `AgentBuilder.withSkills` → `AgentSkillsPlugin`, `SkillLoadTool`, `SkillSearchTool`, `ToolActivatorTool`, `SkillActivationHook` | catalog instead of full text; `load_skill` on demand |
| Guardrails | `Guardrail`, `GuardrailPlugin` | blocks prompt injection |
| PII/GDPR | `PiiAnonymizerHook` | masks email/phone/credit card before the model |
| Audit | `AuditTrailHook` (+ `AuditStore`) | tool/agent actions audit-proof |
| RBAC/Permissions | `RoleBasedToolGuard`, `SecurityContext` (via `QuadSecurity`) | role-based tool scopes (deny by absence) |
| Saga/compensation | `SagaAgentInterceptor`, `CompensatingAction` (+ Temporal `SagaWorkflow`) | compensation forward/backward, durable |
| Structured Output | `StructuredOutputConfig`, `Agent.executeStructured` | typed outputs |
| Memory | `PersistentMemoryStore`, `MemoryEntry`, `MemoryCategory` | long-term memory per scope |
| Provenance | `ProvenanceTracker`, `ProvenanceEntry` | origin/source per tool |
| MCP | `SseMcpProxy`, `McpToolMethod`, `McpToolClient` | external MCP servers as tools |
| Sub-agents | `SubAgentTool`, `DynamicSubAgentTool` | hierarchy/handoff via registered tool |
| Metrics/OTel | `MetricsHook`, `AgentMetrics`, `TracingHook`, `OTelAgentTracer` | Micrometer counters (Prometheus) + OTel spans |
| Capability search/RAG | `CapabilitySearch`, `CapabilityIndex`, `CapabilitySearchTool`, `CapabilitySearchRouter` | tool/skill retrieval (HNSW or PgVector) |
| Events/Logging | `AgentEventPublisher` + `LoggingHook` | token/tool/lifecycle events |
| Workspace/Session | `WorkspaceResolver`, `CurrentSession` | path confinement, session binding |
| Model | `ModelFactory` (+ `ModelHeaders`) | OpenAI-compatible incl. OpenCode Go headers |

**Available, not yet wired** (next expansion stage)

| Building block | quad-core class | Idea |
|---|---|---|
| Skill/tool discovery | `ToolActivatorTool`, `CapabilityRegistry` | dynamic tool activation from retrieval |

> Principle: **Temporal** = durability, **LangGraph4j** = graph/HITL, **quad-core** = agent capabilities
> (tools/skills/guardrails/PII/memory/saga). Every capability is encapsulated as an activity.

---

## 2. Complex use cases

### UC-1 — "Deep Research & Editorial" (multi-stage, tools, HITL)
The news workflow known from `quad`, but durable and with approval.
```
start → planner → researcher(tools: webSearch, webfetch) → analyst → writer
                         │                                   │
                         └────────── interrupt (HITL) ───────┘
                                        │ approve
                                     publish
```
- **Uses:** LangGraph4j nodes + sub-agents, quad-core `ToolRegistry`/MCP, HITL interrupt,
  Temporal retries (flaky web fetches), persistent checkpoints.
- **Why Temporal:** duration minutes–hours, external sources, asynchronous approval.

### UC-2 — Regulatory approval process (days, audit, saga)
An application goes through reviews; on rejection, side effects are compensated.
- **Uses:** Temporal `Workflow.await` with timers (SLA escalation), HITL signals, quad-core
  `SagaAgent`/`CompensatingAction` in activities, audit events.
- **UI:** HITL inbox, deadlines, audit trail.

### UC-3 — RAG-assisted case review (structured output)
Retrieval → assessment → decision with a typed result.
- **Uses:** Spring AI VectorStore/embeddings, Structured Output (JSON schema), reviewer node.
- **Graph:** `retrieve → assess → decide`; on low confidence → back to `retrieve` (cycle).

### UC-4 — Parallel content pipeline (fan-out/fan-in)
One input → four channels (LinkedIn/X/blog/newsletter) in parallel → join → packaging.
- **Uses:** LangGraph4j parallel branch/join, its own subgraph per channel.
- **Durability:** each branch a Temporal activity; join waits deterministically.

### UC-5 — Overnight batch / scheduled (restart-safe)
Nightly bulk processing, the worker may restart.
- **Uses:** Temporal Cron/`Workflow.await(Duration)`, persistent checkpoints, worker restart test.
- **UI:** schedule view, last/next runs, success rate.

### UC-6 — Multi-agent handoff with skills
Specialized agents (skills) hand over context; a supervisor routes.
- **Uses:** quad-core `SkillRegistry`/`load_skill`, `SubAgentTool`, LangGraph4j handoff,
  Spring AI agent-skills ideas (todo/task isolation).

### UC-7 — What-if / Replay / Time-Travel
Pause a run, inspect state, resume with changed input (comparison).
- **Uses:** LangGraph4j `getStateHistory`/`stateOf` + `GraphInput.resume`, Temporal replay.

---

## 3. UI concept

**Guiding principles**
1. **Durability visible** — not "currently running", but "paused for 2 h waiting for signal X".
2. **Graph = navigation object** — nodes are clickable, state/IO per node visible.
3. **Two levels** — orchestration (Temporal: timeline, retries, signals) + reasoning
   (LangGraph4j: state, checkpoints, interrupts).
4. **HITL is an inbox**, not a dialog.
5. **Everything is explained by events** — log/event/LLM-IO consistent per run.

### 3.1 Information architecture
```
┌ QUAD / AgentFlow Studio ─────────────────────────────────────────────┐
│  Overview   Runs   Start   HITL   Logs   Config   Models   Settings   │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Screen: Overview (Observe)
```
┌ Overview ────────────────────────────────────────────────────────────┐
│ Running 12   Waiting HITL 3   Failed 2   Completed 148   p95 41s      │
│ ┌ Throughput (24h) ──────────┐  ┌ HITL queue ──────────────────────┐ │
│ │      ▂▄▆█▆▄▂               │  │ 2× UC-2 approval (SLA 4h)        │ │
│ └────────────────────────────┘  │ 1× UC-1 editorial                │ │
│ ┌ Latest errors ─────────────┐  └──────────────────────────────────┘ │
│ │ run-…  NODE_FAILED writer  │                                        │
│ └────────────────────────────┘                                        │
└───────────────────────────────────────────────────────────────────────┘
```

### 3.3 Screen: Runs + Run-Detail (Observe/Live)
```
┌ Run: agentflow-hitl-8f3a  (UC-1 Editorial) ──────────────────────────┐
│ status: WAITING_HITL   started 14:02   node: reviewer   ⏱ 00:03:12    │
│ ┌ Graph ───────────────┐ ┌ Timeline (Temporal) ────────────────────┐ │
│ │ planner ─► executor  │ │ 14:02 START                           │ │
│ │     ▲          │     │ │ 14:02 ACTIVITY runAgentGraph  ✓ 8.9s  │ │
│ │     │    ┌─────▼───┐ │ │ 14:02 INTERRUPT before reviewer       │ │
│ │     └────┤ reviewer│ │ │ 14:02 SIGNAL waiting…                 │ │
│ │          └─────┬───┘ │ │ 14:05 (awaiting human)               │ │
│ │                ▼ END │ └───────────────────────────────────────┘ │
│ └──────────────────────┘                                           │
│ ┌ Node: executor ─────────────────────────────────────────────────┐ │
│ │ input : {plan: "…"}          output: "…"      cost: 0.0031      │ │
│ │ [LLM request/response]  [Events]  [Checkpoint]                  │ │
│ └────────────────────────────────────────────────────────────────┘ │
│ [ Approve ]  [ Reject with reason ]                                 │
└──────────────────────────────────────────────────────────────────────┘
```
- **Data:** Temporal Describe/History (timeline, signals), LangGraph4j `getStateHistory`
  (state/checkpoint per node), activity outputs (node IO), quad-core `AgentEvent` (token/tool).
- **Live:** SSE/WebSocket pushes `RUN_STARTED/NODE_*/WAITING_HITL/APPROVED/FAILED`.

### 3.4 Screen: Start (Task-Start)
```
┌ Start a workflow ───────────────────────────────────────────────────┐
│ Workflow  [ UC-1 Editorial ▾ ]   Variant [ Standard ▾ ]             │
│ Input     ┌────────────────────────────────────────────┐            │
│           │ Summarise today's AI news situation…       │            │
│           └────────────────────────────────────────────┘            │
│ Overrides Model [ deepseek-v4.1-flash ▾ ]  Temp [0.2]  MaxTok [4096]│
│           Tools [☑ webSearch ☑ webfetch ☐ writeFile]               │
│ HITL      [✔ approve before the reviewer]  Assignee [editorial]      │
│ Schedule  ( ) now   ( ) Cron [ 0 7 * * * ]   ( ) Webhook             │
│                              [ Start ]                              │
└──────────────────────────────────────────────────────────────────────┘
```
- **Data:** `POST /agentflow/runs` (or `/runs/hitl`), graph/agent selection from config,
  overrides as parameters. Show the session ID (OpenCode Go) per run.

### 3.5 Screen: HITL-Inbox
```
┌ HITL Inbox ─────────────────────────────────────────────────────────┐
│ ⏰ SLA        Run             Step      Assignee     Created          │
│ ▸ 00:42  hitl-8f3a        reviewer  editorial   14:02               │
│ ▸ 02:10  uc2-gesuch-77    approval  compliance 12:30   ⚠ SLA soon   │
├─────────────────────────────────────────────────────────────────────┤
│ Detail: Step reviewer — proposal (Plan/result), Guardrails,         │
│ Tool calls, costs.  [ Approve ]  [ Reject+Reason ]  [ Reassign ]     │
└─────────────────────────────────────────────────────────────────────┘
```
- **Data:** `POST /runs/hitl/{id}/decide?approved=…` (Temporal signal). List = Temporal visibility
  query `ExecutionStatus=Running AND Waiting`.

### 3.6 Screen: Logs
```
┌ Logs ───────────────────────────────────────────────────────────────┐
│ Run [hitl-8f3a ▾] Level [INFO ▾] Node [all ▾] Search [          ]   │
│ 14:02:03 INFO  planner  LLM req 412 tok → resp 88 tok (0.9s)        │
│ 14:02:11 INFO  executor tool webSearch(query="AI News") → 8 hits    │
│ 14:02:11 WARN  executor context 9.8k > 8k → summarised              │
│ 14:02:12 INFO  reviewer WAITING_HITL (signal x-opencode-session=…)  │
└─────────────────────────────────────────────────────────────────────┘
```
- **Data:** structured logs per run/node; correlation via `runId`/`workflowId`/`activityId`.
  Additionally quad-core `LoggingHook`/`AgentEvent` and Temporal history.

### 3.7 Screen: Config
```
┌ Config ─────────────────────────────────────────────────────────────┐
│ [Graphs] [Agents] [Models/Providers] [Tools/MCP] [Skills] [Limits]  │
│ Graphs:   UC-1 Editorial  (nodes: planner,executor,reviewer)  [Edit]│
│ Agents:   Researcher (quad-core) tools=[webSearch,webfetch]        │
│ Providers:OpenCode Go  base=https://opencode.ai/zen/go/v1           │
│            User-Agent=my-test-agent/1.0  Header x-opencode-session ✔│
│ Limits:   timeout 5m  retries 3  maxIterations 10  context 8k        │
│ [ Validate ]  [ Publish version ]  [ Diff ]                         │
└─────────────────────────────────────────────────────────────────────┘
```
- **Data:** config CRUD (new API needed). Provider headers (optional) visible/controllable here.
  Versioning as in `docs/WORKFLOW_FORMAT_PROPOSAL.md` (immutable, `contentHash`).

---

## 4. Data flow & API needs (gaps)

| UI need | Today | Required API |
|---|---|---|
| Run list | — | `GET /runs?status=&workflow=` (Temporal visibility) |
| Run detail | — | `GET /runs/{id}` (Describe + history + state) |
| Live update | — | `GET /runs/{id}/events` (SSE/WS) |
| HITL queue | only signal endpoint | `GET /hitl` + `POST /hitl/{id}/decide` |
| Node-IO/LLM-IO | — | `GET /runs/{id}/nodes/{nodeId}` |
| Logs | — | `GET /runs/{id}/logs` |
| Config | `GET /info` | Graph/agent/provider CRUD + validate |
| Start | ✅ `/runs*` | + graph selection/schedule/overrides |

**Mapping to the engines**
- **Temporal:** `WorkflowClient.newWorkflowStub(...).describe()`, `getResult`, `Query`,
  history via `WorkflowServiceStubs`, visibility (`ListWorkflowExecutions`).
- **LangGraph4j:** `CompiledGraph.getStateHistory(config)`, `stateOf/ lastStateOf` → checkpoint/state.
- **quad-core:** `AgentEventPublisher`/`AgentEvent` (token, ToolCall, BeforeInvocation…) as
  event source; `LoggingHook` for logs.
- **Integration:** UI ↔ AgentFlow REST + SSE. For graphs there is additionally **LangGraph4j Studio**
  (embeddable) and **AG-UI** — usable as an inspect/dev tool instead of a custom build.

---

## 5. Non-functional aspects

- **Security:** OIDC/Keycloak (like quad), roles for HITL (`approver`, `admin`), secrets never in the UI.
- **Multi-Tenancy:** Temporal **namespaces** per tenant; checkpoint DB/workflow IDs separated.
- **GDPR/PII:** checkpoints contain prompts/outputs → masking + retention; audit trail.
- **Costs:** aggregate token/cost per node; budgets per workflow.
- **Observability:** OTel traces (activity↔node), metrics (Micrometer) → Prometheus/Grafana.
- **Reliability:** idempotent activities, idempotency keys for external effects, DLQ.

---

## 6. Roadmap (phases)

| Phase | Content | Benefit |
|---|---|---|
| P1 | Run list + detail (Describe/History), SSE events | Observe basis |
| P2 | HITL inbox + approve/reject (UI) | real approvals |
| P3 | Logs/node IO + costs | Diagnosis |
| P4 | Start form (graph selection, overrides, schedule) | Self-service |
| P5 | Config CRUD + versioning (v2 format) | Governance |
| P6 | Replay/what-if (checkpoints) | Debug/experiment |
| P7 | Multi-tenant/RBAC/audit | Enterprise |

---

## 7. Open questions

1. **Own UI or embed LangGraph4j Studio/AG-UI?** (Studio saves graph viz.)
2. **Temporal UI vs. own observe view** — combine or replace?
3. **Reuse quad-ui** (Win98 React) or an own modern frontend?
4. **Config truth** — v2/OWS as the canonical format (see `docs/WORKFLOW_FORMAT_PROPOSAL.md`)?
5. **HITL routing** — roles/assignees/SLA; escalation via Temporal timer?

---

*Reference:* `docs/AGENT_STACK_EVOLUTION.md` (§7 evolution path, §10 risks) and
`docs/WORKFLOW_FORMAT_PROPOSAL.md` (definition/versioning).
