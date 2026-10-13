# AgentFlow UI — Design & Specification

**Status:** Draft (2026-10-10) · Goal: Angular web app in the `agentflow-ui` module.

This document describes the planned user interface for AgentFlow: information architecture,
screens, interactions, data flow, API mapping, design system and roadmap. The implementation
is done as a standalone Angular module (`agentflow-ui/`), which uses the backend (`agentflow-app`)
via REST.

---

## 1. Goals & guiding principles

AgentFlow is **durable** (Temporal), **graph-based** (LangGraph4j) and **quad-composed**
(tools/skills/guardrails/memory/saga/…). The UI makes exactly these capabilities operable:

1. **Observe first** — make runs, status, metrics, provenance/audit visible.
2. **Start in seconds** — every engine/case startable via a form, without curl.
3. **Editable without code** — validate and save declarative `quad.workflow` v2 definitions
   (workflow **and** agents) in the browser.
4. **Human-in-the-Loop** — find paused runs, send decisions.
5. **Security & transparency** — show effective settings, **never** secrets.
6. **No maintenance burden** — one SPA, one build artifact, deployable statically.

**Non-goals (v1):** drag-and-drop graph editor, user management/login, multi-tenant,
persistence of workflow changes across restarts (runtime override instead of file write).

---

## 2. Tech stack & architecture

| Aspect | Decision | Rationale |
|---|---|---|
| Framework | **Angular 20**, Standalone Components | Requirement; Signals + `provideHttpClient` |
| State | **Signals** + small stores (no NgRx) | low complexity, reactive |
| Routing | `provideRouter` with lazy loading | fast startup |
| HTTP | `HttpClient` + typed services | REST against `agentflow-app` |
| Styling | SCSS + CSS variables (own design system) | no heavy UI deps |
| Build | Angular CLI / `ng build` | static assets → deployable by the backend/reverse proxy |
| Tests | Vitest/Jasmine (CLI default) + mocked services | ensure buildability |

**Deployment paths**
- Dev: `ng serve` (proxy is wired in `angular.json`: `proxy.conf.json` → `/agentflow`, `/actuator`, `/examples` go to `http://localhost:8095`).
- Prod: `ng build` → `dist/` static; either its own static hosting or served by the backend
  (`spring.web.resources`) under `/ui`.

---

## 3. Information architecture

```
┌──────────── Sidebar ────────────┐
│ ▣ Dashboard        (/)           │   Overview: health, metrics, counts, quick start
│ ▶ Runs             (/runs)       │   start + list + detail + signals
│ ⧉ Workflows        (/workflows)  │   declarative defs + editor (v2 JSON) + validate/save
│ ✦ Generate         (/generate)   │   task → LLM-generated v2 workflow (preview/save/run)
│ ⚙ Agents           (/agents)     │   agents from all defs + editor
│ ⏸ HITL             (/hitl)       │   paused/approval-required runs + decisions
│ ∿ Observability    (/observe)    │   metrics, provenance, audit, Temporal link
│ ☰ Examples         (/examples)   │   the five business cases
│ ⚙ Settings         (/settings)   │   effective configuration (read-only, without secrets)
└──────────────────────────────────┘
Topbar: app title · backend status badge (health) · Temporal UI link · theme toggle
```

---

## 4. Screens im Detail

### 4.1 Dashboard `/`
**Purpose:** situational overview + entry point.
- **Status cards:** backend (`/actuator/health`), Temporal connection (derived from the runs list),
  number of workflow definitions (`/agentflow/workflows`), number of capabilities (`/agentflow/capabilities`).
- **Metric tiles:** `agent.llm.calls`, `agent.tool.executions`, `agent.errors`,
  `agent.execution.duration` (from `/actuator/prometheus`, filtered).
- **Quick start:** "Engine" dropdown (quad, quad-rich, authorized, structured, provenance,
  subagents, metrics, capability-search, saga, declarative, examples/*) + prompt field → starts.
- **Latest runs:** from the runs list (`/agentflow/runs?limit=10`).
- **Engine info:** `/agentflow/info` (graph/durability/quad-reuse).

### 4.2 Runs `/runs`, `/runs/:id`
**List:** table with `workflowId`, `workflowType`, `status`, `startTime`, `duration`, `taskQueue`.
- Filter: free text (Temporal query, e.g. `ExecutionStatus = "Running"`), time range, type.
- Actions per row: *detail*; *terminate* only for running runs (`status = RUNNING`) — finished
  runs cannot be terminated (the backend then responds with `409 Conflict`).
- **Start panel** (right/top): forms per engine with the respective parameters
  (input, tools, roles, orderId+amount, op/scope/content, serverUrl/tool/args). For the engine
  **declarative**, `workflowId` is a **dropdown of the existing definitions**
  (`GET /agentflow/workflows`, display `Name (id)`) + task — no ID input needed.

**Detail `/runs/:id`:**
- Header data + status badge, history length, timing (`describe`).
- **Result** (if completed): payload string (JSON pretty-print).
- **Signals:** context-dependent "HITL: approve/reject", "Declarative: approve", "Escalate".
- **Timeline:** raw event list (if available) + deep link into the Temporal UI.

### 4.3 Workflows `/workflows`, `/workflows/:id`
**List:** id, name, version, status, number of nodes, agent refs. **+ new workflow** (enter name)
→ `POST /agentflow/workflows` creates an empty v2 document (only `end`) and opens it in the detail view.
**Detail:**
- **Graph view** (v1: nested list/table by `dependsOn` topology,
  `type` badges `agent|decision|loop|fork|join|hitl|subworkflow|end`,
  `requiresHumanApproval` marker).
- **Agents** of the document (`agents[]`): id, model, tools, systemPrompt, guardrails/hooks.
- **Editor:** JSON textarea of the v2 document with:
  - *validate* → `POST /agentflow/workflows/validate` (publish compiler + semantic validator).
  - *save* → `PUT /agentflow/workflows/{id}` (runtime override, immediately runnable).
  - Error list with JSON pointer; buttons *format*, *reset*, *download*.
- **Run this** → starts `POST /agentflow/runs/declarative?workflowId=…&task=…`.

### 4.4 Generate `/generate`
**Purpose:** generate a runnable v2 workflow from a task (LLM decomposition).
- Form: task (textarea), name (optional).
- Actions:
  - *preview* → `POST /agentflow/workflows/generate` (`{task,name?}`) → published v2 document
    (not persisted).
  - *save* → `POST /agentflow/workflows/generate/save` → runtime override + file; appears
    under workflows/agents.
  - *run* → `POST /agentflow/workflows/generate/run` → durable Temporal run (result under runs).
- Preview: node/agent counters, node table (id/type/title/dependsOn/agent), agent chips and JSON.

### 4.5 Agents `/agents`
- Flat list of all agents across all workflow definitions (id, name, model, guardrail summary,
  def).
- **Editor:** edits the `agents[]` section of the associated document (form:
  id/name/model/temperature/maxTokens/tools/systemPrompt/jsonOutput) and saves via the same
  `PUT` endpoint (changes the definition, not the workflow graph).
- **+ new agent:** choose target definition → new agent (unique id, defaults) is added to the
  `agents[]` (appears immediately in the graph editor's agent-ref dropdown).
- **Guardrails & hooks:** multi-selects *guardrails (input/output)* (`pii`, `sql-injection`,
  `prompt-injection`) → `guardrailsInput`/`guardrailsOutput`; *PII masks* → hook `pii:…`;
  checkbox *audit trail* → hook `audit`. Values from `GET /agentflow/quad/capabilities`.
- *test run*: starts a quad run with the `systemPrompt` as prompt (quick test).

### 4.6 HITL `/hitl`
- Filters the runs list to "waiting": `ExecutionStatus = "Running"` **and** workflow type in
  {`HitlWorkflow`, `DeclarativeAgentWorkflow`, `AuthorizedAgentWorkflow`, `SupportTriageWorkflow`,
  `ContractReviewWorkflow`}.
- Card per run: type, start time, waiting for what; actions *approve* / *reject* / *escalate*.
- Optional: aging indicator (how long waiting).

### 4.7 Observability `/observe`
- **Metrics:** `agent_*` families filtered from `/actuator/prometheus`; `agent.llm.calls`,
  `agent.tool.executions`, `agent.errors`, `agent.execution.duration.seconds_*`, token summaries.
- **Provenance:** "provenance run" button → `POST /agentflow/runs/provenance`, result as a table
  (tool, source, time).
- **Audit/logs:** guided run (`/runs/provenance`, `/runs/metrics`) + raw Prometheus text
  as a collapsible block; deep link to Temporal UI for event history.
- **Note:** there is (not yet) a log-file API; logs = metrics + run history + audit hook output.

### 4.8 Examples `/examples`
- Cards from `GET /examples` (id, title, endpoint, capabilities).
- Per case a startable form (reads the `endpoint` path and offers the matching fields).

### 4.9 Settings `/settings`
- **Read-only** effective configuration (`GET /agentflow/settings`), grouped:
  - *LLM*: model, base-url, `hasApiKey` (boolean — **no** key), `headersConfigured` + header **names**.
  - *Durability*: temporal target/namespace, taskQueue.
  - *Capability search*: index (hnsw/pgvector), table, autoload, cache.
  - *Workflows*: location.
  - *Checkpoint*: path.
  - *Endpoints*: metrics, temporal-ui.
- Changes deliberately **only** via env/restart (no hot config v1) — the UI shows where they are set.
- **Security rule:** secrets are never serialized or displayed.

---

## 5. API mapping (backend endpoints)

### Existing
| Method | Path | Purpose |
|---|---|---|
| POST | `/agentflow/runs` | LangGraph workflow |
| POST | `/agentflow/runs/quad` | quad `Agents.simple()` |
| POST | `/agentflow/runs/quad-rich` | Tools/Skills/Guardrails/PII/Audit |
| POST | `/agentflow/runs/authorized` | RBAC |
| POST | `/agentflow/runs/hitl` + `/hitl/{id}/decide` | HITL |
| POST | `/agentflow/runs/saga` | Saga |
| POST | `/agentflow/runs/quad-saga` | quad session saga |
| POST | `/agentflow/runs/structured` | Structured Output |
| POST | `/agentflow/runs/provenance` | Provenance |
| POST | `/agentflow/runs/memory?op=remember\|recall` | Memory |
| POST | `/agentflow/runs/mcp?op=list\|run` | MCP |
| POST | `/agentflow/runs/subagents` | Sub-agents |
| POST | `/agentflow/runs/metrics` | Metrics/OTel |
| POST | `/agentflow/runs/capability-search` | Capability/RAG |
| POST | `/agentflow/runs/declarative` + `/{id}/approve` | declarative v2 |
| GET | `/agentflow/info`, `/quad/capabilities`, `/security/roles` | Info/config |
| GET | `/agentflow/capabilities`, `/capabilities/search?q=` | Capability catalog |
| GET | `/agentflow/workflows`, `/workflows/{id}` | Declarative defs |
| GET | `/examples` | Business cases |
| GET | `/actuator/health`, `/metrics`, `/prometheus` | Observability |

### New (added for the UI)
| Method | Path | Purpose |
|---|---|---|
| GET | `/agentflow/runs?query=&limit=` | Run list (Temporal visibility) |
| GET | `/agentflow/runs/{id}` | Run detail (status + result if applicable) |
| POST | `/agentflow/runs/{id}/terminate` | Admin abort |
| POST | `/agentflow/workflows/validate` | Validate v2 document (compiler+semantics) |
| PUT | `/agentflow/workflows/{id}` | Save v2 document (runtime override) |
| GET | `/agentflow/settings` | Effective config without secrets |
| GET | `/agentflow/runs/stream?query=` | **SSE**: run list live |
| GET | `/agentflow/runs/{id}/stream` | **SSE**: status **+ history events** until terminal |
| GET | `/agentflow/runs/{id}/events?max=` | History events (log) as JSON |
| POST | `/agentflow/testbench/run` | Single-agent trace (tokens/tools/cost/provenance) |
| GET | `/agentflow/workflows/{id}/versions` | Version snapshots |
| GET | `/agentflow/workflows/{id}/versions/{v}` | Load version |
| POST | `/agentflow/workflows/generate` | LLM: task → v2 document (preview) |
| POST | `/agentflow/workflows/generate/save` | generate + save (override + file) |
| POST | `/agentflow/workflows/generate/run` | generate + run durably (Temporal) |
| POST | `/agentflow/workflows` | create a new, empty workflow (only `end`) |

Optional headers (only when enabled in the backend): `X-API-Key`/`Authorization: Bearer`
(API-key gate), `X-Tenant-Id` (capability scoping).

---

## 6. Data flow & state

```
ApiService (HttpClient)
   ├── HealthService      → /actuator/health            (Poll 10s)
   ├── RunsService        → /agentflow/runs(/…)         (Poll 5s)
   ├── WorkflowService    → /agentflow/workflows(/…), validate, save
   ├── AgentService       → derived from workflows (+ test run)
   ├── CapabilityService  → /agentflow/capabilities(/search)
   ├── MetricsService     → /actuator/prometheus        (Poll 15s)
   ├── ExampleService     → /examples
   └── SettingsService    → /agentflow/settings

Stores (Signals):
   RunsStore   { runs, filter, selected, loading, error }
   WorkflowStore { definitions, draft, validationErrors, dirty }
   ToastStore  { messages }
```
- **Polling** instead of SSE/WebSocket (Temporal provides no push API to the client).
- Errors are shown centrally as a toast; HTTP status visible.

---

## 7. Design system

- **Colors:** dark theme by default, light theme switchable via CSS variables.
  Accents: indigo (primary), amber (warn/waiting), green (ok), red (error).
- **Typography:** system stack; monospace for IDs/JSON.
- **Components:** `AppShell`, `StatCard`, `DataTable`, `StatusBadge`, `JsonEditor`,
  `FormField`, `Toast`, `Tabs`, `EmptyState`, `Spinner`.
- **Status colors:** RUNNING=blue, COMPLETED=green, FAILED/TIMED_OUT=red, CANCELED/TERMINATED=gray,
  WAITING/approval=amber.
- **Accessibility:** focus rings, `aria-label`, sufficient contrasts, keyboard operation.
- **Language:** German (labels/error messages), code/IDs in English.

---

## 8. Security

- No secrets in the UI/API response; `/settings` only returns `hasApiKey`.
- Admin actions (terminate) with a confirmation dialog.
- CSP-friendly, static assets; no external CDNs.
- CORS: bypassed in dev via the Angular proxy; in prod it is recommended to serve the UI from the same origin.

---

## 9. Implemented (v2)

1. **Persistence & versioning**: `WorkflowDefinitionStore` writes published defs to
   `agentflow.workflows.state-dir` and, before each overwrite, stores a snapshot under
   `versions/<id>/<timestamp>.json` (git-friendly). UI tab "Versions" (load/view).
2. **Visual graph editor**: `af-graph-editor` — layered layout, drag (position per workflow in
   localStorage), **set dependencies** via checkbox list and **click-to-connect** (🔗 source→target),
   rename/delete with **reference integrity** (updates `dependsOn`/branches/`bodySteps`/`defaultTarget`),
   `+ Node`/`+ connected`, targets as dropdowns. Changes are applied to the JSON in a schema-conformant way.
3. **SSE complete**: `/agentflow/runs/stream` (list) and `/agentflow/runs/{id}/stream`
   (status **+ incremental history events**); additionally REST `GET /agentflow/runs/{id}/events`.
   The UI shows a live log (event ID/type/detail/time) and uses `EventSource` with REST fallback.
4. **All node types in the runner**: `DeclarativePlanner` flattens (sub-workflows inline, alias to
   `end`) + `DeclarativeAgentWorkflow` interprets `agent/decision/loop/fork/join/hitl/end`
   (dependency-driven, branch pruning, loop exit conditions, per-node HITL signal `decide`).
5. **Auth: OIDC (disabled) + API key/tenant (opt-in)**: `WebSecurityConfig` serves permissively
   (default, `agentflow.security.oidc.enabled=false`); when activated, a
   **JWT resource server** (`issuer-uri`/`jwk-set-uri`) is enabled. Separately `ApiKeyFilter`
   (`agentflow.security.enabled`) and `TenantFilter` (`X-Tenant-Id` scoping).
6. **Agent testbench**: `/agentflow/testbench/run` returns answer, input/output tokens,
   tool calls, duration, cost estimate and tool provenance; UI page "Testbench".
7. **Dynamic workflow generation**: `/agentflow/workflows/generate[/save|/run]` — an LLM
   ("workflow architect", quad-core path) decomposes a task into a v2 workflow (agent steps
   + exactly one `end`), published via the same compiler/validator; UI page "Generate"
   (preview/save/run).
8. **Create agents/workflows + guardrails/PII/audit**: `POST /agentflow/workflows` (empty
   workflow), "+ new agent" on `/agents`. Declarative guardrails (`guardrailsInput`/
   `guardrailsOutput`), PII masking and audit (`hooks`) are enforced in the runner via quad-core's
   `GuardrailPlugin` (hook pipeline, `beforeModelCall`/`afterModelCall`), `PiiAnonymizerHook` and
   `AuditTrailHook`; names via `GET /agentflow/quad/capabilities`.

### Roadmap (further)
- Enable OIDC + Keycloak realm (roles → tenant mapping); `agentflow.security.oidc.enabled=true`.
- Persistence trigger "git commit" on save (hook).
- Graph editor: undo/redo, multi-select, delete edges by clicking the line.
- Align `decision` with quad/kafka (regex/expression) — for comparison see `DECISION_NODE_COMPARISON.md`,
  deliberately **not yet** implemented.

---

## 10. Implementation checklist (v1)

- [x] App shell + routing + design system
- [x] Dashboard (health, metrics, counts, quick start)
- [x] Runs (list, detail, start forms, signals/terminate)
- [x] Workflows (list, detail, JSON editor with validate/save)
- [x] Agents (list, editor, test run)
- [x] HITL (waiting runs + decisions)
- [x] Observability (metrics, provenance, Temporal link)
- [x] Examples (cards + forms)
- [x] Settings (read-only, without secrets)
- [x] Visual graph editor (drag/properties, all node types)
- [x] Persistence + versions (file + snapshot, UI tab)
- [x] SSE live updates (list + single run **with history events**)
- [x] Runner for all node types (decision/loop/fork/join/subworkflow/hitl)
- [x] Auth (OIDC **disabled** + API key/tenant opt-in) + testbench page
- [x] Generate (LLM task → v2 workflow: preview/save/run)
- [x] Graph editor: set/connect dependencies, add/rename/delete
- [x] `decision` comparison quad/kafka/agentflow (`DECISION_NODE_COMPARISON.md`)
- [x] Proxy + build + README
