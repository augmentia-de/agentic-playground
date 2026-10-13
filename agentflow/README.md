# AgentFlow

Separately deployable module that combines three complementary Java building blocks:

```
Graph/Agentic reasoning   →  LangGraph4j 1.9.4   (StateGraph, conditional edges, HITL-ready)
Framework / model calls   →  Spring AI 2.0.1     (ChatClient; MCP-capable)
Durable orchestration     →  Temporal 1.40.0     (deterministic replay, retries, timers)
Reuse                     →  quad-core           (Agent/Tools/Skills/MCP/Guardrails)
```

Position in the monorepo: sibling of `quad-quarkus` and `kafka-platform`; registered in the
root aggregator. Builds/deploys independently.

## Modules

| Module | Inhalt | Abhängigkeiten |
|---|---|---|
| `agentflow-core` | Framework-neutraler **LangGraph4j**-Graph (`AgentFlowGraph`, `AgentFlowState`) | nur `langgraph4j-core` |
| `agentflow-examples` | Fünf **Sample-Business-Cases** als durable Temporal-Workflows (Support-Triage, Contract-Review, Invoice-Saga, Market-Research, Onboarding) | `agentflow-core`, `quad-core`, Temporal |
| `agentflow-app` | **Spring Boot**-App: Graph-Reasoning via **Spring AI** in einer **Temporal**-Activity, Workflow + REST | `agentflow-core`, `agentflow-examples`, `quad-core`, Spring AI, Temporal |
| `agentflow-ui` | **Angular 20** Webapp: Dashboard, Runs, Workflow-/Agent-Editor, HITL, Observability, Examples, Settings (siehe `UI.md`) | `agentflow-app` (REST) |

The split is the point: **Temporal owns durability**, **LangGraph4j owns the graph**, the
LLM/tool calls live in **activities** (never in deterministic workflow code).

`agentflow-examples` is a plain library that `agentflow-app` component-scans (`de.augmentia.agentflow`),
so its workflows/activities are served by the same worker and exposed via REST.

## Build

```bash
# nur dieses Modul (quad-core muss im lokalen Repo liegen)
mvn -pl agentflow -am -DskipTests package      # aus dem Root-Aggregator
# oder im Modul:
cd agentflow && mvn -DskipTests package
```

## Run (Temporal + App)

**Schnellstart (empfohlen)** — startet Temporal (Docker), App und optional die UI und legt
Beispiel-Daten an:

```bash
cd agentflow
./start-agentflow.sh                       # nutzt OpenAI-Settings aus quad-quarkus/.env
./start-agentflow.sh --print-config        # zeigt geladene .env + effektive Werte (Key maskiert)
./start-agentflow.sh -k "sk-..." --no-ui   # Key überschreiben, ohne UI
./start-agentflow.sh --no-demo-runs        # nur Memory/Capabilities seeden
```

Manuell:

```bash
# 1) Temporal dev server (Docker)
cd agentflow/deploy && docker compose up -d        # Temporal :7233, UI :8233

# 2) App starten
export OPENAI_API_KEY=...                           # OpenAI-kompatibel
export OPENAI_BASE_URL=https://api.openai.com/v1    # beliebiger kompatibler Endpoint
export OPENAI_MODEL=gpt-4o-mini
cd agentflow && mvn -pl agentflow-app spring-boot:run

# 3) Workflow starten
curl -X POST "http://localhost:8095/agentflow/runs?input=Summarize%20Temporal%20vs%20Kafka"
# → { "workflowId": "agentflow-...", "started": true }
# Fortschritt/Ergebnis: Temporal UI http://localhost:8233
```

`GET /agentflow/info` shows which building blocks are active (and that `quad-core` is reused).

## LLM configuration (OpenAI-compatible)

agentflow uses a single, OpenAI-compatible configuration — no separate provider switch:

```bash
export OPENAI_API_KEY=sk-...                        # API key (empty → hasApiKey=false)
export OPENAI_BASE_URL=https://api.openai.com/v1    # or any compatible endpoint
export OPENAI_MODEL=gpt-4o-mini
```

`application.yml` maps these to `spring.ai.openai.base-url/-key/chat.options.model`.
`LlmConfigBridge` copies the same values onto `OPENAI_*`/`QUAD_LLM_*` so the quad/LangChain4j
agents use the identical endpoint/model/key. Set `OPENAI_BASE_URL` to e.g.
`https://opencode.ai/zen/go/v1` if you want that endpoint — it is just OpenAI-compatible.

**Shared settings:** `start-agentflow.sh` loads `quad-quarkus/.env` (which already defines
`OPENAI_API_KEY`/`OPENAI_BASE_URL`/`OPENAI_MODEL`) automatically, so the same OpenAI setup is reused.
Priority: CLI (`-k`/`--base-url`/`--model`) > `agentflow/.env` > `quad-quarkus/.env` > defaults.
Check what is effective with `./start-agentflow.sh --print-config`.

> **Duplicate keys:** sourcing a `.env` keeps the **last** value. `quad-quarkus/.env` defines
> `OPENAI_*` twice (openrouter + opencode), so the last block silently won. The script now warns
> about duplicated keys — **pin** the endpoint in `agentflow/.env` (see `.env.example`) so agentflow
> is deterministic.

Probe the endpoint (add whatever headers your endpoint requires, see below):

```bash
curl -sS -o /dev/null -w "%{http_code}\n" "$OPENAI_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $OPENAI_API_KEY" -H "Content-Type: application/json" \
  -H "User-Agent: agentflow/1.0" \
  -d "{\"model\":\"$OPENAI_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}],\"max_tokens\":1}"
```

### Configurable HTTP headers for LLM calls

Headers are **only** what you configure under `agentflow.llm.headers.*`; if nothing is configured,
**no headers** are sent (implemented from `eys.md`). There is no provider-specific default.

```yaml
agentflow:
  llm:
    fallback-request-id: agentflow-process
    headers:
      User-Agent: "agentflow/1.0 (run {{requestId}})"   # partly dynamic
      x-opencode-session: "{{requestId}}"               # dynamic (sticky) — only if the endpoint needs it
      HTTP-Referer: https://agentflow.local             # static
      Authorization: "Bearer {{apiKey}}"                # from the configured key
```

- Placeholders (double braces on purpose): `{{requestId}}`/`{{sessionId}}` (per run; fallback
  `fallback-request-id`, else a stable process id) and `{{apiKey}}`.
- **Spring AI**: applied per request by an OkHttp interceptor reading the `LlmHeaderResolver`; the
  run id comes from the activity's Temporal workflow id (`RequestContext`).
- **LangChain4j (quad)**: `LlmConfigBridge` resolves the same set at startup and maps it to
  `QUAD_LLM_*`.
- **Direct HTTP**: there is no direct-HTTP LLM path in agentflow (only Spring AI/OkHttp +
  LangChain4j); should one be added, wire the same `LlmHeaderResolver` (`eys.md` §5.3).
- Example (OpenCode needs `User-Agent` + `x-opencode-session`) via `agentflow/.env` — note the
  **single quotes** (otherwise bash strips the inner JSON quotes):
  ```bash
  SPRING_APPLICATION_JSON='{"agentflow":{"llm":{"headers":{"User-Agent":"agentflow-local/1.0","x-opencode-session":"{{requestId}}"}}}}'
  ```
  On the LangChain4j/quad path `{{requestId}}` resolves to the process-scoped fallback
  (`agentflow.llm.fallback-request-id`); the Spring AI path uses the actual per-run id.

### Smoke-test a single LLM call

Two ways, both use the configured base URL / key / model / headers:

1. **Preflight without the app** — one raw `chat/completions` request with the same settings and
   headers (reads `quad-quarkus/.env` + `agentflow/.env`):
   ```bash
   cd agentflow && ./llm-smoke.sh "Sag in einem kurzen Satz Hallo."
   ```
   Shows the resolved base URL/model/headers (key masked) and the HTTP status (e.g. `HTTP 200`,
   or `HTTP 402 Insufficient account funds`).
2. **Through agentflow** — the agent testbench runs a **single** agent call synchronously and
   returns answer/tokens/duration/cost:
   ```bash
   curl -sS -X POST http://localhost:8095/agentflow/testbench/run \
     -H 'Content-Type: application/json' \
     -d '{"systemPrompt":"Antworte in einem kurzen Satz.","prompt":"Sag kurz Hallo."}'
   ```
   (Optional `tools`: e.g. `["readFile","writeFile"]`.)

## Reuse of quad-core (deep)

`agentflow-app` depends on `de.augmentia.quad:quad-core`, `quad-tool-builtin` **and**
`quad-tool-mcp`. `QuadCapabilities` composes a rich quad agent via the canonical `AgentBuilder`:

- **Built-in tools** (workspace-confined): `readFile`, `writeFile`, `listDirectory`,
  `grepSearch`, `findFiles`, `webSearch`, `webfetch` — registered in a `QuadToolRegistry`.
- **Skills (JIT)**: `withSkills(dir)` → `load_skill` / `skill_search`.
- **Guardrails**: `GuardrailPlugin` (prompt-injection).
- **PII/GDPR**: `PiiAnonymizerHook` (EMAIL/PHONE/CREDIT_CARD → `[REDACTED]`).
- **Audit**: `AuditTrailHook` (in-memory `AuditStore`).
- **Events/Workspace/Session**: `AgentEventPublisher` + `LoggingHook`, `WorkspaceResolver`,
  `CurrentSession`. Model via `ModelFactory` (reads `OPENAI_*`/`QUAD_LLM_*`, filled by
  `LlmConfigBridge` from `spring.ai.openai.*` + configured headers).

Endpoints:
```bash
# rich quad agent: tools (comma list, empty = all) + optional skills dir
curl -X POST "http://localhost:8095/agentflow/runs/quad-rich?input=List%20the%20workspace&tools=readFile,listDirectory&skillsDir=./skills"
# role-scoped agent (RBAC): tools are limited to the caller's roles
curl -X POST "http://localhost:8095/agentflow/runs/authorized?input=Summarise%20the%20README&roles=reader"
# durable saga: reserve → charge; amount>100 triggers compensation
curl -X POST "http://localhost:8095/agentflow/runs/saga?orderId=o-1&amount=200"
# quad-core session-level saga (registerCompensation + unwind)
curl -X POST "http://localhost:8095/agentflow/runs/quad-saga?input=demo"
# structured output (typed ContentPlan)
curl -X POST "http://localhost:8095/agentflow/runs/structured?input=Plan%20a%20blog%20about%20Temporal"
# provenance (tool origins)
curl -X POST "http://localhost:8095/agentflow/runs/provenance?input=What%20time%20is%20it%3F"
# long-term memory (remember / recall)
curl -X POST "http://localhost:8095/agentflow/runs/memory?op=remember&scope=project-a&content=Uses%20Temporal"
curl -X POST "http://localhost:8095/agentflow/runs/memory?op=recall&scope=project-a"
# MCP over SSE (list tools / run a tool)
curl -X POST "http://localhost:8095/agentflow/runs/mcp?op=list&serverUrl=http://localhost:3001/sse"
# orchestrator delegating to a sub-agent
curl -X POST "http://localhost:8095/agentflow/runs/subagents?input=Research%20and%20summarise%20Temporal"
# agent with quad MetricsHook (Micrometer) + TracingHook (OTel)
curl -X POST "http://localhost:8095/agentflow/runs/metrics?input=Hello"
curl http://localhost:8095/actuator/prometheus
# capability search / RAG over the tool+skill catalog
curl -X POST "http://localhost:8095/agentflow/runs/capability-search?input=Find%20files%20and%20search%20the%20web&query=file%20web"
curl "http://localhost:8095/agentflow/capabilities/search?q=web&topK=5"
curl http://localhost:8095/agentflow/capabilities
# capability / role introspection (for the UI config screen)
curl http://localhost:8095/agentflow/quad/capabilities
curl http://localhost:8095/agentflow/security/roles
```

**RBAC**: `QuadSecurity` maps roles → tools via quad-core `RoleBasedToolGuard`; tools outside
the role are not registered (deny by absence). Default roles: `reader`, `researcher`, `writer`.
**Saga**: `SagaWorkflow` orchestrates forward steps and compensations durably (Temporal);
`SagaActivitiesImpl.quadSaga` also demonstrates quad-core's session-level saga
(`AgentSessionState.registerCompensation` + `SagaAgentInterceptor.unwindSaga`).
**Structured Output**: `withStructuredOutput(ContentPlan.class)` → `executeStructured(...)`.
**Memory**: quad-core `PersistentMemoryStore` (in-memory here) with `MemoryEntry`/`MemoryCategory`.
**Provenance**: quad-core `ProvenanceTracker` attached as an event listener.
**MCP**: `SseMcpProxy` loads/calls tools from an MCP server over SSE (URL is a parameter).
**Sub-agents**: orchestrator built with `withSubAgent("researcher", ...)` (quad-core `SubAgentTool`).
**Metrics/OTel**: quad-core `MetricsHook` (Micrometer, exported at `/actuator/prometheus`) and
`TracingHook` (OTel) attached to the agent's event publisher. `AgentTelemetry` wires the
`MetricsHook` into **all** agent build paths (declarative runner, rich/authorized, structured,
provenance, sub-agents), so the `agent_*` counters/timers actually increment. Token counts are the
provider-reported usage (quad-core `ModelRespondedEvent`): `agent_tokens_prompt_*` = input tokens,
`agent_tokens_completion_*` = output tokens.
**Capability search/RAG**: quad-core `CapabilitySearch`/`CapabilityIndex` retrieve tools/skills for a
query (via `CapabilitySearchTool` at runtime, or synchronously via REST). Default backend is the
in-memory `HnswCapabilityIndex`; set `agentflow.capability.index=pgvector` (+ a `DataSource`,
`spring-boot-starter-jdbc` and the PostgreSQL driver) to use `PgVectorCapabilityIndex`. The
catalog is seeded at startup from `CapabilityCatalog` (`agentflow.capability.autoload`).

Not wired yet (next stage): skill/tool discovery via `ToolActivatorTool` — see `Ideas.md` §1.1.

Existing values from `docs/AGENT_STACK_EVOLUTION.md`: keep the durable/graph boundaries explicit,
never retry non-idempotent effects without keys, and keep checkpoints slim.

## Implemented

**(a) quad-core agent as activity** — `POST /agentflow/runs/quad?input=...` runs a
`quad-core` `Agents.simple()` agent (LangChain4j) inside a durable Temporal activity.

**(b) Human-in-the-Loop** — the LangGraph4j graph pauses before the reviewer
(`CompileConfig.interruptBefore(REVIEWER)`); the Temporal workflow exposes a signal:
```bash
curl -X POST "http://localhost:8095/agentflow/runs/hitl?input=..."          # → workflowId, waiting
curl -X POST "http://localhost:8095/agentflow/runs/hitl/<id>/decide?approved=true"
```
The wait is durable (worker restarts are fine).

**(c) Persistent checkpoints** — `SQLiteSaverV2` stores graph checkpoints in a SQLite file, so
a paused graph resumes in a new worker instance (verified by `PersistentCheckpointTest`).
Configure via `agentflow.checkpoint.path` (default `target/agentflow-checkpoints.db`).

## Examples — five sample business cases

`agentflow-examples` ships five end-to-end cases that compose the capabilities above. They are
served by the same app/worker; see `agentflow-examples/README.md` for details.

```bash
curl http://localhost:8095/examples                       # list the cases
curl -X POST "http://localhost:8095/examples/support-triage?customerId=c-1&message=My%20invoice%20is%20wrong"
curl -X POST "http://localhost:8095/examples/support-triage/<id>/escalate?approved=true"
curl -X POST "http://localhost:8095/examples/contract-review?contract=..."
curl -X POST "http://localhost:8095/examples/contract-review/<id>/approve?approved=true"
curl -X POST "http://localhost:8095/examples/invoice?invoiceId=inv-1&amount=500"
curl -X POST "http://localhost:8095/examples/market-research?topic=Temporal%20vs%20Kafka"
curl -X POST "http://localhost:8095/examples/onboarding?newHire=Ada&projectScope=agentflow"
```

| Case | Komponierte Fähigkeiten |
|---|---|
| `support-triage` | Structured Output, Memory, Capability/RAG, HITL |
| `contract-review` | PII-Anonymisierung, Guardrail, Structured Output, HITL |
| `invoice-processing` | Saga/Kompensation (durabel), Structured Output |
| `market-research` | Sub-Agenten, Web-Tools |
| `onboarding` | Long-term Memory, Capability-Search, MCP |

## Declarative workflow definitions (`quad.workflow` v2)

Neben den Java-Workflows lädt `agentflow-app` **deklarative Definitionen** im kanonischen
`quad.workflow`-v2-Format (dieselben Schemas/Validatoren wie `quad-quarkus`/`kafka-platform`,
wiederverwendet aus `quad-workflow-core`):

- Dateien: `agentflow-app/src/main/resources/workflows/*.json` (die fünf Business-Cases,
  jeweils mit eingebettetem `agents[]`).
- Beim Start werden sie mit `QuadWorkflowPublishCompiler` publiziert (füllt `contentHash`,
  setzt `status: published`, prüft gegen das v2-JSON-Schema) und mit
  `QuadWorkflowSemanticValidator` geprüft (genau ein `end`, azyklisches `dependsOn`,
  auflösbare Referenzen). **Fail-closed**: eine ungültige Datei verhindert den Start.
- Ort konfigurierbar über `agentflow.workflows.location` (Default `classpath*:workflows/*.json`,
  z. B. `file:./workflows/*.json`).

```bash
# Definitionen auflisten / im Volltext (publiziert, mit Hashes)
curl http://localhost:8095/agentflow/workflows
curl http://localhost:8095/agentflow/workflows/market-research

# deklarativ definierten Workflow durabel ausführen (Agent-Knoten in Abhängigkeitsreihenfolge)
curl -X POST "http://localhost:8095/agentflow/runs/declarative?workflowId=market-research&task=EV%20batteries%20in%20Europe"
# Knoten mit requiresHumanApproval pausieren durabel:
curl -X POST "http://localhost:8095/agentflow/runs/declarative/<temporalId>/approve?approved=true"
```

Beispiel-Definition (Auszug): `mainWorkflow.nodes` mit `type: agent` + `agent.ref` auf einen
Eintrag aus `agents[]`; `requiresHumanApproval` markiert HITL-Knoten.

```jsonc
{ "format": "quad.workflow", "schemaVersion": 2,
  "mainWorkflow": { "id": "market-research", "name": "Market Research", "version": "1.0.0",
    "status": "draft", "nodes": [
      { "id": "scout", "type": "agent", "agent": { "ref": "ex-market-scout" } },
      { "id": "analyst", "type": "agent", "dependsOn": ["scout"], "agent": { "ref": "ex-market-analyst" } },
      { "id": "end", "type": "end", "dependsOn": ["analyst"] } ] },
  "agents": [ { "id": "ex-market-scout", "name": "Market Scout", "tools": ["webSearch","webfetch"],
                "systemPrompt": "..." } ] }
```

Der Node-Typ-Vertex ist damit derselbe wie in quad/kafka (`agent`, `decision`, `loop`, `fork`,
`join`, `hitl`, `subworkflow`, `end`). Der generische Runner führt **alle** Knotentypen aus:
`agent`, `decision` (Mini-Conditions, Branch-Pruning), `loop` (maxIterations + exitCondition),
`fork`/`join` (`all`/`any`/`n_of_m`), `hitl` (per-Node-Signal) und `end`; `subworkflow`-Knoten
werden inline expandiert. Authoring wird nach `agentflow.workflows.state-dir` persistiert (mit
Versions-Snapshots), einsehbar über `/agentflow/workflows/{id}/versions`.

### Guardrails, PII & Hooks pro Agent (deklarativ)

Agenten im v2-Dokument können Sicherheits-/Compliance-Features **deklarativ** setzen; der Runner
(`DeclarativeActivitiesImpl`) wertet sie beim Bau des quad-core-Agenten aus:

| Feld | Werte | Wirkung |
|---|---|---|
| `guardrailsInput` | `pii`, `sql-injection`, `prompt-injection` | Eingabe-Prüfung **vor** dem Modellaufruf |
| `guardrailsOutput` | dieselben Namen | Ausgabe-Prüfung **nach** dem Modellaufruf |
| `hooks` | `pii` / `pii:EMAIL,PHONE_NUMBER,…`, `audit` | PII-Maskierung (`[REDACTED]`), Audit-Trail |

```jsonc
{ "id": "ex-guarded", "name": "Guarded", "model": "…", "systemPrompt": "…",
  "guardrailsInput": ["prompt-injection", "sql-injection"],
  "guardrailsOutput": ["pii"],
  "hooks": ["pii:EMAIL,PHONE_NUMBER,CREDIT_CARD", "audit"] }
```

Hinweis: Die Guardrails werden von quad-core's `GuardrailPlugin` im Hook-Pipeline durchgesetzt
(`beforeModelCall`/`afterModelCall`); ein Block liefert `Request cancelled: …`. Die
PII-Maskierung (`PiiAnonymizerHook`) und der Audit-Trail (`AuditTrailHook`) sind reguläre
quad-core-Hooks. Verfügbare Namen liefert `GET /agentflow/quad/capabilities`
(`guardrails`, `hooks`, `piiMaskTypes`).

### Dynamische Workflows generieren (LLM)

Aus einer Aufgabenbeschreibung erzeugt ein LLM („Workflow-Architect“) einen **v2-Workflow**
(Agent-Schritte als DAG). Der Generator läuft über denselben quad-core-Pfad wie die Agenten
(`ModelFactory.createOpenAiFromEnv()`, also `OPENAI_*`/`QUAD_LLM_*` inkl. dynamischer Header) und
publiziert das Ergebnis durch denselben Compiler/Validator wie die Klassenpfad-Definitionen — das
Resultat ist damit garantiert lauffähig (genau ein `end`, azyklisch, auflösbare Referenzen).

```bash
# 1) Vorschau (nicht persistiert)
curl -X POST http://localhost:8095/agentflow/workflows/generate \
  -H 'Content-Type: application/json' \
  -d '{"task":"Prüfe die eingehende Support-Anfrage, recherchiere das Playbook und entwirf eine Antwort","name":"Support Generiert"}'

# 2) Generieren + speichern (Runtime-Override + Datei; erscheint in Workflows/Agents)
curl -X POST http://localhost:8095/agentflow/workflows/generate/save \
  -H 'Content-Type: application/json' -d '{"task":"..."}'

# 3) Generieren + durabel ausführen (Temporal; HITL via /agentflow/runs/declarative/<id>/approve)
curl -X POST http://localhost:8095/agentflow/workflows/generate/run \
  -H 'Content-Type: application/json' -d '{"task":"..."}'
```

Pro Schritt entsteht ein `agent`-Knoten (mit `dependsOn` aus den Abhängigkeiten) plus ein
eingebetteter `agents[]`-Eintrag; genau ein `end`-Knoten fasst die Terminals zusammen. Bei mehreren
Vorgängern werden deren Ergebnisse als Text im Schritt-Kontext („Previous results“) übergeben.
Tool-Namen des Planers werden auf die verfügbaren Built-ins (`readFile`, `writeFile`,
`listDirectory`, `grepSearch`, `findFiles`, `webSearch`, `webfetch`) beschränkt; das Token-Budget
ist über `agentflow.workflow.generator.max-output-tokens` steuerbar.

## Web UI (Angular)

Modul `agentflow-ui/` — Angular-20-SPA (Standalone Components, Signals) für den gesamten
AgentFlow-Betrieb. Design/Spezifikation: [`UI.md`](UI.md).

Screens: Dashboard · Runs (Liste/Detail/Signale) · Workflows (Editor v2-JSON, Validate/Save,
**+ Neuer Workflow**) · **Generate** (Aufgabe → Vorschau/Speichern/Ausführen) ·
Agents (Editor, **+ Neuer Agent** inkl. Guardrails/PII/Audit) · HITL-Queue ·
Observability (Metriken/Provenance/Capabilities) · Examples · Settings.

```bash
# 1) Backend (Temporal + App) wie oben starten
# 2) UI im Dev-Modus (Proxy → :8095)
cd agentflow-ui && npm install && npm start        # http://localhost:4200
# 3) Produktionsbuild (statisch, dist/agentflow-ui)
cd agentflow-ui && npm run build
# optional im Maven-Reaktor (lädt Node via frontend-maven-plugin):
cd .. && mvn -pl agentflow-ui -am -Pui package
```

Die UI nutzt ausschließlich bestehende/ergänzte REST-Endpoints; Secrets werden nie angezeigt
(`/agentflow/settings` liefert nur `hasApiKey`). Neu im Backend ergänzt: `GET /agentflow/runs`
(Visibility-Liste), `GET /agentflow/runs/{id}`, `POST /agentflow/runs/{id}/terminate`,
`GET /agentflow/runs/{id}/events` (History-Log), `POST /agentflow/workflows/validate`,
`PUT /agentflow/workflows/{id}`, `POST /agentflow/workflows` (neuen, leeren Workflow anlegen),
`GET /agentflow/workflows/{id}/versions`, `GET /agentflow/settings`,
`POST /agentflow/workflows/generate` / `.../generate/save` / `.../generate/run` (LLM-Generierung),
**SSE** `GET /agentflow/runs/stream` + `/agentflow/runs/{id}/stream` (Status **+ Events**),
`POST /agentflow/testbench/run`.

Screens zusätzlich: **Testbench** (Tokens/Tools/Kosten/Provenance), **Graph-Editor** (Dependencies
setzen/verbinden, Add/Rename/Delete) und **Versionen**; Run-Detail zeigt ein **Live-Log** (History-Events).
Opt-in Backend-Features: `agentflow.security.oidc.enabled` (JWT-Resource-Server, **deaktiv** per Default),
`agentflow.security.enabled` (API-Key-Gate), `agentflow.tenant.enabled` (`X-Tenant-Id`-Scoping) —
Client-Header setzbar unter Settings.

Der `decision`-Knoten ist über quad/kafka/agentflow hinweg dokumentiert (bewusst **unverändert**):
siehe [`DECISION_NODE_COMPARISON.md`](DECISION_NODE_COMPARISON.md).

## What is intentionally NOT here (yet)

- Skill/tool discovery via quad-core `ToolActivatorTool` (capability search/RAG is wired).
- `temporal-spring-ai` (tool calls as durable activities).
- Postgres checkpoint saver for multi-node deployments (SQLite is single-file).
- MCP servers in the deploy stack (the MCP *client* is wired; an example server is not shipped).
- OIDC is implemented but **disabled by default** (`agentflow.security.oidc.enabled=false`); the
  API-key gate and `X-Tenant-Id` scoping are independently opt-in.

See `docs/AGENT_STACK_EVOLUTION.md` (§7 Evolutionspfad, §10 Risiken) for the roadmap.
