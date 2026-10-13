# AgentFlow — Ideas

**Status:** Konzept (2026-10-10). Grundlage: `agentflow/` (`agentflow-core`, `agentflow-app`).
Stack: **Spring Boot 4.0.8 · Spring AI 2.0.1 · LangGraph4j 1.9.4 · Temporal 1.40.0 · quad-core**.
Ziel dieses Dokuments: Was die App heute kann, welche komplexen Use Cases darauf aufsetzen, und
wie eine UI für **Observe, Config, Logs, HITL und Task-Start** aussehen kann.

---

## 0. Was die App heute kann (real, nicht Wunsch)

| Fähigkeit | Umsetzung | Status |
|---|---|---|
| Graph-Reasoning (plan → execute → review, conditional retry) | `AgentFlowGraph` (`StateGraph`, `addConditionalEdges`) | ✅ |
| Durable Orchestration (Replay/Retry/Timers) | Temporal `AgentFlowWorkflow` + Activity | ✅ |
| quad-core-Agent als durable Activity | `QuadAgentWorkflow` + `AgentFlowActivities.runQuadAgent` (`Agents.simple()`) | ✅ |
| **Rich quad-Agent** (Tools/Skills/Guardrails/PII/Audit) | `QuadCapabilities` + `RichAgentWorkflow`, `/agentflow/runs/quad-rich` | ✅ |
| **Built-in Tools** (workspace-confined) | readFile, writeFile, listDirectory, grepSearch, findFiles, webSearch, webfetch | ✅ |
| **Skills (JIT)** | `AgentBuilder.withSkills` → `load_skill` / `skill_search` | ✅ |
| **Guardrails** | `GuardrailPlugin` via `withGuardrail` (Prompt-Injection) | ✅ |
| **PII-Anonymisierung** | `PiiAnonymizerHook` (EMAIL/PHONE/CREDIT_CARD → `[REDACTED]`) | ✅ |
| **Audit-Trail** | `AuditTrailHook` (in-memory) | ✅ |
| **RBAC (rollenbasierte Tools)** | `QuadSecurity` + quad `RoleBasedToolGuard` (`/runs/authorized`) | ✅ |
| **Saga/Kompensation (durabel)** | Temporal `SagaWorkflow` + quad `SagaAgentInterceptor` (`/runs/saga`, `/runs/quad-saga`) | ✅ |
| **Structured Output** | `withStructuredOutput(ContentPlan)` + `/runs/structured` | ✅ |
| **Provenance** | quad `ProvenanceTracker` + `/runs/provenance` | ✅ |
| **Long-term Memory** | quad `PersistentMemoryStore` (in-memory) + `/runs/memory` | ✅ |
| **MCP-Tools** | `SseMcpProxy`/`McpToolMethod` + `/runs/mcp` (SSE-URL konfigurierbar) | ✅ |
| **Sub-Agenten** | quad `SubAgentTool` via `withSubAgent` + `/runs/subagents` | ✅ |
| **OTel/Metriken** | quad `MetricsHook` (Micrometer) + `TracingHook` (OTel) + `/runs/metrics`, `/actuator/prometheus` | ✅ |
| **Capability-Search/RAG** | quad `CapabilitySearch`/`CapabilityIndex` (HNSW bzw. PgVector) + `/runs/capability-search`, `/capabilities/search` | ✅ |
| **Business-Cases** | Modul `agentflow-examples`: 5 Cases unter `/examples/**` | ✅ |
| **Declarative Workflows/Agents (v2)** | `quad.workflow` v2 JSON (`workflows/*.json`, inkl. `agents[]`) via `quad-workflow-core` (Publish-Compiler + Semantic-Validator) → `/agentflow/workflows`, `/runs/declarative` | ✅ |
| Human-in-the-Loop | `interruptBefore(REVIEWER)` + `HitlWorkflow` (`@SignalMethod decide`) | ✅ |
| Persistente Checkpoints (Worker-Neustart-fest) | `SQLiteSaverV2`, `graphId=agentflow` | ✅ |
| LLM (OpenAI-kompatibel) + konfigurierbare Header | `spring.ai.openai.*` (eine Quelle) + `LlmConfigBridge` + `LlmHeaderConfig` (Header nur wenn konfiguriert) | ✅ |
| REST-Start/Signal | `/runs*` (inkl. `structured`, `provenance`, `memory`, `mcp`, `subagents`, `metrics`, `capability-search`, `declarative`), `/capabilities`, `/capabilities/search`, `/workflows`, `/workflows/{id}`, `/info`, `/quad/capabilities`, `/security/roles`, `/examples**` | ✅ |
| Tests | Graph (3), Exceptions (15), Examples (8, inkl. durable Workflows via `TestWorkflowEnvironment`) | ✅ |
| **UI** | Modul `agentflow-ui` (Angular 20), siehe `UI.md` | ✅ |
| Run-Listing / HITL-Queue / Config-API | `GET /agentflow/runs(/{id})`, `/agentflow/settings`, Workflow-Editor (`/workflows/validate`, `PUT`) | ✅ |

**Ehrliche Grenzen heute:** keine UI, keine Run-Liste, kein Live-Stream, keine Config-CRUD,
State lebt in Activity-Logik (nicht als First-Class-Run-Objekt), HITL pausiert nur den einen Knoten.

---

## 1. Fähigkeitslandkarte (Zielbild)

```
Temporal (Durability)      LangGraph4j (Reasoning)        Spring AI (Model/Tools)
  Workflow / Signals         StateGraph / Reducers          ChatClient / MCP
  Timers / Replay            Interrupts / Checkpoints       Structured Output
        │                           │                              │
        └─────────────── AgentFlow Core (quad-core reuse) ────────┘
                    Agents · Tools · Skills · Guardrails
```

---

## 1.1 Tiefe quad-Nutzung — Katalog

**Bereits verdrahtet** (`QuadCapabilities` / `RichAgentWorkflow`, `/agentflow/runs/quad-rich`)

| Baustein | quad-core-Klasse | Wirkung |
|---|---|---|
| Built-in Tools | `ReadFile/WriteTool/LsTool/GrepTool/FindTool/WebSearchTool/WebFetchTool` | Datei-/Web-Tools, im Workspace confined |
| Tool-Registry | `QuadToolRegistry` + `ReflectiveToolMethod` | Tools unter Katalog-Namen registriert |
| Skills (JIT) | `AgentBuilder.withSkills` → `AgentSkillsPlugin`, `SkillLoadTool`, `SkillSearchTool`, `ToolActivatorTool`, `SkillActivationHook` | Katalog statt Volltext; `load_skill` on demand |
| Guardrails | `Guardrail`, `GuardrailPlugin` | blockt Prompt-Injection |
| PII/GDPR | `PiiAnonymizerHook` | maskiert E-Mail/Telefon/Kreditkarte vor dem Modell |
| Audit | `AuditTrailHook` (+ `AuditStore`) | Tool-/Agent-Aktionen revisionssicher |
| RBAC/Permissions | `RoleBasedToolGuard`, `SecurityContext` (via `QuadSecurity`) | rollenbasierte Tool-Scopes (deny by absence) |
| Saga/Kompensation | `SagaAgentInterceptor`, `CompensatingAction` (+ Temporal `SagaWorkflow`) | Ausgleich vorwärts/rückwärts, durabel |
| Structured Output | `StructuredOutputConfig`, `Agent.executeStructured` | typisierte Ausgaben |
| Memory | `PersistentMemoryStore`, `MemoryEntry`, `MemoryCategory` | Langzeitgedächtnis je Scope |
| Provenance | `ProvenanceTracker`, `ProvenanceEntry` | Herkunft/Quelle je Tool |
| MCP | `SseMcpProxy`, `McpToolMethod`, `McpToolClient` | externe MCP-Server als Tools |
| Sub-Agenten | `SubAgentTool`, `DynamicSubAgentTool` | Hierarchie/Handoff via registriertem Tool |
| Metriken/OTel | `MetricsHook`, `AgentMetrics`, `TracingHook`, `OTelAgentTracer` | Micrometer-Counter (Prometheus) + OTel-Spans |
| Capability-Search/RAG | `CapabilitySearch`, `CapabilityIndex`, `CapabilitySearchTool`, `CapabilitySearchRouter` | Tool-/Skill-Retrieval (HNSW bzw. PgVector) |
| Events/Logging | `AgentEventPublisher` + `LoggingHook` | Token-/Tool-/Lifecycle-Events |
| Workspace/Session | `WorkspaceResolver`, `CurrentSession` | Pfad-Confinenment, Session-Bindung |
| Modell | `ModelFactory` (+ `ModelHeaders`) | OpenAI-kompatibel inkl. OpenCode-Go-Header |

**Verfügbar, noch nicht verdrahtet** (nächste Ausbaustufe)

| Baustein | quad-core-Klasse | Idee |
|---|---|---|
| Skill-/Tool-Discovery | `ToolActivatorTool`, `CapabilityRegistry` | dynamische Tool-Aktivierung aus dem Retrieval |

> Prinzip: **Temporal** = Durability, **LangGraph4j** = Graph/HITL, **quad-core** = Agent-Fähigkeiten
> (Tools/Skills/Guardrails/PII/Memory/Saga). Jede Fähigkeit wird als Activity gekapselt.

---

## 2. Komplexe Use Cases

### UC-1 — „Deep Research & Editorial" (mehrstufig, Tools, HITL)
Der aus `quad` bekannte News-Workflow, aber durabel und mit Freigabe.
```
start → planner → researcher(tools: webSearch, webfetch) → analyst → writer
                         │                                   │
                         └────────── interrupt (HITL) ───────┘
                                        │ approve
                                     publish
```
- **Nutzt:** LangGraph4j-Knoten + Sub-Agenten, quad-core `ToolRegistry`/MCP, HITL-Interrupt,
  Temporal-Retries (Flaky Web-Fetches), persistente Checkpoints.
- **Warum temporal:** Dauer Minuten–Stunden, externe Quellen, Freigabe asynchron.

### UC-2 — Regulatorischer Freigabeprozess (Tage, Audit, Saga)
Ein Antrag durchläuft Prüfungen; bei Ablehnung werden Seiteneffekte kompensiert.
- **Nutzt:** Temporal `Workflow.await` mit Timern (SLA-Eskalation), HITL-Signale, quad-core
  `SagaAgent`/`CompensatingAction` in Activities, Audit-Events.
- **UI:** HITL-Inbox, Fristen, Audit-Trail.

### UC-3 — RAG-gestützte Fallprüfung (strukturierte Ausgabe)
Retrieval → Bewertung → Entscheidung mit typisiertem Ergebnis.
- **Nutzt:** Spring AI VectorStore/Embeddings, Structured Output (JSON-Schema), Reviewer-Node.
- **Graph:** `retrieve → assess → decide`; bei niedriger Konfidenz → zurück zu `retrieve` (Cycle).

### UC-4 — Parallele Content-Pipeline (Fan-out/Fan-in)
Ein Input → vier Kanäle (LinkedIn/X/Blog/Newsletter) parallel → Join → Packaging.
- **Nutzt:** LangGraph4j Parallel-Branch/Join, je Kanal eigener Subgraph.
- **Durability:** jeder Zweig ein Temporal-Activity; Join wartet deterministisch.

### UC-5 — Overnight-Batch / Scheduled (Neustart-fest)
Nächtliche Massenverarbeitung, Worker darf neu starten.
- **Nutzt:** Temporal Cron/`Workflow.await(Duration)`, persistente Checkpoints, Worker-Restart-Test.
- **UI:** Schedule-Ansicht, letzte/nächste Läufe, Erfolgsquote.

### UC-6 — Multi-Agent-Handoff mit Skills
Spezialisierte Agenten (Skills) übergeben Kontext; Supervisor routet.
- **Nutzt:** quad-core `SkillRegistry`/`load_skill`, `SubAgentTool`, LangGraph4j-Handoff,
  Spring AI Agent-Skills-Ideen (Todo/Task-Isolation).

### UC-7 — What-if / Replay / Time-Travel
Lauf anhalten, State inspizieren, mit geändertem Input fortsetzen (Vergleich).
- **Nutzt:** LangGraph4j `getStateHistory`/`stateOf` + `GraphInput.resume`, Temporal-Replay.

---

## 3. UI-Konzept

**Leitprinzipien**
1. **Durability sichtbar** — nicht „läuft grad", sondern „pausiert seit 2 h auf Signal X".
2. **Graph = Navigationsobjekt** — Knoten sind anklickbar, State/IO je Knoten sichtbar.
3. **Zwei Ebenen** — Orchestrierung (Temporal: Timeline, Retries, Signals) + Reasoning
   (LangGraph4j: State, Checkpoints, Interrupts).
4. **HITL ist eine Inbox**, kein Dialog.
5. **Alles erklärt sich aus Events** — Log/Event/LLM-IO pro Lauf konsistent.

### 3.1 Informationsarchitektur
```
┌ QUAD / AgentFlow Studio ─────────────────────────────────────────────┐
│  Overview   Runs   Start   HITL   Logs   Config   Models   Settings   │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Screen: Overview (Observe)
```
┌ Overview ────────────────────────────────────────────────────────────┐
│ Running 12   Waiting HITL 3   Failed 2   Completed 148   p95 41s      │
│ ┌ Durchsatz (24h) ───────────┐  ┌ HITL-Warteschlange ──────────────┐ │
│ │      ▂▄▆█▆▄▂               │  │ 2× UC-2 Freigabe (SLA 4h)        │ │
│ └────────────────────────────┘  │ 1× UC-1 Redaktion                │ │
│ ┌ Letzte Fehler ─────────────┐  └──────────────────────────────────┘ │
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
- **Daten:** Temporal Describe/History (Timeline, Signals), LangGraph4j `getStateHistory`
  (State/Checkpoint je Knoten), Activity-Outputs (Node-IO), quad-core `AgentEvent` (Token/Tool).
- **Live:** SSE/WebSocket pusht `RUN_STARTED/NODE_*/WAITING_HITL/APPROVED/FAILED`.

### 3.4 Screen: Start (Task-Start)
```
┌ Start a workflow ───────────────────────────────────────────────────┐
│ Workflow  [ UC-1 Editorial ▾ ]   Variante [ Standard ▾ ]            │
│ Input     ┌────────────────────────────────────────────┐            │
│           │ Nachrichtenlage KI heute zusammenfassen…   │            │
│           └────────────────────────────────────────────┘            │
│ Overrides Model [ deepseek-v4.1-flash ▾ ]  Temp [0.2]  MaxTok [4096]│
│           Tools [☑ webSearch ☑ webfetch ☐ writeFile]               │
│ HITL      [✔ vor dem Reviewer freigeben]  Assignee [redaktion]       │
│ Schedule  ( ) jetzt   ( ) Cron [ 0 7 * * * ]   ( ) Webhook           │
│                              [ Start ]                              │
└──────────────────────────────────────────────────────────────────────┘
```
- **Daten:** `POST /agentflow/runs` (bzw. `/runs/hitl`), Graph-/Agent-Auswahl aus Config,
  Overrides als Parameter. Session-Id (OpenCode Go) pro Lauf anzeigen.

### 3.5 Screen: HITL-Inbox
```
┌ HITL Inbox ─────────────────────────────────────────────────────────┐
│ ⏰ SLA        Lauf            Schritt   Assignee     Erstellt         │
│ ▸ 00:42  hitl-8f3a        reviewer  redaktion   14:02               │
│ ▸ 02:10  uc2-gesuch-77    approval  compliance 12:30   ⚠ SLA bald   │
├─────────────────────────────────────────────────────────────────────┤
│ Detail: Schritt reviewer — Vorschlag (Plan/Ergebnis), Guardrails,   │
│ Tool-Aufrufe, Kosten.  [ Approve ]  [ Reject+Reason ]  [ Reassign ]  │
└─────────────────────────────────────────────────────────────────────┘
```
- **Daten:** `POST /runs/hitl/{id}/decide?approved=…` (Temporal-Signal). Liste = Temporal-Visibility
  Query `ExecutionStatus=Running AND Waiting`.

### 3.6 Screen: Logs
```
┌ Logs ───────────────────────────────────────────────────────────────┐
│ Lauf [hitl-8f3a ▾] Level [INFO ▾] Node [alle ▾] Suche [          ]  │
│ 14:02:03 INFO  planner  LLM req 412 tok → resp 88 tok (0.9s)        │
│ 14:02:11 INFO  executor tool webSearch(query="KI News") → 8 hits    │
│ 14:02:11 WARN  executor context 9.8k > 8k → summarised              │
│ 14:02:12 INFO  reviewer WAITING_HITL (signal x-opencode-session=…)  │
└─────────────────────────────────────────────────────────────────────┘
```
- **Daten:** Strukturierte Logs je Run/Node; Korrelation über `runId`/`workflowId`/`activityId`.
  Zusätzlich quad-core `LoggingHook`/`AgentEvent` und Temporal-History.

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
- **Daten:** Config-CRUD (neue API nötig). Provider-Header (optional) hier sichtbar/steuerbar.
  Versionierung wie im `docs/WORKFLOW_FORMAT_PROPOSAL.md` (immutable, `contentHash`).

---

## 4. Datenfluss & API-Bedarf (Lücken)

| UI-Bedarf | Heute | Nötige API |
|---|---|---|
| Run-Liste | — | `GET /runs?status=&workflow=` (Temporal Visibility) |
| Run-Detail | — | `GET /runs/{id}` (Describe + History + State) |
| Live-Update | — | `GET /runs/{id}/events` (SSE/WS) |
| HITL-Queue | nur Signal-Endpoint | `GET /hitl` + `POST /hitl/{id}/decide` |
| Node-IO/LLM-IO | — | `GET /runs/{id}/nodes/{nodeId}` |
| Logs | — | `GET /runs/{id}/logs` |
| Config | `GET /info` | Graph/Agent/Provider CRUD + Validate |
| Start | ✅ `/runs*` | + Graph-Auswahl/Schedule/Overrides |

**Abbildung auf die Engines**
- **Temporal:** `WorkflowClient.newWorkflowStub(...).describe()`, `getResult`, `Query`,
  History via `WorkflowServiceStubs`, Visibility (`ListWorkflowExecutions`).
- **LangGraph4j:** `CompiledGraph.getStateHistory(config)`, `stateOf/ lastStateOf` → Checkpoint/State.
- **quad-core:** `AgentEventPublisher`/`AgentEvent` (Token, ToolCall, BeforeInvocation…) als
  Event-Quelle; `LoggingHook` für Logs.
- **Anbindung:** UI ↔ AgentFlow REST + SSE. Für Graphen gibt es zusätzlich **LangGraph4j Studio**
  (einbettbar) und **AG-UI** — als Inspect/Dev-Tool nutzbar statt Eigenbau.

---

## 5. Nicht-funktionale Aspekte

- **Security:** OIDC/Keycloak (wie quad), Rollen für HITL (`approver`, `admin`), Secrets nie im UI.
- **Multi-Tenancy:** Temporal **Namespaces** je Tenant; Checkpoint-DB/WorkflowIds getrennt.
- **GDPR/PII:** Checkpoints enthalten Prompts/Outputs → Maskierung + Retention; Audit-Trail.
- **Kosten:** Token/Cost je Node aggregieren; Budgets pro Workflow.
- **Observability:** OTel-Traces (Activity↔Node), Metriken (Micrometer) → Prometheus/Grafana.
- **Reliability:** idempotente Activities, Idempotency-Keys für externe Effekte, DLQ.

---

## 6. Roadmap (Phasen)

| Phase | Inhalt | Nutzen |
|---|---|---|
| P1 | Run-Liste + Detail (Describe/History), SSE-Events | Observe-Basis |
| P2 | HITL-Inbox + Approve/Reject (UI) | echte Freigaben |
| P3 | Logs/Node-IO + Kosten | Diagnose |
| P4 | Start-Formular (Graph-Auswahl, Overrides, Schedule) | Self-Service |
| P5 | Config-CRUD + Versionierung (v2-Format) | Governance |
| P6 | Replay/What-if (Checkpoints) | Debug/Experiment |
| P7 | Multi-Tenant/RBAC/Audit | Enterprise |

---

## 7. Offene Fragen

1. **Eigene UI oder LangGraph4j Studio/AG-UI einbetten?** (Studio spart Graph-Viz.)
2. **Temporal-UI vs. eigene Observe-Ansicht** — kombinieren oder ersetzen?
3. **quad-ui wiederverwenden** (Win98-React) oder eigenes modernes Frontend?
4. **Config-Wahrheit** — v2/OWS als kanonisches Format (siehe `docs/WORKFLOW_FORMAT_PROPOSAL.md`)?
5. **HITL-Routing** — Rollen/Assignees/SLA; Eskalation via Temporal-Timer?

---

*Bezug:* `docs/AGENT_STACK_EVOLUTION.md` (§7 Evolutionspfad, §10 Risiken) und
`docs/WORKFLOW_FORMAT_PROPOSAL.md` (Definition/Versionierung).
