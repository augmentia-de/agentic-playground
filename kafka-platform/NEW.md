# UI.md – Complete System Architecture & UI Blueprint

## 0. Purpose of the Document
This document serves as the **single source of truth** for the target system "Agentic Platform". It summarizes the architecture of all layers: frontend (React), legacy backend (Quarkus), modern event-driven backend (Kafka) and the central bridge layers. Every developer should get a complete picture of the data flows, formats, APIs and migration steps from it.

---

## 1. Overall Architecture: Hybrid Monorepo (`agentic-platform`)

The system is not a classic monolith but a **hybrid workspace structure** that unites three independent technological worlds under one roof:

| Layer | Component | Technology | Main Task |
|---------|------------|-------------|--------------|
| **Frontend** | `quad-ui/` | React 19.2, Vite 8.x, TypeScript 6.0 | Visual interface, agent testing, monitoring |
| **Legacy Backend** | `quad-quarkus/` | Quarkus, JAX-RS, SQLite | Stable core agent functions, internal in-process execution |
| **Modern Backend** | `kafka-platform/` | Spring Boot 4, Kafka Streams, Redis | Highly scalable event-distributed DAG processing |
| **Bridge layer** | `de.augmentia.agentplatform.bridge` | Spring Boot components (no longer its own stack) | API gateway, JSON converter, email/Vault integration |

### Directory Structure (Root Level)
```text
agentic-platform/                                  # 🌐 Root Workspace
├── .code-workspace                                # 🔧 VSCode multi-root definition
├── README.md                                      # 📖 Master documentation
├── Makefile                                       # 🚀 Unified Commands (make compile-java, build-ui...)
│
├── quad-quarkus/                                  # 🟣 Quad framework (existing nooa/quad)
│   ├── pom.xml                                    # 📦 Parent POM
│   ├── quad-core/                                 # 🧠 Java Core (AgentRuntime, OTel Hooks)
│   ├── quad-quarkus/                              # ⚡ Quarkus Backend (`@Path("/api/ui")`)
│   ├── quad-ui/                                   # 🎨 React SPA (Vite, TypeScript)
│   │   └── vite.config.ts                         # Proxy configuration → localhost:8086
│   └── deploy/                                    # 🐳 Docker Infra (Prometheus/Grafana)
│
├── kafka-platform/                                # 🟢 Kafka platform (event-driven)
│   ├── agent-common/                              # 📦 Shared models + workflow/definition bridges
│   ├── agent-orchestrator/                        # 🔄 DAG Engine + Intake Consumer
│   │   └── …/agentplatform/bridge/                # 🔗 Bridge Layer (see below)
│   ├── agent-worker/                              # ⚙️ Deterministic Mock Worker
│   ├── quad-workflow-worker/                      # 🤖 Real LLM Worker (Spring Boot 4)
│   ├── docker-compose.yml                         # 🏗 Kafka, Postgres, Redis deployment
│   └── grafana/                                   # 📊 Grafana dashboard JSON files
```

### The Bridge Layer — Where It Now Lives

The bridge functions are **no longer their own Maven stack**, but normal
components of the Kafka platform reactor under the package
`de.augmentia.agentplatform.bridge`. All seven are picked up via
`scanBasePackages = "de.augmentia.agentplatform"`; safeguarded by
`ComponentScanCoverageTest`.

| Class | Module | Task |
|--------|-------|---------|
| `bridge.web.UnifiedApiGateway` | agent-orchestrator | 🔌 Unified proxy to the Quarkus and Kafka APIs (`/api/v1/unified`) |
| `bridge.email.EmailInputTransformer` | agent-orchestrator | 📥 Mail → Kafka (`app.email.transformer.enabled`) |
| `bridge.email.MailResponseHandler` | agent-orchestrator | 📤 Outgoing mail/HITL (`app.email.responder.enabled`) |
| `bridge.email.ImapMailListener` | agent-orchestrator | 📭 IMAP polling (`app.email.imap.enabled`) |
| `bridge.email.KafkaProducer` | agent-orchestrator | ✉️ Mail delivery, always active |
| `bridge.redis.EmailInitiatorStore` | agent-orchestrator | 🗄️ Idempotency store for intake, always active |
| `bridge.vault.SecretResolver` | agent-orchestrator | 🔑 Vault resolution (`app.vault.enabled`) |
| `bridge.WorkflowDefinitionBridge` | agent-common | 🔀 JSON mapping flat `{nodes,edges}` ↔ nested `{dag_structure}` |
| `bridge.AgentDefinitionBridge` | agent-common | 🔀 Agent definitions across the stack boundary |

> ⚠️ **Not yet available:** a metrics aggregation. The former
> `metrics-aggregator` layer does not exist; monitoring currently runs separately
> via Quarkus PromQL and Spring Actuator.

---

## 2. Deep-Dive: `quad-ui` (React SPA)

### 2.1 Tech Stack & Entry Point
- **Bundler:** Vite 8.x with React plugin and TypeScript support (ESM native).
- **Navigation:** **No** React Router is used. The view is controlled purely via a string state (`activeTab`) in `src/App.tsx`.
- **Styling:** The basis is the retro styling `98.css` (Win98 look) via CDN, locally extended by three CSS modules.
- **State management:** Pure `useState` / `useReducer` + custom context (`WorkflowProvider`). No Redux/Zustand.

### 2.2 Navigation Matrix (Current Tabs)
| Tab Value | Component | Function | Polling/Refresh |
|----------|------------|----------|-----------------|
| `'dashboard'` | `<Dashboard />` | Quickstart, backend status, MCP status | Immediately on mount |
| `'agents'` | `<AgentManager />` | Agent CRUD, tool editor | Manual |
| `'workflows'` | `<WorkflowComposer />` | Drag-and-drop canvas (@xyflow/react) | Manual |
| `'dynamic'` | `<DynamicWorkflow />` | AI-generated workflows (Mermaid) | Asynchronous (WebWorkers) |
| `'runs'` | `<Runs />` | Execution history with detail view | Every 5 seconds |
| `'messaging'` | `<Messaging />` | Channel status, topic-agent mapping | Every 5 seconds |
| `'memory'` | `<MemoryManager />` | Long-term memory CRUD | Manual |
| `'security'` | `<SecurityCenter />` | Guardrails, HITL approvals | SSE + 5 sec polling |
| `'sessions'` | `<Sessions />` | Session histories + GDPR export | Manual |
| `'monitor'` | `<Monitor />` | Token usage, errors, latency | Every 5 seconds |
| `'audit'` | `<Audit />` | Browse audit events | Manual |
| `'debug'` | `<DebugView />` | Journal/trace explorer | Manual |

### 2.3 API Client & Typing
All HTTP requests go through a single central client (`src/services/api.ts`). This greatly eases a later conversion to a gateway.

```typescript
// Base URL kann per Environment Variable überschrieben werden
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/ui'

async function request(method, path, body?) {
  return fetch(`${API_BASE_URL}${path}`, { method, headers: {...}, body })
}
```

**Most important TypeScript interfaces from `types.ts`:**
- `TokenMetrics`: `{ prompt: number; completion: number; total: number }`
- `ErrorMetrics`: `{ toolCalls: number; timeouts: number; guardrails: number }`
- `WorkflowRun`: `{ runId: string; status: 'running'|'completed'|'failed'; nodeResults: NodeRunResult[] }`
- `NodeRunResult`: `{ nodeId: string; type: string; output: string }`
- `SessionSummary`: `{ id: string; task: string; result: string }`

---

## 3. Backend Comparisons & Bridging (Quarkus vs Kafka)

The biggest technical hurdle is the structural differences in the data model between the old Quarkus backend and the new Kafka orchestrator.

### 3.1 Workflow Definitions: The Difference
| Feature | Quad-Quarkus (`/api/ui/workflows`) | Kafka-Orchestrator (`POST /api/v1/workflow-definitions`) | Bridge Solution |
|---------|-------------------------------------|--------------------------------------------------------|---------------|
| **Envelope** | Flat: `{name, nodes[], edges[]}` | Nested: `{name, version, dag_structure: {nodes[]}}` | Java class `WorkflowFormatConverter.java` |
| **Dependencies** | Implicit via edges `edges[{source,target}]` | Explicit `dependsOn: [...]` per node | Convert topological sort & adjacency list |
| **Node Types** | `agent`, `fork`, `join`, `conditional`, `loop` | `STEP`, `FORK`, `JOIN`, `DECISION`, `LOOP` | Enum mapping (`conditional` → `DECISION`) |
| **Branch Routing** | Implicit on the `conditional` node (via edges) | Explicit `branches: [{branchId, condition, target}]` | Extract and structure edge data |
| **Join Strategy** | Edge-level `joinPolicy: "any"/"all"` | Explicit `joinType: "AND"/"XOR"/"N_OF"` + `joinBranches` | `all` → `AND`, `any` → `N_OF(1)` |
| **HITL Support** | Separate service logic | Explicit in the node `requiresHumanApproval: true` | Take over directly (1:1 compatible) |
| **Loops** | Implicit whole downstream region | `bodyAgentType` + `exitCondition`; explicit step references | Determine loop body via BFS |

### 3.2 Agent Definitions: Rich vs Reduced
| Property | Quad-Quarkus `AgentDefinition` | Kafka Worker `WorkerProperties.AgentConfig` | Transfer |
|-------------|-------------------------------|---------------------------------------------|-------------|
| **ID / Name** | `id`, `name`, `category` | `agent-id` (optional) | ✅ Field mapping |
| **LLM Config** | `modelId`, `temperature`, `maxTokens` | `model-key`, `temp`, `maxReActIterations` | ✅ Direct mapping |
| **Tools** | Rich config: params, file permissions | `allowedTools: Set<String>` (names) | ⚠️ Reduced |
| **Skills** | Skill registry integration | ❌ Not available | ❌ Feature gap |
| **Hooks** | Named hook registration (pre/post calls) | ❌ Not present | ❌ Feature gap |
| **Context/Prompt** | Full system prompt, templates | Optional `systemPrompt` in YAML | ✅ Adoption possible |
| **Permissions** | Granted directories (read/write levels) | No workspace configuration | ❌ Feature gap |

**Bridge approach:** The mapper in `bridge.WorkflowDefinitionBridge` (module `agent-common`) normalizes the rich objects of the Quarkus backend into the flat YAML configurations of the worker.

---

## 4. Integration Strategies: How Do I Connect the Systems?

### 3 Phases for Step-by-Step Unification:

#### Phase A: Parallel Operation (Immediate)
The current state. Both backends are operated independently.
- **Frontend:** Two separate views or separate server instances.
- **Data flow:** The user starts a task manually via the Quarkus client, OR uses the Kafka orchestrator separately.
- **Goal:** Achieve a seamless UX by having both systems share the same metrics.

#### Phase B: Unified API Gateway ✅ implemented
The central controller is `de.augmentia.agentplatform.bridge.web.UnifiedApiGateway`
(`@RequestMapping("/api/v1/unified")`). It selects the target backend per request via the
`X-Backend` header (default `kafka`) and normalizes the flat format via
`bridge.WorkflowDefinitionBridge`:

```java
@Controller
@RequestMapping("/api/v1/unified")
public class UnifiedApiGateway {

    @Value("${quarkus.base-url:http://localhost:8086}") String quarkusBaseUrl;

    @PostMapping(value = "/workflows/from-quad", consumes = MediaType.APPLICATION_JSON_VALUE)
    public /* … */ Object fromQuad(
            @RequestBody tools.jackson.databind.JsonNode quadPayload,
            @RequestHeader(name = "X-Backend", defaultValue = "kafka") String backend) {
        // Nimmt das Flat-Format an, konvertiert zum Nested DAG und persistiert
        // im Kafka-Orchestrator — bzw. leuchtet an das Quarkus-Backend durch.
    }
}
```
- **Advantage:** The frontend only has to develop against one interface. The backend decides internally which processor is responsible.

#### Phase C: Complete Replacement (Long-Term)
Migration of all features (skills, hooks) into the Kafka stack. The old Quarkus project is deprecated and shut down.

---

## 5. Input Channel Extension: Email Transformer

In the future, not only direct REST API calls should be able to start workflows, but also incoming emails.

### Design Pattern: `EmailInputTransformer`
This pattern ensures that emails use exactly the same internal format as REST calls:

```text
Email input (SMTP/Mailgun/SendGrid trigger)
    │
    ├─► MailHandler: extracts subject (trigger key), body (prompt), attachments
    │     
    │     ↓ EventConverter
    │     Creates TaskEvent: {"workflowName":"...", "initialTask":"..."}
    │     
    │     ↓ Publisher
    │     Publishes to topic: "agent.execution.intake" 
    │     Header: X-Source=EMAIL (so Kafka can track it)
    │     
    │     ↓ Orchestrator
    │     Processes completely identically to REST input
    │     
    │     ↓ MailResponseHandler (Optional)
    │     Sends the result email back to the initiator
```
**Implementation:** As a new Spring component in the Kafka platform project. Starts with `MailListener` and end-to-end Kafka publishing.

---

## 6. Observability Stack & Monitoring

For the monitor in the Quad UI to actually work meaningfully, we must unify the distributed metrics.

### 6.1 Prometheus (Spring Boot Actuator)
The new Kafka system already brings `spring-boot-starter-web` with embedded Tomcat and `micrometer-registry-prometheus`.
Endpoint can be enabled at: `http://localhost:8092/actuator/prometheus`

**Active metric beans (`WorkerMetrics.java`):**
- `tasks_received_total` (Counter)
- `tasks_completed_total` (Counter with tags for agent type and status)
- `task_duration_seconds` (timer for P50/P95/latency)
- `dlq_total` (counter for dead-letter queue entries)

### 6.2 Grafana Dashboards
Grafana consumes the Prometheus buckets as data sources. Standard panels:
1. **Tasks Received:** Bar chart by `agentType`.
2. **P95 Task Duration:** Time series with lines at P50 and P99.
3. **DLQ Rate:** Slope line of failures.
4. **Error Breakdown:** Pie chart of error classes (`timeout`, `guardrail`, `network_error`).

### 6.3 OpenTelemetry + Jaeger (Optionally Enablable)
Since `quad-core` already offers tracing capabilities (`OTelAgentTracer`, `AgentTracing` span builder), this can be fully adopted:
- **Activation:** By setting `management.zipkin.base-url` in `application.yml`.
- **Jaeger container:** Already defined in the infra-layer infrastructure (`deploy/02-observability.yml` within `quad-quarkus`).
- **Span Hierarchy:** `agent.execute` > `llm.call` > `tool.execute`. Enables detailed debug analysis of individual tool calls in the UI.

---

## 7. Tab Role Distribution: Where Does the Data Come From?

| Tab | Data Source Old (Quarkus) | Data Source New (Kafka) | Recommended Transitional Solution |
|-----|---------------------------|-------------------------|----------------------------|
| **Dashboard** | Backend Status, Quick Execute | Kafka Health Check, Task Publish | Keep, but expand |
| **Agents** | Quad Agent CRUD | Kafka Agent Registry (YAML Config) | Keep, but reduced view |
| **Workflows** | Quad DAG (`nodes[]`, `edges[]`) | Kafka DAG (`dag_structure.nodes[]`) | 🔀 TWO TABS (Quad vs Kafka) or unified view with select menu |
| **Dynamic** | AI-generated via DALL-E/Mermaid | ❌ Not yet available | ⏸️ Defer until AI gateway exists |
| **Runs** | Quad Run History (SQLite) | Kafka Execution Log (DB + Topics) | ❌ Completely replace with Kafka views |
| **Messaging** | Internal Channels | Kafka Topic Status, Consumer Groups | ✅ Extend with Kafka-native info |
| **Memory** | Local session storage | Redis Persistence (spring-data-redis) | ✅ Keep, reconnect |
| **Security** | Guardrails, HITL, Audit | Kafka HITL Service, Audit Logger | ✅ Merge, integrate Kafka audit |
| **Sessions** | Session DB | Redis Cache (Session IDs) | ✅ Keep, but show Kafka sessions |
| **Monitor** | Quad Metrics (token usage) | Prometheus Metrics (worker tasks) | ✅ Extend with Kafka metrics + Jaeger trace ID link |
| **Audit** | Audit DB | Kafka Event Log + Spring Audit | ✅ Merge |
| **Debug** | Tool Call Journals | Kafka Record Inspection (Consumer Offsets) | ✅ Extend with Kafka debugging |

---

## 8. Risk Matrix & Strategic Recommendations

| Risk | Probability | Impact | Countermeasure |
|--------|-----------------------------|------------|---------------|
| **API incompatibility** | Very high | Medium | Bridge layer (`bridge.web.UnifiedApiGateway`, `bridge.WorkflowDefinitionBridge`) ✅ present; tests against both backends |
| **Workflow Type Loss** | Medium | High | Document lost features (nested workflows missing); prioritize which ones get migrated |
| **Email-Processing-Failure** | High | Low | Fallback to REST API; retry queue for failed emails in the Kafka broker |
| **Metric-Consistency-Gap** | Medium | Medium | Define a unified metric naming convention (`quad_worker_task_duration`) |
| **Tech-Debt accumulation** | High | High | Clear deprecation timeline for old Quarkus features (shut down in month X) |
| **Performance Bottleneck** | Medium | High | Perform load testing with real emails and parallel workflows |

### Next Steps:
1. ~~**Immediate:** Build the `bridge-format-converter`~~ ✅ done as `bridge.WorkflowDefinitionBridge` (module `agent-common`).
2. ~~**Short-term:** Email listener (`EmailInputTransformer`)~~ ✅ done, located under `bridge.email` and can be enabled via `app.email.transformer.enabled`.
3. ~~**Medium-term:** Unified API Gateway~~ ✅ done as `bridge.web.UnifiedApiGateway`.
4. **Open:** Metrics aggregation across both stacks — does not yet exist (see warning in section 1).
4. **Long-term:** Completely decouple the rest of the quad-ui components from the Quarkus backend and migrate them to the Kafka event model.
