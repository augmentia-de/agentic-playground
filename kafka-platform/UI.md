# UI.md – UI Architecture & Integration Analysis

## 0. Overview

| Layer | Quad-Quarkus (nooa/quad) | Kafka Workflow (kafka_workflow) | Compatible? |
|-------|-------------------------|--------------------------------|-------------|
| **Frontend** | React SPA (`quad-ui`) | ❌ Not available | ✅ Frontend portable |
| **Backend** | Quarkus REST API (`@Path("/api/ui")`) | Spring Boot 4 (`@RestController`) | ⚠️ Adapter needed |
| **Workflows** | Flat `{name, nodes[], edges[]}` with implicit dependencies | Nested `{dag_structure: {nodes[]}}` with explicit `dependsOn` | ⚠️ Bridge converter |
| **Agents** | `AgentDefinition` (rich: tools, skills, hooks, memory, grantedDirs) | `WorkerProperties.AgentConfig` (flat: modelKey, temperature, allowedTools) | ⚠️ Mapping table |
| **Messaging** | Kafka in-process + MessagingRouter | External via topics (`agent.tasks.*`, `agent.execution.results`) | ✅ Same event schema |
| **Monitoring** | `/api/ui/metrics`, `/runs`, OTel + Prometheus, Grafana | `/actuator/prometheus` (Spring) + WorkerMetrics | 🔄 Integration needed |

---

## 1. quad-ui Architecture

### 1.1 Project Structure

```
nooa/quad/quad-ui/
├── package.json          # Vite 8.x, React 19.2, TypeScript 6.0
├── vite.config.ts        # Proxy: /api → localhost:8086
├── src/main.tsx          # React Root
├── src/App.tsx           # Shell with tab navigation (conditional rendering)
├── src/services/
│   ├── api.ts            # Single API client – all calls go via "/api/ui/..."
│   └── types.ts          # All TypeScript interfaces
├── src/components/       # 30+ components (tab-driven)
│   ├── Dashboard.tsx
│   ├── Monitor.tsx       # Real-time monitoring (5s polling)
│   ├── Sessions.tsx      # Session management, GDPR export/delete
│   ├── Runs.tsx          # Execution history with step-level detail
│   ├── AgentManager.tsx  # Agent CRUD + playground test
│   ├── WorkflowComposer.tsx  # Drag-and-drop canvas (@xyflow/react)
│   ├── DynamicWorkflow.tsx # AI-generated workflows (Mermaid)
│   ├── HitlOverlay.tsx   # Global HITL approval popup (SSE + polling)
│   ├── SecurityCenter.tsx    # Guardrails + audit log
│   ├── ProvenanceView.tsx    # Token provenance tracing
│   ├── DebugView.tsx         # Tool call debugger
│   └── ...                   # MemoryManager, Automation, Skills, etc.
└── index.html           # 98.css Win98 retro styling via CDN
```

### 1.2 Navigation Model

**No React Router** is used. Instead, a state-driven tab system in `App.tsx`:

| Tab Value | Component | Function |
|----------|------------|----------|
| `'dashboard'` | `<Dashboard />` | Quickstart, backend status, task execution |
| `'agents'` | `<AgentManager />` | Agent CRUD, test playground |
| `'workflows'` | `<WorkflowComposer />` | Visual DAG creation, nodes/edges |
| `'dynamic'` | `<DynamicWorkflow />` | LLM-generated workflows |
| `'runs'` | `<Runs />` | Historical executions |
| `'messaging'` | `<Messaging />` | Channel status, topic-agent mapping |
| `'memory'` | `<MemoryManager />` | Session storage CRUD |
| `'security'` | `<SecurityCenter />` | Guardrails, HITL, audit log |
| `'sessions'` | `<Sessions />` | Session overview, details, GDPR |
| `'monitor'` | `<Monitor />` | Token usage, errors, latency |
| `'audit'` | `<Audit />` | Browse audit events |
| `'debug'` | `<DebugView />` | Journal/trace explorer |

### 1.3 API Client

```typescript
// services/api.ts
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/ui'

async request(method, path, body?) {
  const url = `${API_BASE_URL}${path}`
  return fetch(url, { method, headers: {...}, body })
}
```

All 79 endpoints in the system use this base prefix. When porting to Spring Boot, either:
- The Vite proxy must be adjusted (`vite.config.ts` → `localhost:8080`), OR
- The `API_BASE_URL` must be configured via environment variable

### 1.4 Typing

Key interfaces from `types.ts`:

```typescript
interface WorkflowRun { runId: string; status: 'running'|'completed'|'failed'; nodeResults: NodeRunResult[] }
interface NodeRunResult { nodeId: string; type: string; status: string; output: string }
interface TokenMetrics { prompt: number; completion: number; total: number }
interface StatusResponse { ready: boolean; model: string; costLimit: number }
interface SessionSummary { id: string; task: string; result: string }
```

→ These interfaces must be adapted to the Spring Boot response shape.

---

## 2. Format Analysis & Bridge Design

### 2.1 Workflow Definitions: Comparison

| Feature | Quad-Quarkus Format | Kafka-Orchestrator Format | Convertible? |
|---------|---------------------|---------------------------|---------------|
| **Envelope** | `{name, nodes[], edges[]}` | `{name, version, dag_structure: {nodes[]}}` | Yes → add nesting |
| **Dependencies** | Implicit via `edges[{source,target}]` | Explicit `dependsOn: [nodeA, nodeB]` | Yes → resolve edge array |
| **Node Types** | `agent`, `fork`, `join`, `conditional`, `loop`, `nested-workflow`, `async` | `STEP`, `FORK`, `JOIN`, `DECISION`, `LOOP`, `END` | Yes → string map |
| **Branch Routing** | Implicit on the `conditional` node (via edges) | Explicit `branches: [{branchId, condition, target}]` per DECISION | Yes → extract branch data |
| **Join Strategy** | Edge-level `joinPolicy: "any"/"all"` | `joinType: "AND"/"XOR"/"N_OF"` + `joinBranches` | Yes → `all→AND`, `any→N_OF(1)` |
| **Guard Conditions** | Separate `conditional` node with config | Inline `guardCondition` string on STEP nodes | Yes → extract and set inline |
| **HITL Support** | Separate service (`requiresHumanApproval`) | Explicit in the node `requiresHumanApproval: true` | Take over directly |
| **Loops** | Implicit entire downstream; optional `loopTargetId` | `bodyAgentType` + `exitCondition`; step refs | Yes → determine body |
| **Nested Workflows** | `nested-workflow` referencing ID | Not supported in examples | ❌ Feature gap |

### 2.2 Recommended Bridge Converter

```java
public class WorkflowDefinitionBridge {
    
    /** Transformiert quad-ui flat format → kafka-workflow DAG */
    public KafkaDagDef fromQuadUi(QuarkusWorkflow qwf) {
        Map<String, List<String>> deps = buildDependencies(qwf); // edges → dependsOn
        
        return new KafkaDagDef(
            name: qwf.name,
            version: extractVersion(qwf),
            nodes: qwf.nodes.stream().map(n -> convertNode(n, deps)).toList()
        );
    }
    
    private KafkaNode convertNode(Map<String,Object> node, Map<String,List<String>> deps) {
        return switch (resolveNodeType(node.type)) {
            case "agent", "async", "messaging-in", "messaging-out" 
                → new KafkaNode(stepId=node.id, nodeType="STEP", agentType=..., dependsOn=deps.get(node.id));
            case "conditional" 
                → new KafkaNode(stepId=node.id, nodeType="DECISION", 
                                 branches=extractBranches(node), defaultTarget=findDefault(node));
            case "fork" 
                → new KafkaNode(stepId=node.id, nodeType="FORK", 
                                 branches=extractBranchFromEdges(node));
            case "join" 
                → new KafkaNode(stepId=node.id, nodeType="JOIN",
                                 joinType=edgeLevelToJoinType(node), joinBranches=getSuccessors(node));
            case "loop" 
                → new KafkaNode(stepId=node.id, nodeType="LOOP",
                                 maxIterations=getLoopMax(node), exitCondition=getLoopExit(node));
            default → throw new IllegalArgumentException("Unsupported type: " + node.type);
        };
    }
}
```

**Recommendation:** Implement the bridge as a separate Spring bean served by its own controller endpoint:
```
POST /api/v1/workflow-definitions/from-quad   → nimmt Quad-Format entgegen, speichert als Kafka-DAG
```

### 2.3 Agent Definitions: Comparison

| Feature | Quad-Quarkus `AgentDefinition` | Kafka Worker `AgentConfig` | Convertible? |
|---------|-------------------------------|---------------------------|---------------|
| **ID/Name** | `id`, `name`, `category` | `agent-id` (optional) | ✅ Field mapping |
| **LLM Config** | `modelId`, `temperature`, `maxTokens` | `model-key`, `temperature`, `maxReActIterations` | ✅ Direct mapping |
| **Tools** | Rich config: tool names, parameters, file permissions | `allowedTools: Set<String>` | ⚠️ Tools as names only, without config |
| **Skills** | Skill registry integration, dynamic loading | ❌ Not available | ❌ Feature gap |
| **Hooks** | Named hook registration (pre/post agent/tool calls) | ❌ Not present | ❌ Feature gap |
| **Context/Prompt** | Full system prompt, templates | Optional `systemPrompt` in YAML | ✅ Adoption possible |
| **Memory** | Session memory with compression config | Simple memory (in AgentState) | ⚠️ Reduced |
| **Permissions** | Granted directories with read/write levels | No workspace config | ❌ Feature gap |
| **MCP/Sandbox** | MCP tool client, Docker sandbox support | ❌ Not present | ❌ Feature gap |

**Recommendation for the agent bridge:**
```java
public class AgentDefinitionBridge {
    
    public WorkerAgentConfig fromAgentDefinition(AgentDefinition qad) {
        return new WorkerAgentConfig(
            agentId: qad.id != null ? qad.id : qad.name,
            modelKey: resolveModelKey(qad.modelId),
            temperature: qad.temperature,
            maxReActIterations: qad.maxTokens != null ? qad.maxTokens / 4 : 8,
            allowedTools: extractToolNames(qad.toolDefinitions),
            structuredOutputEnabled: true  // Default für Worker
            // Hinweis: Skills, Hooks, Permissions gehen verloren!
        );
    }
}
```

**Strategy:** Both systems can coexist. The worker uses reduced configs, while the full Quad framework remains in the Quarkus backend.

---

## 3. Integration Options: quad-ui ↔ Kafka Orchestrator

### 3.1 Option A: Parallel Operation (recommended for the start)

```
┌─────────────────────────────────────────────┐
│              quad-ui (React SPA)            │
│                                             │
│  Tab "Workflows"  →  POST /api/ui/workflows │ ← Quad-Quarkus Backend
│  Tab "Monitor"    →  GET  /api/ui/metrics   │ ← Quad-Quarkus Metrics
│  Tab "Agents"     →  CRUD /api/ui/agents    │ ← Quad-Quarkus Agents
│  Tab "Runs"       →  GET  /workflow-runs    │ ← Quad-Quarkus Runs
│                                             │
│  Tab "Kafka Workflows"  →  POST /api/v1/workflow-definitions │ ← Kafka Orchestrator
│  Tab "Kafka Runs"       →  GET  /api/v1/workflows/{id}/status│ ← Kafka Orchestrator Status
│  Tab "Kafka Monitor"    →  GET  /actuator/prometheus        │ ← Kafka Metrics
│                                             │
│         Bridge Layer                        │
│  • Converts between Quad-Format ↔ Kafka-DAG │
│  • Unified token metrics (Prometheus)       │
│  • Aggregated runs history                  │
└─────────────────────────────────────────────┘
```

**Advantages:**
- Immediate commissioning possible
- No risk with the existing Quad-Quarkus stack
- Users can test both backends in parallel
- Gradual migration (new workflows via Kafka first)

**Disadvantages:**
- Two different workflow formats maintained
- Monitoring separate (Quad Metrics vs Spring Actuator)
- Double maintenance for tabs/views

### 3.2 Option B: Unified Backend with Plugin Architecture

A new "gateway" controller in the Kafka orchestrator that supports both Quad and Kafka APIs:

```java
@RestController
@RequestMapping("/api")
public class UnifiedApiGateway {
    
    @Autowired private QuadAdapterService quadAdapter;
    @Autowired private KafkaWorkflowService kafkaService;
    
    // Alias für bestehende quad-ui endpoints
    @PostMapping("/ui/workflows")
    public ResponseEntity<?> createWorkflow(@RequestBody Object payload) {
        if (isQuadFormat(payload)) {
            KafkaDagDef dag = quadAdapter.toKafkaDag(payload);
            return kafkaService.createDefinition(dag);
        } else {
            return kafkaService.createDefinition(payload);
        }
    }
    
    @GetMapping("/ui/metrics")
    public ResponseEntity<?> getMetrics() {
        return ResponseEntity.ok(MetricAggregator.merge(
            quadAdapter.fetchPrometheusMetrics(),
            kafkaService.fetchActuatorMetrics()
        ));
    }
}
```

**Advantages:**
- Single point of truth for frontend calls
- No bridge converters needed outside
- Unified monitoring dashboard

**Disadvantages:**
- Large codebase merger
- Long-term maintenance of both stack versions
- Technical debt from the abstraction layer

### 3.3 Option C: Complete Replacement (long-term)

The Kafka orchestrator completely replaces Quad-Quarkus as the backend.

**Steps:**
1. Adapt quad-ui to the Spring Boot APIs (base URL, endpoint paths)
2. Write Java bridges for workflow and agent definitions
3. Re-implement missing features (skills, hooks, MCP)
4. Deprecate Quad-Quarkus, then remove it

**Time horizon:** 3-6 months to production-ready.

---

## 4. Tab Role Distribution: Which Tab for Which Backend?

### Analysis of the Existing quad-ui Tabs

| Tab | Data Source | Kafka Equivalent | Recommendation |
|-----|-------------|------------------|------------|
| **Dashboard** | Backend Status, Quick Execute | Kafka Health Check, Task Publish | ✅ Keep, but extend |
| **Agents** | Quad Agent CRUD | Kafka Agent Registry (YAML Config) | ✅ Keep, but reduced view |
| **Workflows** | Quad DAG (nodes/edges) | Kafka DAG (dag_structure) | 🔀 **Two tabs or unified view** |
| **Dynamic** | AI-generated via DALL-E/Mermaid | ❌ Not available | ⏸️ Defer until AI Gateway exists |
| **Runs** | Quad Run History | Kafka Execution Log (DB + Kafka Topics) | ✅ Replace with Kafka views |
| **Messaging** | Internal Channels | Kafka Topic Status, Consumer Groups | ✅ Extend with Kafka-native info |
| **Memory** | Local session storage | Redis Persistence (spring-data-redis) | ✅ Keep, but reconnect |
| **Security** | Guardrails, HITL, Audit | Kafka HITL Service, Audit Logger | ✅ Keep, integrate Kafka audit |
| **Sessions** | Session DB | Redis Cache (Session IDs) | ✅ Keep, but show Kafka sessions |
| **Monitor** | Quad Metrics (token usage) | Prometheus Metrics (worker tasks) | ✅ Extend with Kafka metrics |
| **Audit** | Audit DB | Kafka Event Log + Spring Audit | ✅ Merge |
| **Debug** | Tool Call Journals | Kafka Record Inspection (Consumer Offsets) | ✅ Extend with Kafka debugging |

### Recommended Tab Structure

**Phase 1 (Parallel):** Duplicate tabs for the workflow focus:
- `Workflows (Quad)` — existing view for Quarkus workflows
- `Workflows (Kafka)` — new view for Kafka DAGs

**Phase 2 (Unified):** One tab with backend selection:
```tsx
// New: Backend Selector im WorkflowTab
<select value={backend} onChange={setBackend}>
  <option value="quarkus">Quarkus Backend</option>
  <option value="kafka">Kafka Orchestrator</option>
</select>

<WorkflowCanvas backend={backend} />  // Uses different API endpoint
```

**Phase 3 (Replace):** Completely remove the old Quad tab, Kafka as the only source.

---

## 5. Email as a Second Input Channel

### Design Proposal: Email Transformer Pattern

```
Email input (SMTP/Mailgun/SendGrid)
    │
    ├─► MailHandler (Spring Boot Component)
    │     - Receives incoming emails at tasks@quad.local
    │     - Extracts: subject → trigger key, body → initialTask
    │     - Attachments → artifact references
    │     
    │     ↓
    │     EventConverter: creates the same TaskEvent as REST
    │     {"workflowName": "...", "initialTask": "...", initiaData: "..."}
    │     
    │     ↓
    │     Publisher: publishes to "agent.execution.intake" (same topic!)
    │     Header: X-Source=EMAIL (vs X-Source=REST)
    │     
    │     ↓
    │     Orchestrator processes it like a normal REST API call
    │     
    │     ↓
    │     MailResponder (optional): sends the result email back
    │     - Completed → "Workflow finished: summary"
    │     - NeedsHITL → "Please approve: link to approval"
    │     - Error/DLQ   → "Error record: details"
```

### Implementation Steps

**Step 1: Email Listener Component**
```java
@Component
public class EmailInputTransformer {
    
    @EventListener
    public void handleInboundMail(IncomingMailEvent event) {
        // Parse email metadata
        String subject = event.subject(); // e.g. "[QUAD] Review PR #123"
        String body = event.body();
        String initiator = event.from();
        
        // Extract workflow trigger key from subject
        String workflowKey = extractTriggerKey(subject); // regex
        
        // Create execution request
        var request = new StartWorkflowRequest(
            workflowName: findWorkflow(workflowKey),
            inputParams: Map.of("emailBody", body, "initiator", initiator),
            idempotencyKey: generateKey(emailId)
        );
        
        // Publish to same intake topic
        eventPublisher.publishIntake(request);
    }
}
```

**Step 2: Response Handler**
```java
@Component
public class MailResponseHandler implements ApplicationListener<WorkflowExecutionCompleteEvent> {
    
    @Override
    public void onApplicationEvent(WorkflowExecutionCompleteEvent event) {
        if (event.hasInitiatorEmail()) {
            mailSender.send(MailMessage.builder()
                .to(event.initiatorEmail())
                .subject("Workflow Result: " + event.workflowName())
                .body(buildResultSummary(event))
                .build());
        }
    }
}
```

**Feature Phases:**
1. **Phase 1:** Input transformer only (email → Kafka), no response emails
2. **Phase 2:** Response emails for Completed/Error, simple text messages
3. **Phase 3:** Rich response (PDF attachments, HTML templates), HITL approval links

---

## 6. Observability Stack

### 6.1 Prometheus Metrics (already present)

Spring Boot Actuator + micrometer-registry-prometheus already integrated:
```yaml
management:
  endpoints:
    web.exposure.include: health,metrics,prometheus
```

Active metric beans:
- `tasks_received_total` (Counter)
- `tasks_completed_total` (Counter with tags)
- `task_duration_seconds` (Timer)
- `dlq_total` (Counter)
- `tool_errors_total` (Counter)

### 6.2 Grafana Dashboards (to be created)

Recommended panels:

| Panel Name | Query | Visualization |
|------------|-------|----------------|
| Tasks Received | `sum(increase(tasks_received_total[5m])) by (agentType)` | Bar Chart |
| P95 Task Duration | `histogram_quantile(0.95, rate(task_duration_seconds_bucket[5m]))` | Time series with P50/P99 lines |
| DLQ Events | `increase(dlq_total[1h])` | Line Graph |
| Kafka Lag | `lkm_consumer_lag_max{group="quad-worker"}` | Gauge |
| Error Breakdown | `rate(errors_total[5m]) by (errorClass)` | Pie Chart |

Dashboards versionable as JSON files in the repo:
```
grafana/dashboards/workers-overview.json
grafana/dashboards/llm-performance.json
grafana/dashboards/queue-health.json
```

### 6.3 OpenTelemetry + Jaeger (optionally enablable)

Quad-core brings OTel integration:
- `OTelAgentTracer` → span builder for tool calls
- `AgentTracing` → parent/child span hierarchy (agent → llm call → tool exec)
- `TracingHook` → event-to-span bridge

Activation in the worker:
```yaml
management:
  zipkin:
    base-url: http://jaeger:9411
  traces:
    sampling:
      probability: 0.1  # nur 10% sampled in Prod
```

Jaeger deployment in docker-compose:
```yaml
jaeger:
  image: jaegertracing/all-in-one:latest
  ports: ["16686:16686", "14268:14268"]
```

**Complete pipeline (optional):**
```
OTel Collector ←┬──── Prometheus Scrape (Metrics)
               ├──── HTTP gRPC (Logs/Traces)
               └──── Kafka Exporter
                          ↓
                    Jaeger/Zipkin (Traces)
                          ↓
                    Grafana (Dashboard Integration)
```

---

## 7. Overall Concept: Architecture Recommendation

### Short-Term (Month 1-2)
1. **Implement workflow bridge** — JSON format converter between Quad and Kafka
2. **Develop email transformer** — SMTP listener → Kafka intake topic
3. **Create Grafana dashboards** — Prometheus queries as JSON files
4. **Operate quad-ui in parallel** — with two workflow tabs (Quad vs Kafka)

### Medium-Term (Month 3-4)
5. **Unified API Gateway** — unite Quad and Kafka APIs under a unified prefix
6. **Deploy OTel Collector** — enable Jaeger for distributed tracing
7. **Advanced context prefix caching** — implement in the worker for repeated prompts
8. **GDPR compliance endpoints** — implement export/delete APIs for sessions

### Long-Term (Month 5-6)
9. **Full quad-ui port** — Alpine.js or Vue instead of React? Or simply keep React?
10. **Migration from the Quarkus backend** — establish the Kafka orchestrator as the primary backend
11. **Multi-Tenant Support** — tenant isolation via Redis keyspace
12. **Skill-Cache Layer** — Redis-based caching for repeated skill lookups
13. **Auto-scaling Worker** — based on Kafka partition offset delta

---

## 8. Risk Assessment

| Risk | Probability | Impact | Countermeasures |
|--------|-------------------|------------|---------------|
| **API incompatibility** | High | Medium | Implement bridge layer; tests against both backends |
| **Workflow-Type-Loss** | Medium | High | Document lost features; prioritize which ones get migrated |
| **Email-Processing-Failure** | High | Low | Fallback to REST API; retry queue for failed emails |
| **Metric-Consistency-Gap** | Medium | Medium | Define a unified metric naming convention |
| **Tech-Debt accumulation** | High | High | Clear deprecation schedule for Quarkus features |
| **Performance Bottleneck** | Medium | High | Perform load testing with real emails and parallel workflows |

---

## 9. Checklist for the Next Sprint

### ✅ Done
- [x] `quad-ui` architecture analyzed (30+ Components, 79 API Endpoints)
- [x] Format compatibility checked (workflow: bridge needed; agent: reduced mapping)
- [x] Integration options defined (Parallel → Unified → Replace)
- [x] Email transformer design created
- [x] Observability stack planned/extended

### 🔲 To Do (Prioritized)
- [ ] **P0:** Implement workflow bridge converter (`WorkflowDefinitionBridge.java`)
- [ ] **P1:** Create Grafana dashboard JSON files (`grafana/*.json`)
- [ ] **P1:** Email transformer as a Spring Component (`EmailInputTransformer.java`)
- [ ] **P2:** Unified API Gateway controller (`UnifiedApiGateway.java`)
- [ ] **P2:** Implement advanced context prefix caching in the worker
- [ ] **P3:** Configure OTel Collector deployment (`otel-collector-config.yaml`)
- [ ] **P3:** Implement GDPR export/delete API

### 💡 Ideas for Future
- AI-generative UI: "Create a workflow for my email" → Mermaid diagram → converted to Kafka DAG
- Multi-cloud deployment: worker instances on different clouds with a regional Kafka cluster
- Webhook integration alongside email: GitHub, Jira, Slack as alternative triggers

---

*As of: 2026-09-17*  
*Next step: sprint planning for P0–P2 items*
