# UI.md – UI-Architektur & Integrationsanalyse

## 0. Übersicht

| Ebene | Quad-Quarkus (nooa/quad) | Kafka-Workflow (kafka_workflow) | Kompatibel? |
|-------|-------------------------|--------------------------------|-------------|
| **Frontend** | React SPA (`quad-ui`) | ❌ Nicht vorhanden | ✅ Frontend portierbar |
| **Backend** | Quarkus REST API (`@Path("/api/ui")`) | Spring Boot 4 (`@RestController`) | ⚠️ Adapter nötig |
| **Workflows** | Flat `{name, nodes[], edges[]}` mit impliziten Abhängigkeiten | Nested `{dag_structure: {nodes[]}}` mit explizitem `dependsOn` | ⚠️ Bridge-Konverter |
| **Agenten** | `AgentDefinition` (rich: tools, skills, hooks, memory, grantedDirs) | `WorkerProperties.AgentConfig` (flach: modelKey, temperature, allowedTools) | ⚠️ Mapping-Tabelle |
| **Messaging** | Kafka in-process + MessagingRouter | External via topics (`agent.tasks.*`, `agent.execution.results`) | ✅ Gleiches Event-Schema |
| **Monitoring** | `/api/ui/metrics`, `/runs`, OTel + Prometheus, Grafana | `/actuator/prometheus` (Spring) + WorkerMetrics | 🔄 Integration nötig |

---

## 1. quad-ui Architektur

### 1.1 Projektstruktur

```
nooa/quad/quad-ui/
├── package.json          # Vite 8.x, React 19.2, TypeScript 6.0
├── vite.config.ts        # Proxy: /api → localhost:8086
├── src/main.tsx          # React Root
├── src/App.tsx           # Shell mit Tab-Navigation (conditional rendering)
├── src/services/
│   ├── api.ts            # Single API client – alle Calls gehen über "/api/ui/..."
│   └── types.ts          # Alle TypeScript Interfaces
├── src/components/       # 30+ Komponenten (Tab-getrieben)
│   ├── Dashboard.tsx
│   ├── Monitor.tsx       # Echtzeit-Monitoring (Polling 5s)
│   ├── Sessions.tsx      # Session-Management, GDPR Export/Delete
│   ├── Runs.tsx          # Ausführungsverlauf mit Step-Level-Detail
│   ├── AgentManager.tsx  # Agent CRUD + Playground-Test
│   ├── WorkflowComposer.tsx  # Drag-and-Drop Canvas (@xyflow/react)
│   ├── DynamicWorkflow.tsx # AI-generierte Workflows (Mermaid)
│   ├── HitlOverlay.tsx   # Global HITL-Billigungspopup (SSE + Polling)
│   ├── SecurityCenter.tsx    # Guardrails + Audit-Log
│   ├── ProvenanceView.tsx    # Token-Herkunftstracing
│   ├── DebugView.tsx         # Tool-Call-Debugger
│   └── ...                   # MemoryManager, Automation, Skills, etc.
└── index.html           # 98.css Win98 Retro-Styling via CDN
```

### 1.2 Navigationsmodell

Es wird **kein React Router** verwendet. Stattdessen ein State-driven Tab-System in `App.tsx`:

| Tab-Wert | Komponente | Funktion |
|----------|------------|----------|
| `'dashboard'` | `<Dashboard />` | Quickstart, Backend-Status, Task-Ausführung |
| `'agents'` | `<AgentManager />` | Agent CRUD, Test-Playground |
| `'workflows'` | `<WorkflowComposer />` | Visuelle DAG-Erstellung, Nodes/Edges |
| `'dynamic'` | `<DynamicWorkflow />` | LLM-generierte Workflows |
| `'runs'` | `<Runs />` | Historische Ausführungen |
| `'messaging'` | `<Messaging />` | Channel-Status, Topic-Agent-Mapping |
| `'memory'` | `<MemoryManager />` | Session-Speicherung CRUD |
| `'security'` | `<SecurityCenter />` | Guardrails, HITL, Audit-Log |
| `'sessions'` | `<Sessions />` | Session-Übersicht, Details, GDPR |
| `'monitor'` | `<Monitor />` | Token-Verbrauch, Errors, Latenz |
| `'audit'` | `<Audit />` | Audit-Events Browsen |
| `'debug'` | `<DebugView />` | Journal/Trace-Explorer |

### 1.3 API-Client

```typescript
// services/api.ts
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/ui'

async request(method, path, body?) {
  const url = `${API_BASE_URL}${path}`
  return fetch(url, { method, headers: {...}, body })
}
```

Alle 79 Endpunkte im System verwenden diesen Base-Prefix. Bei Portierung auf Spring Boot muss entweder:
- Der Vite-Proxy angepasst werden (`vite.config.ts` → `localhost:8080`), ODER
- Die `API_BASE_URL` per Environment-Variable konfiguriert werden

### 1.4 Typisierung

Key-Interfaces aus `types.ts`:

```typescript
interface WorkflowRun { runId: string; status: 'running'|'completed'|'failed'; nodeResults: NodeRunResult[] }
interface NodeRunResult { nodeId: string; type: string; status: string; output: string }
interface TokenMetrics { prompt: number; completion: number; total: number }
interface StatusResponse { ready: boolean; model: string; costLimit: number }
interface SessionSummary { id: string; task: string; result: string }
```

→ Diese Interfaces müssen an die Spring Boot Response-Shape angepasst werden.

---

## 2. Format-Analyse & Bridge-Design

### 2.1 Workflow-Definitionen: Vergleich

| Merkmal | Quad-Quarkus Format | Kafka-Orchestrator Format | Konvertierbar? |
|---------|---------------------|---------------------------|---------------|
| **Envelope** | `{name, nodes[], edges[]}` | `{name, version, dag_structure: {nodes[]}}` | Ja → Nesting hinzufügen |
| **Abhängigkeiten** | Implizit durch `edges[{source,target}]` | Explizit `dependsOn: [nodeA, nodeB]` | Ja → Edge-Array auflösen |
| **Node Types** | `agent`, `fork`, `join`, `conditional`, `loop`, `nested-workflow`, `async` | `STEP`, `FORK`, `JOIN`, `DECISION`, `LOOP`, `END` | Ja → String-Map |
| **Branch Routing** | Auf `conditional` Node implizit (via edges) | Explizit `branches: [{branchId, condition, target}]` pro DECISION | Ja → Branchdaten extrahieren |
| **Join Strategie** | Edge-level `joinPolicy: "any"/"all"` | `joinType: "AND"/"XOR"/"N_OF"` + `joinBranches` | Ja → `all→AND`, `any→N_OF(1)` |
| **Guard Conditions** | Separates `conditional` Node mit Config | Inline `guardCondition` String auf STEP Nodes | Ja → Extrahieren und inline setzen |
| **HITL Support** | Separate Service (`requiresHumanApproval`) | Explizit im Node `requiresHumanApproval: true` | Direkt übernehmen |
| **Loops** | Implicit entire downstream; optional `loopTargetId` | `bodyAgentType` + `exitCondition`; step refs | Ja → Body ermitteln |
| **Nested Workflows** | `nested-workflow` referencing ID | Nicht unterstützt in Beispielen | ❌ Feature-Lücke |

### 2.2 Empfohlene Bridge-Konverter

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

**Empfehlung:** Bridge als separaten Spring Bean implementieren, der von einem eigenen Controller endpoint bedient wird:
```
POST /api/v1/workflow-definitions/from-quad   → nimmt Quad-Format entgegen, speichert als Kafka-DAG
```

### 2.3 Agent-Definitionen: Vergleich

| Merkmal | Quad-Quarkus `AgentDefinition` | Kafka-Worker `AgentConfig` | Konvertierbar? |
|---------|-------------------------------|---------------------------|---------------|
| **ID/Name** | `id`, `name`, `category` | `agent-id` (optional) | ✅ Feldmapping |
| **LLM Config** | `modelId`, `temperature`, `maxTokens` | `model-key`, `temperature`, `maxReActIterations` | ✅ Direktes Mapping |
| **Tools** | Rich config: tool names, parameters, file permissions | `allowedTools: Set<String>` | ⚠️ Tools nur Namen ohne Config |
| **Skills** | Skill registry integration, dynamic loading | ❌ Nicht vorhanden | ❌ Feature-Lücke |
| **Hooks** | Named hook registration (pre/post agent/tool calls) | ❌ Nicht existent | ❌ Feature-Lücke |
| **Context/Prompt** | Full system prompt, templates | Optional `systemPrompt` in YAML | ✅ Übernahme möglich |
| **Memory** | Session memory with compression config | Simple memory (in AgentState) | ⚠️ Reduziert |
| **Permissions** | Granted directories with read/write levels | No workspace config | ❌ Feature-Lücke |
| **MCP/Sandbox** | MCP tool client, Docker sandbox support | ❌ Nicht existent | ❌ Feature-Lücke |

**Empfehlung für Agent-Bridge:**
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

**Strategie:** Beide Systeme können nebeneinander existieren. Im Worker werden reduzierte Configs genutzt, während das volle Quad-Framework im Quarkus-Backend bleibt.

---

## 3. Integrationsoptionen: quad-ui ↔ Kafka-Orchestrator

### 3.1 Option A: Parallelbetrieb (empfohlen für Start)

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

**Vorteile:**
- Sofortige Inbetriebnahme möglich
- Keine Risiko bei bestehendem Quad-Quarkus-Stack
- Nutzer können beide Backends parallel testen
- Graduelle Migration (zuerst neue Workflows über Kafka)

**Nachteile:**
- Zwei verschiedene Workflow-Formate verwaltet
- Monitoring getrennt (Quad Metrics vs Spring Actuator)
- Double Maintenance für Tabs/Views

### 3.2 Option B: Unified Backend mit Plugin-Architektur

Ein neuer "Gateway"-Controller im Kafka-Orchestrator, der sowohl Quad- als auch Kafka-APIs unterstützt:

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

**Vorteile:**
- Single Point of Truth für frontend calls
- Keine Bridge-Konverter außerhalb nötig
- Einheitliches Monitoring-Dashboard

**Nachteile:**
- Große Codebasis-Verschmelzung
- Langfristige Wartung beider Stack-Versionen
- Technische Schulden durch Abstraktions-Layer

### 3.3 Option C: Vollständiger Ersatz (langfristig)

Kafka-Orchestrator ersetzt vollständig Quad-Quarkus als Backend.

**Schritte:**
1. quad-ui gegen Spring Boot APIs anpassen (Base URL, Endpoint-Pfade)
2. Java Bridges schreiben für Workflow- und Agent-Definitionen
3. Missing Features nachimplementieren (Skills, Hooks, MCP)
4. Quad-Quarkus deprecated, dann entfernt

**Zeithorizont:** 3-6 Monate bis produktionsreif.

---

## 4. Tab-Rollenverteilung: Welcher Tab für welchen Backend?

### Analyse der bestehenden quad-ui Tabs

| Tab | Datenquelle | Kafka-Äquivalent | Empfehlung |
|-----|-------------|------------------|------------|
| **Dashboard** | Backend Status, Quick Execute | Kafka Health Check, Task Publish | ✅ Erhalten, aber erweitern |
| **Agents** | Quad Agent CRUD | Kafka Agent Registry (YAML Config) | ✅ Erhalten, aber reduced view |
| **Workflows** | Quad DAG (nodes/edges) | Kafka DAG (dag_structure) | 🔀 **Zwei Tabs oder unified view** |
| **Dynamic** | AI-generated via DALL-E/Mermaid | ❌ Nicht verfügbar | ⏸️ Defer until AI Gateway exists |
| **Runs** | Quad Run History | Kafka Execution Log (DB + Kafka Topics) | ✅ Ersetzen durch Kafka Views |
| **Messaging** | Internal Channels | Kafka Topic Status, Consumer Groups | ✅ Erweitern um Kafka-native Infos |
| **Memory** | Local session storage | Redis Persistence (spring-data-redis) | ✅ Erhalten, aber neu connecten |
| **Security** | Guardrails, HITL, Audit | Kafka HITL Service, Audit Logger | ✅ Erhalten, Kafka-audit integrieren |
| **Sessions** | Session DB | Redis Cache (Session IDs) | ✅ Erhalten, aber Kafka-sessions anzeigen |
| **Monitor** | Quad Metrics (token usage) | Prometheus Metrics (worker tasks) | ✅ Erweitern um Kafka-Metriken |
| **Audit** | Audit DB | Kafka Event Log + Spring Audit | ✅ Zusammenführen |
| **Debug** | Tool Call Journals | Kafka Record Inspection (Consumer Offsets) | ✅ Erweitern um Kafka-debugging |

### Empfohlene Tab-Struktur

**Phase 1 (Parallel):** Doppelte Tabs für Workflow-fokus:
- `Workflows (Quad)` — bestehende View für Quarkus-Workflows
- `Workflows (Kafka)` — neue View für Kafka-DAGs

**Phase 2 (Unified):** Ein Tab mit Backend-Auswahl:
```tsx
// New: Backend Selector im WorkflowTab
<select value={backend} onChange={setBackend}>
  <option value="quarkus">Quarkus Backend</option>
  <option value="kafka">Kafka Orchestrator</option>
</select>

<WorkflowCanvas backend={backend} />  // Uses different API endpoint
```

**Phase 3 (Replace):** Alte Quad-Tab komplett entfernen, Kafka als einzige Quelle.

---

## 5. Email als zweiter Input-Kanal

### Design-Vorschlag: Email Transformer Pattern

```
Email-Input (SMTP/Mailgun/SendGrid)
    │
    ├─► MailHandler (Spring Boot Component)
    │     - Empfängt eingehende Emails an tasks@quad.local
    │     - Extrahiert: Subject → Trigger-Key, Body → initialTask
    │     - Attachments → artifact references
    │     
    │     ↓
    │     EventConverter: erzeugt identisches TaskEvent wie REST
    │     {"workflowName": "...", "initialTask": "...", initiaData: "..."}
    │     
    │     ↓
    │     Publisher: publiziert auf "agent.execution.intake" (gleicher Topic!)
    │     Header: X-Source=EMAIL (vs X-Source=REST)
    │     
    │     ↓
    │     Orchestrator verarbeitet wie normaler Rest-API-Aufruf
    │     
    │     ↓
    │     MailResponder (optional): sendet Ergebnis-Email zurück
    │     - Completed → "Workflow finished: summary"
    │     - NeedsHITL → "Bitte billigen: link zu approval"
    │     - Error/DLQ   → "Fehleraufzeichnung: details"
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

**Feature-Phasen:**
1. **Phase 1:** Nur Input-Transformer (Email → Kafka), keine Response-Emails
2. **Phase 2:** Response-Emails für Completed/Error, simple Textnachrichten
3. **Phase 3:** Rich Response (PDF Attachments, HTML Templates), HITL Approval Links

---

## 6. Observability Stack

### 6.1 Prometheus Metrics (bereits vorhanden)

Spring Boot Actuator + micrometer-registry-prometheus bereits integriert:
```yaml
management:
  endpoints:
    web.exposure.include: health,metrics,prometheus
```

Aktive Metric-Beans:
- `tasks_received_total` (Counter)
- `tasks_completed_total` (Counter mit Tags)
- `task_duration_seconds` (Timer)
- `dlq_total` (Counter)
- `tool_errors_total` (Counter)

### 6.2 Grafana Dashboards (zu erstellen)

Empfohlene Panels:

| Panel Name | Query | Visualisierung |
|------------|-------|----------------|
| Tasks Received | `sum(increase(tasks_received_total[5m])) by (agentType)` | Bar Chart |
| P95 Task Duration | `histogram_quantile(0.95, rate(task_duration_seconds_bucket[5m]))` | Time Series mit P50/P99 Linien |
| DLQ Events | `increase(dlq_total[1h])` | Line Graph |
| Kafka Lag | `lkm_consumer_lag_max{group="quad-worker"}` | Gauge |
| Error Breakdown | `rate(errors_total[5m]) by (errorClass)` | Pie Chart |

Dashboards als JSON-Dateien versionierbar im Repo:
```
grafana/dashboards/workers-overview.json
grafana/dashboards/llm-performance.json
grafana/dashboards/queue-health.json
```

### 6.3 OpenTelemetry + Jaeger (optional zuschaltbar)

Quad-core bringt OTel-Integration mit:
- `OTelAgentTracer` → Span builder für Tool Calls
- `AgentTracing` → parent/child span hierarchy (agent → llm call → tool exec)
- `TracingHook` → event-to-span bridge

Aktivierung im Worker:
```yaml
management:
  zipkin:
    base-url: http://jaeger:9411
  traces:
    sampling:
      probability: 0.1  # nur 10% sampled in Prod
```

Jaeger-Deployment in docker-compose:
```yaml
jaeger:
  image: jaegertracing/all-in-one:latest
  ports: ["16686:16686", "14268:14268"]
```

**Komplette Pipeline (optional):**
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

## 7. Gesamtkonsept: Architekturempfehlung

### Kurzfristig (Monat 1-2)
1. **Workflow-Bridge implementieren** — JSON-Format-Konverter zwischen Quad und Kafka
2. **Email-Transformer entwickeln** — SMTP Listener → Kafka Intake Topic
3. **Grafana Dashboards erstellen** — Prometheus Queries als JSON files
4. **quad-ui parallel betreiben** — Mit zwei Workflow-Tabs (Quad vs Kafka)

### Mittelfristig (Monat 3-4)
5. **Unified API Gateway** — Quad und Kafka APIs unter统一em Prefix vereinen
6. **OTel Collector deployen** — Jaeger für Distributed Tracing aktivieren
7. **Advanced Context Prefix caching** — Implementieren im Worker für repeated Prompts
8. **GDPR Compliance endpoints** — Export/Delete APIs für Sessions implementieren

### Langfristig (Monat 5-6)
9. **Full quad-ui Port** — Alpine.js oder Vue statt React? Oder einfach React beibehalten?
10. **Migration vom Quarkus-Backend** — Kafka-Orchestrator als primäres Backend etablieren
11. **Multi-Tenant Support** — Tenant-Isolation über Redis Keyspace
12. **Skill-Cache Layer** — Redis-basiertes Caching für wiederholte Skill-Lookups
13. **Auto-scaling Worker** — Basierend auf Kafka-Partition-Offset-Delta

---

## 8. Risikobewertung

| Risiko | Wahrscheinlichkeit | Auswirkung | Gegenmaßnahmen |
|--------|-------------------|------------|---------------|
| **API-Inkompatibilität** | Hoch | Medium | Bridge-Schicht implementieren; Tests gegen beide Backends |
| **Workflow-Type-Loss** | Mittel | Hoch | Lost features dokumentieren; Priorisieren welche migriert werden |
| **Email-Processing-Failure** | Hoch | Niedrig | Fallback auf REST API; Retry-Queue für fehlgeschlagene Emails |
| **Metric-Consistency-Gap** | Mittel | Mittel | Unified metric naming convention definieren |
| **Tech-Debt accumulation** | Hoch | Hoch | Klare Deprecation-Zeitplan für Quarkus-Features制定 |
| **Performance Bottleneck** | Mittel | Hoch | Load Testing mit realen Emails und parallelen Workflows durchführen |

---

## 9. Checklist für nächsten Sprint

### ✅ Done
- [x] `quad-ui` Architektur analysiert (30+ Components, 79 API Endpoints)
- [x] Format-Kompatibilität geprüft (Workflow: bridge nötig; Agent: reduced mapping)
- [x] Integrationsoptionen definiert (Parallel → Unified → Replace)
- [x] Email-Transformer Design erstellt
- [x] Observability-Stack geplante erweitert

### 🔲 To Do (Priorisiert)
- [ ] **P0:** Workflow-Bridge-Konverter implementieren (`WorkflowDefinitionBridge.java`)
- [ ] **P1:** Grafana Dashboard JSON Files erstellen (`grafana/*.json`)
- [ ] **P1:** Email-Transformer als Spring Component (`EmailInputTransformer.java`)
- [ ] **P2:** Unified API Gateway Controller (`UnifiedApiGateway.java`)
- [ ] **P2:** Advanced Context Prefix caching im Worker implementieren
- [ ] **P3:** OTel Collector Deployment konfigurieren (`otel-collector-config.yaml`)
- [ ] **P3:** GDPR Export/Delete API implementieren

### 💡 Ideas for Future
- AI-generative UI: "Erstelle einen Workflow für meine E-Mail" → Mermaid Diagramm → Konvertiert zu Kafka-DAG
- Multi-cloud deployment: Worker Instanzen auf verschiedenen Clouds mit regionalem Kafka Cluster
- Webhook-Integration neben Email: GitHub, Jira, Slack als alternative Trigger

---

*Stand: 2026-09-17*  
*Nächster Schritt: Sprint Planning für P0–P2 Items*
