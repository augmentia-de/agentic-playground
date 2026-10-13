# UI.md – Vollständige Systemarchitektur & UI-Blueprint

## 0. Ziel des Dokuments
Dieses Dokument dient als **Single Source of Truth** für das Ziel-System „Agentic Platform". Es fasst die Architektur aller Ebenen zusammen: Frontend (React), Legacy-Backend (Quarkus), modernes Event-Driven-Backend (Kafka) und die zentralen Brücken-Schichten. Jeder Entwickler sollte hierüber ein vollständiges Bild der Datenflüsse, Formate, APIs und Migrationsschritte erhalten.

---

## 1. Gesamtarchitektur: Hybrides Monorepo (`agentic-platform`)

Das System ist kein klassischer Monolith, sondern eine **hybride Workspace-Struktur**, die drei eigenständige technologische Welten unter einem Dach vereint:

| Schicht | Komponente | Technologie | Hauptaufgabe |
|---------|------------|-------------|--------------|
| **Frontend** | `quad-ui/` | React 19.2, Vite 8.x, TypeScript 6.0 | Visuelle Oberfläche, Agent-Test, Monitoring |
| **Legacy Backend** | `quad-quarkus/` | Quarkus, JAX-RS, SQLite | Stabile Kern-Agentenfunktionen, interne In-Process Ausführung |
| **Modern Backend** | `kafka-platform/` | Spring Boot 4, Kafka Streams, Redis | Hochskalierbare Event-verteilte DAG-Verarbeitung |
| **Brückenschicht** | `de.augmentia.agentplatform.bridge` | Spring-Boot-Komponenten (kein eigener Stack mehr) | API-Gateway, JSON-Konverter, E-Mail/Vault-Anbindung |

### Verzeichnisstruktur (Root Level)
```text
agentic-platform/                                  # 🌐 Root Workspace
├── .code-workspace                                # 🔧 VSCode Multi-root Definition
├── README.md                                      # 📖 Master Dokumentation
├── Makefile                                       # 🚀 Unified Commands (make compile-java, build-ui...)
│
├── quad-quarkus/                                  # 🟣 Quad-Framework (Existing nooa/quad)
│   ├── pom.xml                                    # 📦 Parent POM
│   ├── quad-core/                                 # 🧠 Java Core (AgentRuntime, OTel Hooks)
│   ├── quad-quarkus/                              # ⚡ Quarkus Backend (`@Path("/api/ui")`)
│   ├── quad-ui/                                   # 🎨 React SPA (Vite, TypeScript)
│   │   └── vite.config.ts                         # Proxy Konfiguration → localhost:8086
│   └── deploy/                                    # 🐳 Docker Infra (Prometheus/Grafana)
│
├── kafka-platform/                                # 🟢 Kafka-Plattform (Event-driven)
│   ├── agent-common/                              # 📦 Geteilte Modelle + Workflow-/Definition-Bridges
│   ├── agent-orchestrator/                        # 🔄 DAG Engine + Intake Consumer
│   │   └── …/agentplatform/bridge/                # 🔗 Bridge Layer (siehe unten)
│   ├── agent-worker/                              # ⚙️ Deterministischer Mock Worker
│   ├── quad-workflow-worker/                      # 🤖 Echte LLM Worker (Spring Boot 4)
│   ├── docker-compose.yml                         # 🏗 Kafka, Postgres, Redis Deployment
│   └── grafana/                                   # 📊 Grafana Dashboard JSON Dateien
```

### Brückenschicht — wo sie jetzt liegt

Die Brückenfunktionen sind **kein eigener Maven-Stack mehr**, sondern normale
Komponenten des Kafka-Platform-Reaktors unter dem Paket
`de.augmentia.agentplatform.bridge`. Alle sieben werden über
`scanBasePackages = "de.augmentia.agentplatform"` erfasst; abgesichert durch
`ComponentScanCoverageTest`.

| Klasse | Modul | Aufgabe |
|--------|-------|---------|
| `bridge.web.UnifiedApiGateway` | agent-orchestrator | 🔌 Einheitlicher Proxy auf die Quarkus- und Kafka-APIs (`/api/v1/unified`) |
| `bridge.email.EmailInputTransformer` | agent-orchestrator | 📥 Mail → Kafka (`app.email.transformer.enabled`) |
| `bridge.email.MailResponseHandler` | agent-orchestrator | 📤 Ausgehende Mail/HITL (`app.email.responder.enabled`) |
| `bridge.email.ImapMailListener` | agent-orchestrator | 📭 IMAP-Polling (`app.email.imap.enabled`) |
| `bridge.email.KafkaProducer` | agent-orchestrator | ✉️ Mailversand, unbedingt aktiv |
| `bridge.redis.EmailInitiatorStore` | agent-orchestrator | 🗄️ Idempotenz-Store für Intake, unbedingt aktiv |
| `bridge.vault.SecretResolver` | agent-orchestrator | 🔑 Vault-Auflösung (`app.vault.enabled`) |
| `bridge.WorkflowDefinitionBridge` | agent-common | 🔀 JSON-Mapping Flat `{nodes,edges}` ↔ Nested `{dag_structure}` |
| `bridge.AgentDefinitionBridge` | agent-common | 🔀 Agent-Definitionen über die Stack-Grenze |

> ⚠️ **Noch nicht vorhanden:** eine Metriken-Zusammenführung. Die frühere
> `metrics-aggregator`-Schicht existiert nicht; Monitoring läuft aktuell getrennt
> über Quarkus-PromQL und Spring Actuator.

---

## 2. Deep-Dive: `quad-ui` (React SPA)

### 2.1 Tech-Stack & Einstiegspunkt
- **Bundler:** Vite 8.x mit React-Plugin und TypeScript Unterstützung (ESM native).
- **Navigation:** Es wird **kein** React Router verwendet. Die Ansicht steuert sich rein über einen String-State (`activeTab`) in `src/App.tsx`.
- **Styling:** Basis ist das Retro-Styling `98.css` (Win98 Look) via CDN, lokal erweitert durch drei CSS Module.
- **State Management:** Reines `useState` / `useReducer` + Custom Context (`WorkflowProvider`). Kein Redux/Zustand.

### 2.2 Navigationsmatrix (Aktuelle Tabs)
| Tab-Wert | Komponente | Funktion | Polling/Refresh |
|----------|------------|----------|-----------------|
| `'dashboard'` | `<Dashboard />` | Quickstart, Backend-Status, MCP Status | Sofortig bei Mount |
| `'agents'` | `<AgentManager />` | Agent CRUD, Tool-Editor | Manuell |
| `'workflows'` | `<WorkflowComposer />` | Drag-and-Drop Canvas (@xyflow/react) | Manuell |
| `'dynamic'` | `<DynamicWorkflow />` | AI-generierte Workflows (Mermaid) | Asynchron (WebWorkers) |
| `'runs'` | `<Runs />` | Ausführungsverlauf mit Detailansicht | Alle 5 Sekunden |
| `'messaging'` | `<Messaging />` | Channel-Status, Topic-Agent Mapping | Alle 5 Sekunden |
| `'memory'` | `<MemoryManager />` | Langzeitgedächtnis CRUD | Manuell |
| `'security'` | `<SecurityCenter />` | Guardrails, HITL-Billigungen | SSE + 5 Sek Polling |
| `'sessions'` | `<Sessions />` | Session Histories + GDPR Export | Manuell |
| `'monitor'` | `<Monitor />` | Token-Verbrauch, Errors, Latenz | Alle 5 Sekunden |
| `'audit'` | `<Audit />` | Audit-Events Browsen | Manuell |
| `'debug'` | `<DebugView />` | Journal/Trace Explorer | Manuell |

### 2.3 API Client & Typisierung
Alle HTTP-Anfragen laufen über einen einzigen zentralen Client (`src/services/api.ts`). Dies erleichtert den späteren Umbau auf ein Gateway massiv.

```typescript
// Base URL kann per Environment Variable überschrieben werden
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/ui'

async function request(method, path, body?) {
  return fetch(`${API_BASE_URL}${path}`, { method, headers: {...}, body })
}
```

**Wichtigste TypeScript Interfaces aus `types.ts`:**
- `TokenMetrics`: `{ prompt: number; completion: number; total: number }`
- `ErrorMetrics`: `{ toolCalls: number; timeouts: number; guardrails: number }`
- `WorkflowRun`: `{ runId: string; status: 'running'|'completed'|'failed'; nodeResults: NodeRunResult[] }`
- `NodeRunResult`: `{ nodeId: string; type: string; output: string }`
- `SessionSummary`: `{ id: string; task: string; result: string }`

---

## 3. Backend Vergleiche & Bridging (Quarkus vs Kafka)

Die größte technische Hürde ist die Strukturunterschiede im Datenmodell zwischen dem alten Quarkus-Backend und dem neuen Kafka-Orchestrator.

### 3.1 Workflow-Definitionen: Der Unterschied
| Merkmal | Quad-Quarkus (`/api/ui/workflows`) | Kafka-Orchestrator (`POST /api/v1/workflow-definitions`) | Bridge Lösung |
|---------|-------------------------------------|--------------------------------------------------------|---------------|
| **Envelope** | Flat: `{name, nodes[], edges[]}` | Nested: `{name, version, dag_structure: {nodes[]}}` | Java Klasse `WorkflowFormatConverter.java` |
| **Abhängigkeiten** | Implizit durch Kanten `edges[{source,target}]` | Explizit `dependsOn: [...]` pro Node | Topologischer Sort & Adjazenzliste umwandeln |
| **Node Types** | `agent`, `fork`, `join`, `conditional`, `loop` | `STEP`, `FORK`, `JOIN`, `DECISION`, `LOOP` | Enum Mapping (`conditional` → `DECISION`) |
| **Branch Routing** | Auf `conditional` Node implizit (via edges) | Explizit `branches: [{branchId, condition, target}]` | Edge-Daten extrahieren und strukturieren |
| **Join Strategie** | Edge-level `joinPolicy: "any"/"all"` | Explicit `joinType: "AND"/"XOR"/"N_OF"` + `joinBranches` | `all` → `AND`, `any` → `N_OF(1)` |
| **HITL Support** | Separate Service Logik | Explizit im Node `requiresHumanApproval: true` | Direkt übernehmen (1:1 kompatibel) |
| **Loops** | Implicit gesamte Downstream-Region | `bodyAgentType` + `exitCondition`; explizite Step-Referenzen | Loop-Body durch BFS ermitteln |

### 3.2 Agenten-Definitionen: Rich vs Reduced
| Eigenschaft | Quad-Quarkus `AgentDefinition` | Kafka-Worker `WorkerProperties.AgentConfig` | Übertragung |
|-------------|-------------------------------|---------------------------------------------|-------------|
| **ID / Name** | `id`, `name`, `category` | `agent-id` (optional) | ✅ Feldmapping |
| **LLM Config** | `modelId`, `temperature`, `maxTokens` | `model-key`, `temp`, `maxReActIterations` | ✅ Direktes Mapping |
| **Tools** | Rich config: params, file permissions | `allowedTools: Set<String>` (Namen) | ⚠️ Reduziert |
| **Skills** | Skill registry integration | ❌ Nicht vorhanden | ❌ Feature-Lücke |
| **Hooks** | Named hook registration (pre/post calls) | ❌ Nicht existent | ❌ Feature-Lücke |
| **Context/Prompt** | Volles System-Prompt, Templates | Optionaler `systemPrompt` in YAML | ✅ Übernahme möglich |
| **Permissions** | Granted directories (read/write levels) | Keine Workspace-Konfiguration | ❌ Feature-Lücke |

**Bridge Ansatz:** Der Mapper in `bridge.WorkflowDefinitionBridge` (Modul `agent-common`) normalisiert die rich Objekte des Quarkus-Backends in die flachen YAML-Konfigurationen des Workers.

---

## 4. Integrationsstrategien: Wie verbinde ich die Systeme?

### 3 Phasen zur schrittweisen Vereinheitlichung:

#### Phase A: Parallelbetrieb (Sofort)
Der aktuelle Zustand. Beide Backends werden unabhängig betrieben.
- **Frontend:** Zwei getrennte Views oder separate Server Instanzen.
- **Datenfluss:** Nutzner startet einen Task manuell über den Quarkus-Client, ODER nutzt den Kafka-Orchestrator separat.
- **Ziel:** Nahtlose UX erreichen, indem beide Systeme dieselben Metriken teilen.

#### Phase B: Unified API Gateway ✅ umgesetzt
Der zentrale Controller ist `de.augmentia.agentplatform.bridge.web.UnifiedApiGateway`
(`@RequestMapping("/api/v1/unified")`). Er wählt das Zielbackend pro Request über den
Header `X-Backend` (Default `kafka`) und normalisiert das Flat-Format über
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
- **Vorteil:** Das Frontend muss sich nur noch gegen ein Interface entwickeln. Das Backend entscheidet intern, welcher Prozessor zuständig ist.

#### Phase C: Vollständiger Ersatz (Langfristig)
Migration aller Features (Skills, Hooks) in den Kafka-Stack. Das alte Quarkus-Projekt wird deprecated und abgeschaltet.

---

## 5. Input-Kanal-Erweiterung: Email Transformer

Künftig sollen nicht nur direkte REST-API Calls Workflows starten können, sondern auch eingehende E-Mails.

### Design Pattern: `EmailInputTransformer`
Dieses Pattern sorgt dafür, dass Emails exakt dasselbe interne Format nutzen wie REST-Aufrufe:

```text
E-Mail (SMTP/Mailgun/SendGrid Trigger)
    │
    ├─► MailHandler: Extrahiert Subject (Trigger-Key), Body (Prompt), Attachments
    │     
    │     ↓ EventConverter
    │     Erzeugt TaskEvent: {"workflowName":"...", "initialTask":"..."}
    │     
    │     ↓ Publisher
    │     Publishes auf Topic: "agent.execution.intake" 
    │     Header: X-Source=EMAIL (damit Kafka es tracken kann)
    │     
    │     ↓ Orchestrator
    │     Verarbeitet völlig identisch wie REST-Input
    │     
    │     ↓ MailResponseHandler (Optional)
    │     Sendet Ergebnis-Email zurück an Initiator
```
**Implementierung:** Als neuer Spring Component im Kafka-Platform Projekt. Startet mit `MailListener` und end-to-end Kafka Publishing.

---

## 6. Observability Stack & Monitoring

Damit der Monitor im Quad-UI tatsächlich sinnvoll arbeitet, müssen wir die verteilten Metriken vereinen.

### 6.1 Prometheus (Spring Boot Actuator)
Das neue Kafka-System bringt bereits `spring-boot-starter-web` mit eingebettetem Tomcat und `micrometer-registry-prometheus`.
Endpoint aktivierbar unter: `http://localhost:8092/actuator/prometheus`

**Aktive Metric-Beans (`WorkerMetrics.java`):**
- `tasks_received_total` (Counter)
- `tasks_completed_total` (Counter mit Tags für Agent-Type und Status)
- `task_duration_seconds` (Timer für P50/P95/Latenz)
- `dlq_total` (Counter für Dead-Letter-Queuesintritte)

### 6.2 Grafana Dashboards
Grafana konsumiert die Prometheusbuckets als Data Sources. Standard-Panels:
1. **Tasks Received:** Bar-Chart nach `agentType`.
2. **P95 Task Duration:** Time Series mit Linien bei P50 und P99.
3. **DLQ Rate:** Steigungslinie der Fehlschläge.
4. **Error Breakdown:** Piechart der Error-Klassen (`timeout`, `guardrail`, `network_error`).

### 6.3 OpenTelemetry + Jaeger (Optional zuschaltbar)
Da `quad-core` bereits Tracing-Fähigkeiten bietet (`OTelAgentTracer`, `AgentTracing` Span Builder), kann dieses vollständig übernommen werden:
- **Aktivierung:** Durch Setzen von `management.zipkin.base-url` in der `application.yml`.
- **Jaeger Container:** Bereits definiert in der infra-layer Infrastruktur (`deploy/02-observability.yml` innerhalb von `quad-quarkus`).
- **Span Hierarchy:** `agent.execute` > `llm.call` > `tool.execute`. Ermöglicht detaillierte Debug-Analyse einzelner Toolcalls im UI.

---

## 7. Tab-Rollenverteilung: Woher kommen die Daten?

| Tab | Datenquelle Alt (Quarkus) | Datenquelle Neu (Kafka) | Empfohlene Übergangslösung |
|-----|---------------------------|-------------------------|----------------------------|
| **Dashboard** | Backend Status, Quick Execute | Kafka Health Check, Task Publish | Erhalten, aber expandieren |
| **Agents** | Quad Agent CRUD | Kafka Agent Registry (YAML Config) | Erhalten, aber reduced view |
| **Workflows** | Quad DAG (`nodes[]`, `edges[]`) | Kafka DAG (`dag_structure.nodes[]`) | 🔀 ZWEI TABS (Quad vs Kafka) oder Unified View mit Select-Menu |
| **Dynamic** | AI-generated via DALL-E/Mermaid | ❌ Noch nicht verfügbar | ⏸️ Defer bis AI Gateway existiert |
| **Runs** | Quad Run History (SQLite) | Kafka Execution Log (DB + Topics) | ❌ Komplett ersetzen durch Kafka Views |
| **Messaging** | Internal Channels | Kafka Topic Status, Consumer Groups | ✅ Erweitern um Kafka-native Infos |
| **Memory** | Local session storage | Redis Persistence (spring-data-redis) | ✅ Erhalten, neu connecten |
| **Security** | Guardrails, HITL, Audit | Kafka HITL Service, Audit Logger | ✅ Zusammenführen, Kafka-audit integrieren |
| **Sessions** | Session DB | Redis Cache (Session IDs) | ✅ Erhalten, aber Kafka-sessions anzeigen |
| **Monitor** | Quad Metrics (token usage) | Prometheus Metrics (worker tasks) | ✅ Erweitern um Kafka-Metriken + Jaeger Trace-ID Link |
| **Audit** | Audit DB | Kafka Event Log + Spring Audit | ✅ Zusammenführen |
| **Debug** | Tool Call Journals | Kafka Record Inspection (Consumer Offsets) | ✅ Erweitern um Kafka-debugging |

---

## 8. Risikomatrix & Strategische Empfehlungen

| Risiko | Eintrittswahrscheinlichkeit | Auswirkung | Gegenmaßnahme |
|--------|-----------------------------|------------|---------------|
| **API-Inkompatibilität** | Sehr hoch | Mittel | Bridge-Schicht (`bridge.web.UnifiedApiGateway`, `bridge.WorkflowDefinitionBridge`) ✅ vorhanden; Tests gegen beide Backends |
| **Workflow Type Loss** | Mittel | Hoch | Lost features dokumentieren (Nested workflows missing); Priorisieren welche migriert werden |
| **Email-Processing-Failure** | Hoch | Niedrig | Fallback auf REST API; Retry-Queue für fehlgeschlagene Emails im Kafka Broker |
| **Metric-Consistency-Gap** | Mittel | Mittel | Unified metric naming convention definieren (`quad_worker_task_duration`) |
| **Tech-Debt accumulation** | Hoch | Hoch | Klare Deprecation-Timeline für alte Quarkus-Features制定 (Monat X abschalten) |
| **Performance Bottleneck** | Mittel | Hoch | Load Testing mit realen Emails und parallelen Workflows durchführen |

### Nächste Schritte (Next Steps):
1. ~~**Sofort:** Aufbau des `bridge-format-converter`s~~ ✅ erledigt als `bridge.WorkflowDefinitionBridge` (Modul `agent-common`).
2. ~~**Kurzfristig:** Email Listener (`EmailInputTransformer`)~~ ✅ erledigt, liegt unter `bridge.email` und ist per `app.email.transformer.enabled` aktivierbar.
3. ~~**Mittelfristig:** Unified API Gateway~~ ✅ erledigt als `bridge.web.UnifiedApiGateway`.
4. **Offen:** Metrik-Zusammenführung über beide Stacks — existiert bisher nicht (siehe Warnung in Abschnitt 1).
4. **Langfristig:** Rest der quad-ui Components komplett vom Quarkus Backend lösen und auf das Kafka-Event-Model umziehen.
