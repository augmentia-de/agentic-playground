# Umsetzungsplan: Bridge Layer, Observability & Email Input für kafka-platform

**Erstellt:** 2026-09-17  
**Letzte Änderung:** 2026-09-18  
**Status:** Phase 1–4 Implementiert + Frontend Integration  
**Quellen:** NEW.md §3–§5, UI.md §2–§5  

---

## 1. Übersicht

Dieses Dokument dokumentiert die Umsetzung der Integrationsbrücke zwischen Quarkus-Backend und Kafka-Orchestrator sowie aller ergänzenden Komponenten aus NEW.md und UI.md.

### 1.1 Umgesetzte Module

| Phase | Modul | Package | Datei(en) | Status |
|-------|-------|---------|-----------|--------|
| P0 | Workflow Bridge | `com.agentplatform.bridge` | `WorkflowDefinitionBridge.java`, `QuarkusWorkflow.java`, `QuarkusNode.java`, `QuarkusEdge.java` | ✅ |
| P0 | Agent Bridge | `com.agentplatform.bridge` | `AgentDefinitionBridge.java` (inkl. WorkerAgentConfig, QuarkusAgent) | ✅ |
| P1 | Prometheus Metrics | `com.agentplatform.orchestrator.metrics` | `OrchestratorMetrics.java` | ✅ |
| P1 | Grafana Dashboards | `grafana/dashboards/` | `agent-orchestrator-overview.json`, `worker-performance.json`, `queue-health.json` | ✅ |
| P2 | Unified API Gateway | `com.agentplatform.bridge.web` | `UnifiedApiGateway.java` | ✅ |
| P3 | Email Transformer | `com.agentplatform.bridge.email` | `EmailInputTransformer.java`, `KafkaProducer.java`, `MailResponseHandler.java` | ✅ |
| P3 | Control Events | `com.agentplatform.common.event` | `ControlEvents.java` (ergänzt um `EmailIntakeEvent`) | ✅ |
| P4 | Redis Initiator Store | `com.agentplatform.bridge.redis` | `EmailInitiatorStore.java` | ✅ |
| P4 | GDPR API | `com.agentplatform.orchestrator.web` | `GdprController.java` | ✅ |
| P4 | Prefix Cache | `com.agentplatform.worker.cache` | `PrefixCache.java` | ✅ |
| P4 | Intake Listener | `com.agentplatform.orchestrator.kafka` | `IntakeListener.java` | ✅ |
| P4 | OTel/Jaeger Config | `docker/` | `compose-extensions.yml`, `otel-config.yaml` | ✅ |
| P4 | Frontend — Kafka API | `quad-quarkus/quad-ui/src/services/` | `kafkaApi.ts` | ✅ |
| P4 | Frontend — Backend Selector | `quad-quarkus/quad-ui/src/components/` | `WorkflowComposer.tsx` (modified) | ✅ |

### 1.2 Abhängigkeiten

Neue Dependencies in `agent-orchestrator/pom.xml`:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webflux</artifactId>
</dependency>
```

Bestehende Dependencies genutzt:
- `spring-boot-starter-actuator` (bereits vorhanden für Prometheus)
- `spring-boot-starter-mail` muss manuell added werden (für Email-Funktionalität)

---

## 2. Detail-Dokumentation

### 2.1 Workflow Definition Bridge (`WorkflowDefinitionBridge.java`)

**Zweck:** Konvertiert quad-ui flat workflow format → Kafka DAG format

#### Mapping-Tabelle

| Quad Typ | Kafka NodeType | Mapping |
|----------|----------------|---------|
| `agent` | `STEP` | Direkt |
| `conditional` | `DECISION` | Kanten → Branches extrahieren |
| `fork` | `FORK` | Kanten → Branch.firstStep |
| `join` | `JOIN` | joinPolicy → JoinType |
| `loop` | `LOOP` | BFS Loop-Body-Ermittlung |
| `nested-workflow` | ❌ Feature-Lücke | Ausgabe als Warnung |

#### Algorithmus

```
convert(QuarkusWorkflow qwf):
    1. Adjazenzliste bauen aus edges[]
    2. Reverse-Adjazenz = dependsOn-Karte
    3. Pro Node:
       - Knoten-Typ ermitteln (switch auf type.toLowerCase())
       - Spezifischen Converter aufrufen
    4. Liste von DagNode zurückgeben
```

#### Attribute Mapping

| Quad Property | Kafka Feld | Transform |
|---------------|------------|-----------|
| `id` | `stepId` | Identisch |
| `type` | `nodeType` | Enum-Mapping |
| `agentType` | `agentType` | Identisch / Default "generic" |
| `properties.joinPolicy` | `joinType` | `"all"`→`"AND"`, `"any"`→`"XOR"` |
| `timeoutMs` / `timeoutSeconds` | `timeoutMs` | Sekunden × 1000 |
| `requiresHumanApproval` | `requiresHumanApproval` | Identisch |
| `properties.guardCondition` | `guardCondition` | String |

### 2.2 Agent Definition Bridge (`AgentDefinitionBridge.java`)

**Zweck:** Konvertiert Quarkus-AgentDefinition → Kafka WorkerAgentConfig

#### Feature-Lücken (dokumentiert)

| Feature | Status | Grund |
|---------|--------|-------|
| Skills Registry | ❌ Nicht im Worker | Skill-Lookup nicht implementiert |
| Named Hooks | ❌ Nicht im Worker | pre/post hook Infrastruktur fehlt |
| Directory Permissions | ❌ Nicht im Worker | Workspace-Konfiguration fehlt |
| MCP/Sandbox | ❌ Nicht im Worker | Externe Infrastruktur nötig |

#### Model Key Resolution

```
"gpt-4o"           → "gpt-4o"
"claude-3-opus"    → "claude-3"
"claude-3-sonnet"  → "claude-3"
"gemini-pro"       → "gemini"
null               → "gpt-4o" (default)
```

#### ReAct Iterations Heuristic

```
maxReActIterations = maxTokens / 4
min: 1
default: 8
```

### 2.3 Prometheus Metrics (`OrchestratorMetrics.java`)

**Endpoint:** `GET /actuator/prometheus`

#### Metriken

| Name | Typ | Beschreibung | Tags |
|------|-----|--------------|------|
| `orchestrator.workflows_started` | Counter | Workflows gestartet | `status` |
| `orchestrator.routing_decisions` | Counter | Routing-Entscheidungen | `node_type`, `decision` |
| `orchestrator.loop_iterations` | Counter | Loop-Iterationen | `workflow_name` |
| `orchestrator.checkpoints_persisted` | Counter | Checkpoints gespeichert | — |
| `orchestrator.hitl_tickets_created` | Counter | HITL Tickets erstellt | `reason` |
| `orchestrator.dlq_enqueued` | Counter | DLQ-Einträge | `error_type` |
| `orchestrator.workflow_duration_seconds` | DistributionSummary | Ausführungsdauer | — |
| `orchestrator.routing_evaluation_ms` | DistributionSummary | Routing-Eval-Zeit | — |
| `orchestrator.validation_duration_ms` | DistributionSummary | Validierungszeit | — |
| `orchestrator.hitl_open_count` | Gauge | Offene HITL Tickets | — |
| `orchestrator.stale_executions_found` | Gauge | Veraltete Executions gefunden | — |

#### application.yml Konfiguration

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  metrics:
    tags:
      application: agent-orchestrator
  prometheus:
    metrics:
      export:
        enabled: true
```

### 2.4 Grafana Dashboards

#### agent-orchestrator-overview.json

| Panel | Typ | Query |
|-------|-----|-------|
| Workflows Started | Bar Chart | `sum(increase(orchestrator.workflows_started[5m])) by (status)` |
| Workflow Duration P50/P95/P99 | Time Series | `histogram_quantile(..., rate(...[5m]))` |
| Routing Decisions | Bar Chart | `sum(increase(orchestrator.routing_decisions[5m])) by (node_type, decision)` |
| Loop Iterations | Stat | `sum(increase(orchestrator.loop_iterations[5m]))` |
| DLQ Events | Time Series | `increase(orchestrator.dlq_enqueued[1h])` |
| HITL Tickets | Stat | `orchestrator.hitl_open_count` |
| Error Breakdown | Pie Chart | `sum(increase(orchestrator.dlq_enqueued[1h])) by (error_type)` |

#### worker-performance.json

| Panel | Typ | Query |
|-------|-----|-------|
| Tasks Received by Agent Type | Bar Chart | `sum(increase(tasks_received_total[5m])) by (agentType)` |
| Tasks Completed | Bar Chart | `sum(increase(tasks_completed_total[5m])) by (agentType, status)` |
| Task Duration P50/P95/P99 | Time Series | `histogram_quantile(..., rate(task_duration_seconds_bucket[5m]))` |
| DLQ Rate | Time Series | `increase(dlq_total[1h])` |
| Error Breakdown | Pie Chart | `rate(tool_errors_total[5m]) by (errorClass)` |
| Kafka Consumer Lag | Time Series | `kafka_consumer_lag_records{group="agent-worker-group"}` |

#### queue-health.json

| Panel | Typ | Query |
|-------|-----|-------|
| Kafka Topic Lag | Time Series | `sum by (topic, partition) (kafka_consumer_lag_records)` |
| Intake vs Results Rate | Time Series | `rate(kafka_consumergroup_offsets{topic=...})` |
| Queue Depth Summary | Stat | `sum(kafka_consumer_lag_records{group="orchestrator"})` |

### 2.5 Unified API Gateway (`UnifiedApiGateway.java`)

**Base Path:** `/api/v1/unified`

#### Endpunkte

| Method | Path | Funktion | Header Required |
|--------|------|----------|-----------------|
| POST | `/workflows/from-quad` | Quad → Kafka Workflow Conversion | `X-Backend: quarkus` |
| POST | `/agents/convert` | Rich Agent → Worker Config | — |
| GET | `/metrics` | Merged Prometheus + Quarkus Metrics | — |
| GET | `/quarkus/{path}` | REST Proxy to Quarkus | — |
| GET | `/quarkus/stream/{path}` | SSE Proxy to Quarkus | — |

#### Beispielanfrage

```bash
curl -X POST http://localhost:8080/api/v1/unified/workflows/from-quad \
  -H "Content-Type: application/json" \
  -H "X-Backend: quarkus" \
  -d '{
    "name": "MyWorkflow",
    "nodes": [
      {"id": "n1", "type": "agent", "agentType": "researcher"},
      {"id": "n2", "type": "conditional", "properties": {"condition": "${score > 0.5}"}}
    ],
    "edges": [
      {"source": "n1", "target": "n2"}
    ]
  }'
```

### 2.6 Email Input Transformer (`EmailInputTransformer.java`)

**Aktivierung:** `app.email.transformer.enabled=true`  
**Endpunkt:** `POST /api/v1/email/receive`

#### Pipeline

```
Webhook Payload (Mailgun/SendGrid)
    │
    ├─► validate accepted address
    │
    ├─► extract trigger key from subject
    │     regex: \\[QUAD\\]\\s*(.+)
    │
    ├─► generate UUID workflowId
    │
    └─► publish to agent.execution.intake
          topic key: workflowId
          event type: EmailIntakeEvent
```

#### Configuration

```yaml
app:
  email:
    transformer:
      enabled: true
    address: tasks@quad.local
    workflow-key:
      subject-prefix: "[QUAD]"
```

### 2.7 Mail Response Handler (`MailResponseHandler.java`)

**Aktivierung:** `app.email.responder.enabled=true`

#### Event-basierte Emails

| Ergebnis | Email Betreff | Inhalt |
|----------|---------------|--------|
| COMPLETED | "Workflow Complete: {taskId}" | Summary mit Duration |
| NEEDS_HITL | "Action Required: HITL Approval for {taskId}" | Approval Link |
| FAILED_RETRYABLE | "Workflow Error: {taskId}" | Hinweis auf automatisches Retry |
| FAILED_FATAL | "Workflow Error: {taskId}" | Fatal error info |

#### Configuration

```yaml
app:
  email:
    responder:
      enabled: true
    address: tasks@quad.local
    approval-url: http://localhost:8080/hitl/approve
  kafka:
    result-topic: agent.execution.results
```

---

## 3. Dateistruktur

```
kafka-platform/
├── agent-common/src/main/java/com/agentplatform/
│   ├── bridge/                          [PHASE 1]
│   │   ├── AgentDefinitionBridge.java   (#100 Zeilen)
│   │   ├── QuarkusWorkflow.java         (#30 Zeilen)
│   │   ├── QuarkusNode.java             (#55 Zeilen)
│   │   ├── QuarkusEdge.java             (#25 Zeilen)
│   │   └── WorkflowDefinitionBridge.java (#250 Zeilen)
│   ├── common/event/
│   │   └── ControlEvents.java           (+13 Zeilen neue EmailIntakeEvent)
│   └── orchestrator/metrics/            [PHASE 2]
│       └── OrchestratorMetrics.java     (#115 Zeilen)
├── agent-orchestrator/src/main/java/com/agentplatform/orchestrator/
│   ├── bridge/web/                      [PHASE 3]
│   │   └── UnifiedApiGateway.java       (#200 Zeilen)
│   └── bridge/email/                    [PHASE 4]
│       ├── EmailInputTransformer.java   (#160 Zeilen)
│       ├── KafkaProducer.java           (#50 Zeilen)
│       └── MailResponseHandler.java     (#150 Zeilen)
├── agent-orchestrator/src/main/resources/
│   ├── application.yml                  (aktualisiert: actuator exposed)
│   └── ...
├── grafana/dashboards/                  [PHASE 2]
│   ├── agent-orchestrator-overview.json (#160 Zeilen)
│   ├── worker-performance.json          (#120 Zeilen)
│   └── queue-health.json                (#110 Zeilen)
├── agent-orchestrator/pom.xml           (aktualisiert: webflux added)
└── IMPL_STATUS.md                       ← dieses Dokument
```

---

## 4. Build & Deployment

### 4.1 Maven Build

```bash
cd kafka-platform
mvn clean verify -pl agent-common,agent-orchestrator
```

### 4.2 Docker Compose erweitern

Für Email-Funktionalität `docker-compose.yml` aktualisieren:

```yaml
  mail-server:
    image: axllent/mailpit:latest
    ports:
      - "1025:1025"   # SMTP
      - "8025:8025"   # Web UI
```

Application config ergänzen:

```yaml
spring:
  mail:
    host: ${MAIL_HOST:mail-server}
    port: ${MAIL_PORT:1025}
    username: ${MAIL_USERNAME:}
    password: ${MAIL_PASSWORD:}
```

### 4.3 Environment Variables

```bash
# Email Transformer
export APP_EMAIL_TRANSFORMER_ENABLED=true
export APP_EMAIL_ADDRESS=tasks@quad.local

# Mail Responder
export APP_EMAIL_RESPONDER_ENABLED=true
export APP_EMAIL_APPROVAL_URL=http://localhost:8080/hitl/approve

# Quarkus Proxy (für Unified Gateway)
export QUARKUS_BASE_URL=http://localhost:8086
```

---

## 5. Tests

### 5.1 Unit Tests für Bridge-Klassen

Empfohlene Test-Szenarien:

```java
// WorkflowBridgeTest
@Test void testFlatToNestedConversion() {...}
@Test void testConditionalBranchExtraction() {...}
@Test void testJoinPolicyMapping() {...}
@Test void testLoopBodyDetectionViaBfs() {...}
@Test void testUnknownNodeTypeDefaultsToStep() {...}
@Test void testNestedWorkflowsSkippedWithWarning() {...}

// AgentBridgeTest
@Test void testRichAgentToReducedConfig() {...}
@Test void testModelKeyResolution() {...}
@Test void testToolNameExtraction() {...}
@Test void testNullHandlingDefaults() {...}
```

### 5.2 Integrationstests

```bash
# Unified Gateway testen
curl -X POST http://localhost:8080/api/v1/unified/workflows/from-quad \
  -H "X-Backend: quarkus" \
  -H "Content-Type: application/json" \
  -d '{"name":"test","nodes":[],"edges":[]}'

# Prometheus Metrics prüfen
curl http://localhost:8080/actuator/prometheus
```

---

## 6. Phase 5 Ergänzungen (2026-09-18)

### 6.1 Redis Email Initiator Tracking (`EmailInitiatorStore.java`)

Speichert die Zuordnung `workflowId → initiatorEmail` in Redis.  
TTL: 7 Tage. Wird vom MailResponseHandler genutzt, um Ergebnis-EMails zurückzusenden.

Key-Patterns:
- `email:initator:{workflowId}` → email address
- `email:intake:{messageId}` → workflowId (Dedup)

### 6.2 GDPR Export/Delete API (`GdprController.java`)

| Method | Path | Beschreibung |
|--------|------|--------------|
| GET | `/api/v1/gdpr/export?session={id}` | Export Session-Daten als JSON |
| GET | `/api/v1/gdpr/export?workflowId={uuid}` | Export Execution + Checkpoints |
| DELETE | `/api/v1/gdpr/delete?session={id}&confirm=true` | Unwiderrufliche Löschung |
| POST | `/api/v1/gdpr/anonymize?session={id}` | PII durch Hash ersetzen |

### 6.3 Prefix Cache im Worker (`PrefixCache.java`)

Redis-basiertes Prompt-Caching zur Token-Einsparung.

Algorithmus:
```
1. SHA-256(prefix text) → cache hash
2. lookup redis prefix:cache:{hash}
3. Hit → use cached
4. Miss → resolve from source (file/URL/step), store in redis, return
```

Hash function:
```
stableHash(usePrefix, clearPrefix, cachePrefix, sourceType, sourceValue)
```

Stats (Redis keys):
- `prefix:stats:hits` — Cache Treffer
- `prefix:stats:misses` — Cache Fehlschläge

### 6.4 OTel / Jaeger Konfiguration

**docker/compose-extensions.yml:**
- Jaeger All-in-One Container (UI :16686, Collector :14268/:14250)
- OpenTelemetry Collector Contrib mit Transform Processor

**docker/otel-config.yaml:**
- Receivers: OTLP gRPC/HTTP, Prometheus Scrape
- Processors: Batch, Transform (resource attributes)
- Exporters: Jaeger, Prometheus, Logging

**application.yml Ergänzungen:**
```yaml
management:
  zipkin:
    base-url: ${OTEL_ZIPKIN_URL:http://jaeger:9411}
  traces:
    sampling:
      probability: ${OTEL_SAMPLING_RATE:0.1}
```

### 6.5 Intake Listener (`IntakeListener.java`)

Zwei Kafka Listener auf dem gleichen Topic mit verschiedenen Groups:
- `orchestrator-intake-group` — verarbeitet Start-Events
- `email-tracking-group` — extrahiert `initiatorEmail` und speichert in Redis

### 6.6 Frontend Integration

**kafkaApi.ts** — Neue TypeScript-API-Datei mit Funktionen:
- `fetchKafkaWorkflows()` — Kafka Workflow Definitions laden
- `convertQuadToKafka(...)` — Bridge Conversion via REST
- `startKafkaWorkflow(...)` — Workflow via Kafka starten
- `getKafkaWorkflowStatus(executionId)` — Status polling
- `cancelKafkaWorkflow(...)` — Abbrechen
- `fetchMergedMetrics()` — Merged Prometheus metrics

**WorkflowComposer.tsx** Änderungen:
- Backend-Selector Dropdown hinzugefügt (Quarkus / Kafka)
- `mapQuadNodeTypeToKafka()` — Typkonvertierung
- `runKafkaWorkflow()` — Kafka-spezifisches Polling
- Auto-load von Kafka workflows wenn Backend gewählt wird

---

## 7. TODO nach diesem Durchgang

| Item | Priorität | Aufwand | Anmerkung |
|------|-----------|---------|-----------|
| ~~Frontend-Integration (Tab-Auswahl)~~ | ~~P0~~ | ~~Medium~~ | ✅ Implemented in WorkflowComposer.tsx |
| ~~Redis Initiator-E-Mail Tracking~~ | ~~P1~~ | ~~Small~~ | ✅ EmailInitiatorStore.java |
| Mailbox-Listener (IMAP/POP3) | P1 | Medium | Alternative zu Mailgun/SendGrid Webhook |
| Schema Registry JSON Schemas | P2 | Medium | Confluent Schema Registry Integration |
| Vault Secrets Management | P2 | Medium | HashiCorp Vault Integration |
| ~~OTel/Jaeger Container Config~~ | ~~P2~~ | ~~Small~~ | ✅ compose-extensions.yml + otel-config.yaml |
| ~~GDPR Export/Delete APIs~~ | ~~P3~~ | ~~Small~~ | ✅ GdprController.java |
| ~~Advanced Context Prefix Caching~~ | ~~P3~~ | ~~Small~~ | ✅ PrefixCache.java |
| Multi-Tenant Support | P3 | Large | Tenant-Isolation über Redis Keyspace |
| Auto-scaling Worker | P3 | Large | Basierend auf Kafka Partition Offset Delta |

---

## 7. Referenzen

| Dokument | relevante Abschnitte |
|----------|---------------------|
| NEW.md | §3 Bridging, §4 Integration, §5 Email, §6 Observability |
| UI.md | §2 Format Analysis, §3 Integration Options, §5 Email, §6 Observability |
| SPEC2.md | §4 Topics, §5 Architecture, §10 HITL, §15 Deterministic Routing |
| TODO.md | Masterplan mit Done/To Do Checklist |

---

*Ende des Dokuments.*  
*Nächster Schritt: Frontend-Integration der Unified API*

---

## 8. Phase 6 Ergänzungen (2026-09-18) — Frontend + Redis + PrefixCache

### 8.1 quad-ui/src/services/kafkaApi.ts

Neue TypeScript-API-Datei für Kafka-Backend-Kommunikation:

```typescript
// Workflow Definitions
fetchKafkaWorkflows() → KafkaWorkflowDefinition[]
convertQuadToKafka(name, nodes, edges) → {converted, nodeCount}
startKafkaWorkflow(name, version, params, idempotencyKey) → {executionId, duplicate}
getKafkaWorkflowStatus(executionId) → status + checkpoints
cancelKafkaWorkflow(executionId) → void

// HITL
fetchKafkaHitlTickets() → HitlTicket[]
approveKafkaHitl(ticketId, decidedBy) → void
rejectKafkaHitl(ticketId, decidedBy) → void

// Agents
convertAgentForKafka(agentName, model, tools) → WorkerConfig

// Metrics
fetchMergedMetrics() → {kafkaOrchestrator, quadQuarkus, merged}
```

### 8.2 quad-ui/src/components/WorkflowComposer.tsx Modifications

**Zusätzliche States:**
```tsx
const [backend, setBackend] = useState<'quarkus' | 'kafka'>('quarkus')
const [kafkaWorkflows, setKafkaWorkflows] = useState<KafkaWorkflowDefinition[]>([])
```

**Toolbar Extension:**
```tsx
<select value={backend} onChange={e => setBackend(e.target.value)}>
  <option value="quarkus">🟣 Quarkus Backend</option>
  <option value="kafka">🟢 Kafka Orchestrator</option>
</select>
```

**Handler Änderungen:**
- `handleExecute()` prüft `backend` und ruft je nach Wert REST oder Kafka API auf
- `mapQuadNodeTypeToKafka(type)` wandelt Quad-Typen in Kafka-Enums um
- `runKafkaWorkflow(executionId)` pollt `/api/v1/workflows/{id}` alle 2s

### 8.3 kafka-platform/agent-orchestrator/.../bridge/redis/EmailInitiatorStore.java

Redis-basierte Speicherung der Zuordnung Email → Workflow-ID.

Keys:
- `email:initator:{workflowId}` → email address (TTL 7d)
- `email:intake:{messageId}` → workflowId (TTL 7d)

### 8.4 kafka-platform/agent-worker/.../cache/PrefixCache.java

Redis Prompt-Caching mit SHA-256 Hash als Key.

Features:
- `getOrFetch(hash, fallback)` — Cache Lookup
- `put(hash, value)` — Caches entry mit TTL
- `stableHash(usePrefix, clearPrefix, cachePrefix, sourceType, sourceValue)` — deterministische Hash-Erzeugung
- Preload-Funktion für Startup-Prefixes

Stats:
- `prefix:stats:hits` — Counter Treffer
- `prefix:stats:misses` — Counter Fehlschläge

### 8.5 docker/compose-extensions.yml

Jaeger All-in-One + OTel Collector Contrib Services:

```yaml
jaeger:
  image: jaegertracing/all-in-one:latest
  ports: ["16686:16686", "14268:14268", "14250:14250"]

otel-collector:
  image: otel/opentelemetry-collector-contrib:latest
  ports: ["4317:4317", "4318:4318", "8889:8889"]
```

### 8.6 docker/otel-config.yaml

OTel Collector Pipeline:
```
Receivers: OTLP gRPC/HTTP, Prometheus Scrape
Processors: Batch, Transform (resource attributes)
Exporters: Jaeger, Prometheus, Logging
Pipelines: traces → jaeger, metrics → prometheus
```

### 8.7 application.yml Ergänzungen

```yaml
management:
  zipkin:
    base-url: ${OTEL_ZIPKIN_URL:http://jaeger:9411}
  traces:
    sampling:
      probability: ${OTEL_SAMPLING_RATE:0.1}
```


---

## 9. Phase 7 Ergänzungen (2026-09-18) — Alerting, Vault, Schema Registry & Co.

### 9.1 ErrorClassifier (`agent-common/common/model/ErrorClassification.java`)

Drei-Kategorien-Taxonomie für Fehlerklassifizierung:

| Kategorie | Verhalten | Beispiele |
|-----------|-----------|-----------|
| `TRANSIENT` | Automatisch retryen | Timeout, Rate Limit, Netzwerkfehler, Circuit Breaker |
| `BUSINESS` | HITL-Eskalation nötig | Guardrail-Verletzung, niedrige Konfidenz, Format-Problem |
| `FATAL` | Sofort DLQ, kein Retry | Ungültige Step-ID, fehlende Config, Datenkorruption |

Heuristiken:
- `classify(String errorMessage)` — Keyword-Matching im Fehlertext
- `classifyFromException(Class<?>)` — Exception-Klassenname prüfen

### 9.2 HashiCorp Vault Integration (`SecretResolver.java`)

KV v2 Engine Client mit In-Memory Cache (TTL-basiert):

| Funktion | Beschreibung |
|----------|-------------|
| `resolve(path, key)` | Einzelnen Secret-Wert holen |
| `resolveAll(path)` | Alle Keys als Map |
| `invalidate(path, key)` | Cache-Eintrag entfernen |
| `isConnected()` | Health-Check |

Auth-Methoden:
- Kubernetes Service Account Token (empfohlen)
- AppRole (Role ID + Secret ID)

Vorgesehene Pfade:
- `secret/data/agent-platform/db` → DB Credentials
- `secret/data/agent-platform/openai` → LLM API Keys
- `secret/data/agent-platform/slack` → Slack Webhook URL
- `secret/data/agent-platform/pagerduty` → PD Integration Key

### 9.3 Alerting Module

**SlackNotifier:**
- HITL-Eskalation mit interaktiven Buttons (Approve/Reject/Retry)
- Simple Notifications mit Color-Coded Attachments
- Aktiv via `app.slack.enabled=true`

**PagerDutyAlertSender:**
- Critical/Major/Minor Severity Events (Events API v2)
- Resolve-Funktion nach HITL-Abschluss
- Aktiv via `app.pagerduty.enabled=true`

### 9.4 Schema Registry Client (`SchemaRegistryClient.java`)

JSON Schema Registration mit BACKWARD-Compatibility Enforcement:
- `registerAll()` — alle Schemas at startup
- `getVersion(subject, schema)` — neue Version falls changed
- `isCompatible(subject, newSchema)` — Kompatibilitätsprüfung
- `listSubjects(prefix)` — Registered subjects auflisten

Konfiguration:
```yaml
app:
  schema-registry:
    enabled: true
    url: http://schema-registry:8081
    compatibility-mode: BACKWARD
    auto-register: true
```

### 9.5 Resilience4j Circuit Breaker

Konfigurierte Breaker in application.yml:

| Breaker | Sliding Window | Failure Threshold | Open State Duration |
|---------|---------------|-------------------|---------------------|
| `llm-provider` | 10 calls | 50% | 30s |
| `worker-dispatch` | 20 calls | 70% | 60s |

Time limiters:
- `llm-call`: 90s timeout
- `task-dispatch`: 5s timeout

Dependencies added to orchestrator pom.xml:
```xml
resilience4j-spring-boot3:2.2.0
resilience4j-circularbuffer:2.2.0
```

### 9.6 Grafana Provisioning

Neue Verzeichnisstruktur:
```
grafana/
├── datasources/prometheus.yml       # Prometheus Datasource
└── provisioning/
    ├── dashboards/default.yml       # Dashboard Auto-Import
    └── datasources/default.yml      # Datasource Auto-Import
```

Prometheus Datasource config:
```yaml
url: ${GRAFANA_PROMETHEUS_URL:http://prometheus:9090}
timeInterval: 15s
httpMethod: POST
```

### 9.7 ConfigUpdateConsumer (`ConfigUpdateConsumer.java`)

Kafka Listener auf `agent.config.updates`:
- Lädt geänderte Agent-Configs aus PostgreSQL per CDC
- Unterstützt UPDATE und DELETE change types
- Gruppend-ID: `config-updates-group`

### 9.8 MailResponseHandler verbessert

- Import für ResultStatus korrigiert
- Parsing von Kafka JSON-Payload optimiert

### 9.9 Application.yml Ergänzungen

Resilience4j + OTel Tracing + Schéma Registry config hinzugefügt.

