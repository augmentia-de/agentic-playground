# Implementation Plan: Bridge Layer, Observability & Email Input for kafka-platform

**Created:** 2026-09-17  
**Last modified:** 2026-10-15  
**Status:** Phases 1–4 implemented + frontend integration  
**Sources:** NEW.md §3–§5, UI.md §2–§5  

> **Historical implementation log.** The phases below record what was built at the time.
> WP13 (dead-code removal) later deleted the unreachable opt-in components
> `PrefixCache`, `SlackNotifier`, `PagerDutyAlertSender` and `VaultConfig`, and moved
> `SchemaRegistryClient` to test scope; those entries are marked accordingly.
> Test counts are no longer stated here — CI (`.github/workflows/ci.yml`) reports them.

## 1. Overview

This document documents the implementation of the integration bridge between the Quarkus backend and the Kafka orchestrator as well as all supplementary components from NEW.md and UI.md.

### 1.1 Implemented Modules

| Phase | Module | Package | File(s) | Status |
|-------|-------|---------|-----------|--------|
| P0 | Workflow Bridge | `de.augmentia.agentplatform.bridge` | `WorkflowDefinitionBridge.java`, `QuarkusWorkflow.java`, `QuarkusNode.java`, `QuarkusEdge.java` | ✅ |
| P0 | Agent Bridge | `de.augmentia.agentplatform.bridge` | `AgentDefinitionBridge.java` (incl. WorkerAgentConfig, QuarkusAgent) | ✅ |
| P1 | Prometheus Metrics | `de.augmentia.agentplatform.orchestrator.metrics` | `OrchestratorMetrics.java` | ✅ |
| P1 | Grafana Dashboards | `grafana/dashboards/` | `agent-orchestrator-overview.json`, `worker-performance.json`, `queue-health.json` | ✅ |
| P2 | Unified API Gateway | `de.augmentia.agentplatform.bridge.web` | `UnifiedApiGateway.java` | ✅ |
| P3 | Email Transformer | `de.augmentia.agentplatform.bridge.email` | `EmailInputTransformer.java`, `KafkaProducer.java`, `MailResponseHandler.java` | ✅ |
| P3 | Control Events | `de.augmentia.agentplatform.common.event` | `ControlEvents.java` (extended with `EmailIntakeEvent`) | ✅ |
| P4 | Redis Initiator Store | `de.augmentia.agentplatform.bridge.redis` | `EmailInitiatorStore.java` | ✅ |
| P4 | GDPR API | `de.augmentia.agentplatform.orchestrator.web` | `GdprController.java` | ✅ |
| P4 | Prefix Cache | `de.augmentia.agentplatform.worker.cache` | ~~`PrefixCache.java`~~ | ❌ removed in WP13 (no reachable call path) |
| P4 | Intake Listener | `de.augmentia.agentplatform.orchestrator.kafka` | `IntakeListener.java` | ✅ |
| P4 | OTel/Jaeger Config | `docker/` | `compose-extensions.yml`, `otel-config.yaml` | ✅ |
| P4 | Frontend — Kafka API | `quad-quarkus/quad-ui/src/services/` | `kafkaApi.ts` | ✅ |
| P4 | Frontend — Backend Selector | `quad-quarkus/quad-ui/src/components/` | `WorkflowComposer.tsx` (modified) | ✅ |

### 1.2 Dependencies

New dependencies in `agent-orchestrator/pom.xml`:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webflux</artifactId>
</dependency>
```

Existing dependencies used:
- `spring-boot-starter-actuator` (already present for Prometheus)
- `spring-boot-starter-mail` must be added manually (for email functionality)

---

## 2. Detailed Documentation

### 2.1 Workflow Definition Bridge (`WorkflowDefinitionBridge.java`)

**Purpose:** Converts the quad-ui flat workflow format → Kafka DAG format

#### Mapping Table

| Quad Type | Kafka NodeType | Mapping |
|----------|----------------|---------|
| `agent` | `STEP` | Direct |
| `conditional` | `DECISION` | Extract edges → branches |
| `fork` | `FORK` | Edges → Branch.firstStep |
| `join` | `JOIN` | joinPolicy → JoinType |
| `loop` | `LOOP` | BFS loop-body detection |
| `nested-workflow` | ❌ Feature gap | Emitted as warning |

#### Algorithm

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

| Quad Property | Kafka Field | Transform |
|---------------|------------|-----------|
| `id` | `stepId` | Identical |
| `type` | `nodeType` | Enum mapping |
| `agentType` | `agentType` | Identical / default "generic" |
| `properties.joinPolicy` | `joinType` | `"all"`→`"AND"`, `"any"`→`"XOR"` |
| `timeoutMs` / `timeoutSeconds` | `timeoutMs` | Seconds × 1000 |
| `requiresHumanApproval` | `requiresHumanApproval` | Identical |
| `properties.guardCondition` | `guardCondition` | String |

### 2.2 Agent Definition Bridge (`AgentDefinitionBridge.java`)

**Purpose:** Converts Quarkus AgentDefinition → Kafka WorkerAgentConfig

#### Feature Gaps (documented)

| Feature | Status | Reason |
|---------|--------|-------|
| Skills Registry | ❌ Not in the worker | Skill lookup not implemented |
| Named Hooks | ❌ Not in the worker | pre/post hook infrastructure missing |
| Directory Permissions | ❌ Not in the worker | Workspace configuration missing |
| MCP/Sandbox | ❌ Not in the worker | External infrastructure required |

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

#### Metrics

| Name | Type | Description | Tags |
|------|-----|--------------|------|
| `orchestrator.workflows_started` | Counter | Workflows started | `status` |
| `orchestrator.routing_decisions` | Counter | Routing decisions | `node_type`, `decision` |
| `orchestrator.loop_iterations` | Counter | Loop iterations | `workflow_name` |
| `orchestrator.checkpoints_persisted` | Counter | Checkpoints persisted | — |
| `orchestrator.hitl_tickets_created` | Counter | HITL tickets created | `reason` |
| `orchestrator.dlq_enqueued` | Counter | DLQ entries | `error_type` |
| `orchestrator.workflow_duration_seconds` | DistributionSummary | Execution duration | — |
| `orchestrator.routing_evaluation_ms` | DistributionSummary | Routing eval time | — |
| `orchestrator.validation_duration_ms` | DistributionSummary | Validation time | — |
| `orchestrator.hitl_open_count` | Gauge | Open HITL tickets | — |
| `orchestrator.stale_executions_found` | Gauge | Stale executions found | — |

#### application.yml Configuration

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

| Panel | Type | Query |
|-------|-----|-------|
| Workflows Started | Bar Chart | `sum(increase(orchestrator.workflows_started[5m])) by (status)` |
| Workflow Duration P50/P95/P99 | Time Series | `histogram_quantile(..., rate(...[5m]))` |
| Routing Decisions | Bar Chart | `sum(increase(orchestrator.routing_decisions[5m])) by (node_type, decision)` |
| Loop Iterations | Stat | `sum(increase(orchestrator.loop_iterations[5m]))` |
| DLQ Events | Time Series | `increase(orchestrator.dlq_enqueued[1h])` |
| HITL Tickets | Stat | `orchestrator.hitl_open_count` |
| Error Breakdown | Pie Chart | `sum(increase(orchestrator.dlq_enqueued[1h])) by (error_type)` |

#### worker-performance.json

| Panel | Type | Query |
|-------|-----|-------|
| Tasks Received by Agent Type | Bar Chart | `sum(increase(tasks_received_total[5m])) by (agentType)` |
| Tasks Completed | Bar Chart | `sum(increase(tasks_completed_total[5m])) by (agentType, status)` |
| Task Duration P50/P95/P99 | Time Series | `histogram_quantile(..., rate(task_duration_seconds_bucket[5m]))` |
| DLQ Rate | Time Series | `increase(dlq_total[1h])` |
| Error Breakdown | Pie Chart | `rate(tool_errors_total[5m]) by (errorClass)` |
| Kafka Consumer Lag | Time Series | `kafka_consumer_lag_records{group="agent-worker-group"}` |

#### queue-health.json

| Panel | Type | Query |
|-------|-----|-------|
| Kafka Topic Lag | Time Series | `sum by (topic, partition) (kafka_consumer_lag_records)` |
| Intake vs Results Rate | Time Series | `rate(kafka_consumergroup_offsets{topic=...})` |
| Queue Depth Summary | Stat | `sum(kafka_consumer_lag_records{group="orchestrator"})` |

### 2.5 Unified API Gateway (`UnifiedApiGateway.java`)

**Base Path:** `/api/v1/unified`

#### Endpoints

| Method | Path | Function | Header Required |
|--------|------|----------|-----------------|
| POST | `/workflows/from-quad` | Quad → Kafka workflow conversion | `X-Backend: quarkus` |
| POST | `/agents/convert` | Rich Agent → Worker Config | — |
| GET | `/metrics` | Merged Prometheus + Quarkus Metrics | — |
| GET | `/quarkus/{path}` | REST Proxy to Quarkus | — |
| GET | `/quarkus/stream/{path}` | SSE Proxy to Quarkus | — |

#### Example Request

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

**Activation:** `app.email.transformer.enabled=true`  
**Endpoint:** `POST /api/v1/email/receive`

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

**Activation:** `app.email.responder.enabled=true`

#### Event-Based Emails

| Result | Email Subject | Content |
|----------|---------------|--------|
| COMPLETED | "Workflow Complete: {taskId}" | Summary with duration |
| NEEDS_HITL | "Action Required: HITL Approval for {taskId}" | Approval Link |
| FAILED_RETRYABLE | "Workflow Error: {taskId}" | Note about automatic retry |
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

## 3. File Structure

```
kafka-platform/
├── agent-common/src/main/java/com/agentplatform/
│   ├── bridge/                          [PHASE 1]
│   │   ├── AgentDefinitionBridge.java   (#100 lines)
│   │   ├── QuarkusWorkflow.java         (#30 lines)
│   │   ├── QuarkusNode.java             (#55 lines)
│   │   ├── QuarkusEdge.java             (#25 lines)
│   │   └── WorkflowDefinitionBridge.java (#250 lines)
│   ├── common/event/
│   │   └── ControlEvents.java           (+13 lines new EmailIntakeEvent)
│   └── orchestrator/metrics/            [PHASE 2]
│       └── OrchestratorMetrics.java     (#115 lines)
├── agent-orchestrator/src/main/java/com/agentplatform/orchestrator/
│   ├── bridge/web/                      [PHASE 3]
│   │   └── UnifiedApiGateway.java       (#200 lines)
│   └── bridge/email/                    [PHASE 4]
│       ├── EmailInputTransformer.java   (#160 lines)
│       ├── KafkaProducer.java           (#50 lines)
│       └── MailResponseHandler.java     (#150 lines)
├── agent-orchestrator/src/main/resources/
│   ├── application.yml                  (updated: actuator exposed)
│   └── ...
├── grafana/dashboards/                  [PHASE 2]
│   ├── agent-orchestrator-overview.json (#160 lines)
│   ├── worker-performance.json          (#120 lines)
│   └── queue-health.json                (#110 lines)
├── agent-orchestrator/pom.xml           (updated: webflux added)
└── IMPL_STATUS.md                       ← this document
```

---

## 4. Build & Deployment

### 4.1 Maven Build

```bash
cd kafka-platform
mvn clean verify -pl agent-common,agent-orchestrator
```

### 4.2 Extending Docker Compose

Update `docker-compose.yml` for email functionality:

```yaml
  mail-server:
    image: axllent/mailpit:latest
    ports:
      - "1025:1025"   # SMTP
      - "8025:8025"   # Web UI
```

Add application config:

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

### 5.1 Unit Tests for Bridge Classes

Recommended test scenarios:

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

### 5.2 Integration Tests

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

## 6. Phase 5 Additions (2026-09-18)

### 6.1 Redis Email Initiator Tracking (`EmailInitiatorStore.java`)

Stores the mapping `workflowId → initiatorEmail` in Redis.  
TTL: 7 days. Used by MailResponseHandler to send result emails back.

Key-Patterns:
- `email:initator:{workflowId}` → email address
- `email:intake:{messageId}` → workflowId (Dedup)

### 6.2 GDPR Export/Delete API (`GdprController.java`)

| Method | Path | Description |
|--------|------|--------------|
| GET | `/api/v1/gdpr/export?session={id}` | Export session data as JSON |
| GET | `/api/v1/gdpr/export?workflowId={uuid}` | Export execution + checkpoints |
| DELETE | `/api/v1/gdpr/delete?session={id}&confirm=true` | Irreversible deletion |
| POST | `/api/v1/gdpr/anonymize?session={id}` | Replace PII with a hash |

### 6.3 Prefix Cache in the Worker (`PrefixCache.java`) — removed in WP13

> Removed in WP13: the class had no reachable call path. Kept here for the record.

Redis-based prompt caching to save tokens.

Algorithm:
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
- `prefix:stats:hits` — cache hits
- `prefix:stats:misses` — cache misses

### 6.4 OTel / Jaeger Configuration

**docker/compose-extensions.yml:**
- Jaeger all-in-one container (UI :16686, collector :14268/:14250)
- OpenTelemetry Collector Contrib with transform processor

**docker/otel-config.yaml:**
- Receivers: OTLP gRPC/HTTP, Prometheus Scrape
- Processors: Batch, Transform (resource attributes)
- Exporters: Jaeger, Prometheus, Logging

**application.yml additions:**
```yaml
management:
  zipkin:
    base-url: ${OTEL_ZIPKIN_URL:http://jaeger:9411}
  traces:
    sampling:
      probability: ${OTEL_SAMPLING_RATE:0.1}
```

### 6.5 Intake Listener (`IntakeListener.java`)

Two Kafka listeners on the same topic with different groups:
- `orchestrator-intake-group` — processes start events
- `email-tracking-group` — extracts `initiatorEmail` and stores it in Redis

### 6.6 Frontend Integration

**kafkaApi.ts** — New TypeScript API file with functions:
- `fetchKafkaWorkflows()` — load Kafka workflow definitions
- `convertQuadToKafka(...)` — bridge conversion via REST
- `startKafkaWorkflow(...)` — start workflow via Kafka
- `getKafkaWorkflowStatus(executionId)` — Status polling
- `cancelKafkaWorkflow(...)` — cancel
- `fetchMergedMetrics()` — Merged Prometheus metrics

**WorkflowComposer.tsx** changes:
- Backend selector dropdown added (Quarkus / Kafka)
- `mapQuadNodeTypeToKafka()` — type conversion
- `runKafkaWorkflow()` — Kafka-specific polling
- Auto-load of Kafka workflows when a backend is selected

---

## 7. TODO After This Pass

| Item | Priority | Effort | Note |
|------|-----------|---------|-----------|
| ~~Frontend integration (tab selection)~~ | ~~P0~~ | ~~Medium~~ | ✅ Implemented in WorkflowComposer.tsx |
| ~~Redis initiator email tracking~~ | ~~P1~~ | ~~Small~~ | ✅ EmailInitiatorStore.java |
| Mailbox listener (IMAP/POP3) | P1 | Medium | Alternative to Mailgun/SendGrid webhook |
| Schema Registry JSON Schemas | P2 | Medium | Confluent Schema Registry integration |
| Vault Secrets Management | P2 | Medium | HashiCorp Vault integration |
| ~~OTel/Jaeger container config~~ | ~~P2~~ | ~~Small~~ | ✅ compose-extensions.yml + otel-config.yaml |
| ~~GDPR export/delete APIs~~ | ~~P3~~ | ~~Small~~ | ✅ GdprController.java |
| ~~Advanced context prefix caching~~ | ~~P3~~ | ~~Small~~ | ❌ removed in WP13 — `PrefixCache.java` (dead code) |
| Multi-Tenant Support | P3 | Large | Tenant isolation via Redis keyspace |
| Auto-scaling Worker | P3 | Large | Based on Kafka partition offset delta |

---

## 7. References

| Document | Relevant sections |
|----------|---------------------|
| NEW.md | §3 Bridging, §4 Integration, §5 Email, §6 Observability |
| UI.md | §2 Format Analysis, §3 Integration Options, §5 Email, §6 Observability |
| SPEC2.md | §4 Topics, §5 Architecture, §10 HITL, §15 Deterministic Routing |
| TODO.md | Master plan with Done/To Do checklist |

---

*End of document.*  
*Next step: frontend integration of the Unified API*

---

## 8. Phase 6 Additions (2026-09-18) — Frontend + Redis + PrefixCache

### 8.1 quad-ui/src/services/kafkaApi.ts

New TypeScript API file for Kafka backend communication:

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

**Additional states:**
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

**Handler changes:**
- `handleExecute()` checks `backend` and calls the REST or Kafka API depending on its value
- `mapQuadNodeTypeToKafka(type)` converts Quad types into Kafka enums
- `runKafkaWorkflow(executionId)` polls `/api/v1/workflows/{id}` every 2s

### 8.3 kafka-platform/agent-orchestrator/.../bridge/redis/EmailInitiatorStore.java

Redis-based storage of the email → workflow ID mapping.

Keys:
- `email:initator:{workflowId}` → email address (TTL 7d)
- `email:intake:{messageId}` → workflowId (TTL 7d)

### 8.4 kafka-platform/agent-worker/.../cache/PrefixCache.java — removed in WP13

> Removed: no reachable call path (WP13). Kept here for the record.

Redis prompt caching with a SHA-256 hash as the key.

Features:
- `getOrFetch(hash, fallback)` — cache lookup
- `put(hash, value)` — caches entry with TTL
- `stableHash(usePrefix, clearPrefix, cachePrefix, sourceType, sourceValue)` — deterministic hash generation
- Preload function for startup prefixes

Stats:
- `prefix:stats:hits` — counter hits
- `prefix:stats:misses` — counter misses

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

### 8.7 application.yml Additions

```yaml
management:
  zipkin:
    base-url: ${OTEL_ZIPKIN_URL:http://jaeger:9411}
  traces:
    sampling:
      probability: ${OTEL_SAMPLING_RATE:0.1}
```


---

## 9. Phase 7 Additions (2026-09-18) — Alerting, Vault, Schema Registry & Co.

### 9.1 ErrorClassifier (`agent-common/common/model/ErrorClassification.java`)

Three-category taxonomy for error classification:

| Category | Behavior | Examples |
|-----------|-----------|-----------|
| `TRANSIENT` | Automatically retry | Timeout, rate limit, network error, circuit breaker |
| `BUSINESS` | HITL escalation required | Guardrail violation, low confidence, format problem |
| `FATAL` | Immediate DLQ, no retry | Invalid step ID, missing config, data corruption |

Heuristics:
- `classify(String errorMessage)` — keyword matching in the error text
- `classifyFromException(Class<?>)` — check the exception class name

### 9.2 HashiCorp Vault Integration (`SecretResolver.java`)

KV v2 engine client with in-memory cache (TTL-based):

| Function | Description |
|----------|-------------|
| `resolve(path, key)` | Fetch a single secret value |
| `resolveAll(path)` | All keys as a map |
| `invalidate(path, key)` | Remove cache entry |
| `isConnected()` | Health check |

Auth methods:
- Kubernetes service account token (recommended)
- AppRole (Role ID + Secret ID)

Intended paths:
- `secret/data/agent-platform/db` → DB Credentials
- `secret/data/agent-platform/openai` → LLM API Keys
- `secret/data/agent-platform/slack` → Slack Webhook URL
- `secret/data/agent-platform/pagerduty` → PD Integration Key

### 9.3 Alerting Module — removed in WP13

> Removed: `SlackNotifier` and `PagerDutyAlertSender` had no reachable call path (WP13).
> The classes below are kept here for the record.

**SlackNotifier:**
- HITL escalation with interactive buttons (Approve/Reject/Retry)
- Simple notifications with color-coded attachments
- Enabled via `app.slack.enabled=true`

**PagerDutyAlertSender:**
- Critical/major/minor severity events (Events API v2)
- Resolve function after HITL completion
- Enabled via `app.pagerduty.enabled=true`

### 9.4 Schema Registry Client (`SchemaRegistryClient.java`) — moved to test scope in WP13

> Moved to `src/test/java`: only its own test referenced it, so it is no longer a
> production component. Kept here for the record.

JSON schema registration with BACKWARD compatibility enforcement:
- `registerAll()` — all schemas at startup
- `getVersion(subject, schema)` — new version if changed
- `isCompatible(subject, newSchema)` — compatibility check
- `listSubjects(prefix)` — list registered subjects

Configuration:
```yaml
app:
  schema-registry:
    enabled: true
    url: http://schema-registry:8081
    compatibility-mode: BACKWARD
    auto-register: true
```

### 9.5 Resilience4j Circuit Breaker

Configured breakers in application.yml:

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

New directory structure:
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

Kafka listener on `agent.config.updates`:
- Loads changed agent configs from PostgreSQL via CDC
- Supports UPDATE and DELETE change types
- Group ID: `config-updates-group`

### 9.8 MailResponseHandler Improved

- Fixed import for ResultStatus
- Optimized parsing of the Kafka JSON payload

### 9.9 Application.yml Additions

Added Resilience4j + OTel tracing + schema registry config.
