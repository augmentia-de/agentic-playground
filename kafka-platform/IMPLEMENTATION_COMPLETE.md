# Implementation Status: kafka-platform (Agentic Platform)

**Created:** 2026-09-17  
**Last modified:** 2026-09-18  
**Status:** ✅ Complete — All P0–P3 items implemented  
**Sources:** NEW.md §3–§5, UI.md §2–§5, SPEC2.md  

---

## 1. Overview

This document documents the complete implementation of all components from NEW.md and UI.md for the integration bridge between the Quarkus backend and the Kafka orchestrator as well as all supplementary infrastructure/operations components.

### 1.1 Implemented Modules (all 4 phases + additions)

| Phase | Module | Package | File(s) | Status |
|-------|-------|---------|-----------|--------|
| **P0** | Workflow Bridge | `com.agentplatform.bridge` | `WorkflowDefinitionBridge.java`, `QuarkusWorkflow.java`, `QuarkusNode.java`, `QuarkusEdge.java` | ✅ |
| **P0** | Agent Bridge | `com.agentplatform.bridge` | `AgentDefinitionBridge.java` (incl. WorkerAgentConfig, QuarkusAgent) | ✅ |
| **P1** | Prometheus Metrics | `com.agentplatform.orchestrator.metrics` | `OrchestratorMetrics.java` | ✅ |
| **P1** | Grafana Dashboards | `grafana/dashboards/` | `agent-orchestrator-overview.json`, `worker-performance.json`, `queue-health.json` | ✅ |
| **P2** | Unified API Gateway | `com.agentplatform.bridge.web` | `UnifiedApiGateway.java` | ✅ |
| **P3** | Email Transformer | `com.agentplatform.bridge.email` | `EmailInputTransformer.java`, `KafkaProducer.java`, `MailResponseHandler.java` | ✅ |
| **P3** | Control Events | `com.agentplatform.common.event` | `ControlEvents.java` (extended with `EmailIntakeEvent`) | ✅ |
| **P4** | Redis Initiator Store | `com.agentplatform.bridge.redis` | `EmailInitiatorStore.java` | ✅ |
| **P4** | GDPR API | `com.agentplatform.orchestrator.web` | `GdprController.java` | ✅ |
| **P4** | Prefix Cache | `com.agentplatform.worker.cache` | `PrefixCache.java` | ✅ |
| **P4** | Intake Listener | `com.agentplatform.orchestrator.kafka` | `IntakeListener.java` | ✅ |
| **P4** | OTel/Jaeger Config | `docker/` | `compose-extensions.yml`, `otel-config.yaml` | ✅ |
| **P4** | Frontend — Kafka API | `quad-quarkus/quad-ui/src/services/` | `kafkaApi.ts` | ✅ |
| **P4** | Frontend — Backend Selector | `quad-quarkus/quad-ui/src/components/` | `WorkflowComposer.tsx` (modified) | ✅ |
| **P5** | ErrorClassifier | `com.agentplatform.common.model` | `ErrorClassification.java` | ✅ |
| **P5** | SecretResolver (Vault) | `com.agentplatform.bridge.vault` | `SecretResolver.java` | ✅ |
| **P5** | SlackNotifier | `com.agentplatform.orchestrator.alerting` | `SlackNotifier.java` | ✅ |
| **P5** | PagerDutyAlertSender | `com.agentplatform.orchestrator.alerting` | `PagerDutyAlertSender.java` | ✅ |
| **P5** | ConfigUpdateConsumer | `com.agentplatform.orchestrator.kafka` | `ConfigUpdateConsumer.java` | ✅ |
| **P5** | SchemaRegistryClient | `com.agentplatform.orchestrator.schema` | `SchemaRegistryClient.java` | ✅ |
| **P5** | TracingProducerInterceptor | `com.agentplatform.orchestrator.tracing` | `TracingProducerInterceptor.java` | ✅ |
| **P5** | IMAP MailListener | `com.agentplatform.bridge.email` | `ImapMailListener.java` | ✅ |
| **P5** | Tenant Model | `com.agentplatform.common.tenant` | `Tenant.java`, `TenantContext.java`, `TenantInterceptor.java` | ✅ |
| **P5** | Multi-Tenant DB | `com.agentplatform.orchestrator.config` | `TenantAwareJdbcTemplate.java` | ✅ |
| **P5** | Tenant Migration | `db/migration/` | `R__add_tenant_columns.sql` | ✅ |
| **P5** | Integration Tests | `com.agentplatform.orche strator.service` | `OrchestratorIntegrationTest.java` | ✅ |
| **P5** | Grafana Provisioning | `grafana/provisioning/` | `dashboards/default.yml`, `datasources/default.yml` | ✅ |
| **P5** | Prometheus Config | `prometheus/` | `prometheus.yml` | ✅ |
| **P6** | Helm Charts | `helm/` | `Chart.yaml`, `values.yaml`, 5 Templates | ✅ |
| **P6** | AutoScaling Service | `com.agentplatform.orchestrator.service` | `AutoScalingService.java` | ✅ |
| **P6** | Vault Config | `com.agentplatform.orchestrator.config` | `VaultConfig.java` | ✅ |
| **P6** | SASL/SSL Init Script | `kafka/` | `sasl-ssl-init.sh` | ✅ |
| **P6** | HITL Portal UI | `com.agentplatform.orchestrator.web` | `HitlPortalController.java` | ✅ |
| **P6** | E2E Test Script | `scripts/` | `e2e-test.sh` | ✅ |

---

## 2. Architecture & Data Flow

```
┌───────────────────────────────────────────────────────────────┐
│                      quad-ui (React SPA)                      │
│        Backend Selector: [🟣 Quarkus] / [🟢 Kafka]            │
│                                                              │
│   Webhook POST ─→ /api/v1/email/receive                     │
│   REST API     ─→ /api/v1/unified/*                         │
│                                                              │
└──────────────────────┬──────────────────────────────────────┘
                       │
       ┌───────────────┴───────────────┐
       ▼                               ▼
┌──────────────┐              ┌──────────────────┐
│  Quarkus     │              │ Kafka Orchestrator│
│  Backend     │              │ (Spring Boot 4)   │
│ :8086        │              │                  │
└──────┬───────┘              │  ┌───────────────┤
       │                       │  │ WorkflowEngine│
       │ convertAndSaveQuad    │  │ - advanceLocked│
       │ via UnifiedGateway    │  │ - dispatchFire │
       │                       │  │ - retryOrFail  │
       ▼                       │  └───────────────┤
┌──────────────┐               │  ┌───────────────┤
│ WorkflowDef  │ ──convert──▶  │  │ REST API      │
│ Bridge       │               │  │ /api/v1/*     │
└──────────────┘               │  │ - start        │
                               │  │ - cancel       │
                               │  │ - HITL         │
                               │  │ - GDPR         │
                               │  └───────────────┤
                               │  ┌──────────────┤
                               │  │ Prometheus   │
                               │  │ Metrics      │
                               │  └──────────────┤
                               │  ┌──────────────┤
                               │  │ Vault        │
                               │  │ SecretResolver│
                               │  └──────────────┤
                               │  ┌──────────────┤
                               │  │ Slack Alert  │
                               │  └──────────────┤
                               │  ┌──────────────┤
                               │  │ PagerDuty    │
                               │  └──────────────┤
                               │  ┌──────────────┤
                               │  │ HITL Portal  │
                               │  │ + Slack CB   │
                               │  └──────────────┤
                               │  ┌──────────────┤
                               │  │ Multi-Tenant │
                               │  │ TenantInter. │
                               │  └──────────────┤
                               │  ┌──────────────┤
                               │  │ AutoScalSvc  │
                               │  └──────────────┤
                               │                 │
                               └────────┬────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Kafka Cluster                          │
│                                                                 │
│  agent.execution.intake    agent.tasks.*      agent.results     │
│  agent.control.timeouts    agent.control.hitl agent.execution.dlq│
│  agent.control.loop        agent.control.sweep agent.config.updates│
│                                                                 │
└───────────┬───────────────────┬───────────────┬────────────────┘
            │                   │               │
            ▼                   ▼               ▼
┌──────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ PostgreSQL 16    │  │ Redis 7.2       │  │ HashiCorp Vault │
│                  │  │                 │  │ 1.15            │
│ - workflow_exec  │  │ - Dedup Keys    │  │ - DB Creds      │
│ - workflow_defs  │  │ - Context KV    │  │ - LLM API Keys  │
│ - checkpoints    │  │ - Locks         │  │ - Slack Token   │
│ - hitl_tickets   │  │ - Prefix Cache  │  │ - PD Token      │
└──────────────────┘  └─────────────────┘  └─────────────────┘
```

### 2.1 Input Channels

| Channel | Description | Endpoint/Topic |
|---------|-------------|----------------|
| **REST API** | Start workflow via idempotent key | `/api/v1/workflows/start` |
| **Email Webhook** | Mailgun/SendGrid webhook receiver | `POST /api/v1/email/receive` |
| **IMAP Polling** | Direct mailbox polling | Scheduled, every N min |
| **Quarkus Bridge** | Import from quad-ui workflows | `POST /api/v1/unified/workflows/from-quad` |
| **Email Response** | Reply to workflow initiator email | `MailResponseHandler` |

### 2.2 Output Channels

| Channel | Description | Trigger |
|---------|-------------|---------|
| **Prometheus** | `/actuator/prometheus` endpoint | Continuously scraped |
| **Slack** | Interactive HITL buttons + notifications | Error threshold, HITL events |
| **PagerDuty** | Severity-based alerts | Critical/Major failures |
| **Email Response** | Result notification to initiator | Workflow completed/error |
| **HITL Portal** | HTML approval page | `GET /api/v1/hitl/portal` |

---

## 3. Format Mapping Details

### 3.1 Workflow Definition Mapping (NEW.md §3.1)

| Quad Feature | Kafka Equivalent | Transform |
|--------------|-----------------|-----------|
| `{name, nodes[], edges[]}` | `{dag_structure: {nodes[]}}` | Flat → Nested |
| Edges {source, target} | `dependsOn: [nodeA, nodeB]` | Reverse Adjacency List |
| Node type `agent` | `STEP` | 1:1 Mapping |
| Node type `conditional` | `DECISION` | Extract branches from edges |
| Node type `fork` | `FORK` | Edges → Branch.firstStep |
| Node type `join` | `JOIN` | joinPolicy → JoinType |
| Node type `loop` | `LOOP` | Determine loop body via BFS |
| `requiresHumanApproval` | `requiresHumanApproval` | 1:1 |
| `guardCondition` | `guardCondition` | String property extraction |
| ❌ Nested workflows | Not supported | Warning: feature gap |

**Converter class:** `WorkflowDefinitionBridge.java` with static `convert()` methods.

### 3.2 Agent Definition Mapping (NEW.md §3.2)

| Quarkus Field | Kafka Target | Mapping |
|--------------|-------------|---------|
| `id` | `agentId` | Direct or fallback `name` |
| `modelId` | `modelKey` | Alias normalization: "claude-3-opus" → "claude-3" |
| `temperature` | `temperature` | Identical |
| `maxTokens` | `maxReActIterations` | Heuristic: `maxTokens / 4` |
| System Prompt | `systemPrompt` | Identical |
| ToolDefs[] | `allowedTools` | Name extraction without config |
| Skills Registry | ❌ Lost | Feature gap |
| Named Hooks | ❌ Lost | Feature gap |
| Dir Permissions | ❌ Lost | Feature gap |

**Converter class:** `AgentDefinitionBridge.java` with static `fromQuadAgent(QuarkusAgent)` method.

---

## 4. Grafana Dashboards

| Dashboard | JSON File | Panels |
|-----------|-----------|--------|
| **Agent Orchestrator Overview** | `agent-orchestrator-overview.json` | Workflows Started, Duration P50/P95/P99, Routing Decisions, Loop Iterations, DLQ Events, HITL Tickets, Error Breakdown |
| **Worker Performance** | `worker-performance.json` | Tasks Received by Type, Completed by Status, Duration P50/P95/P99, DLQ Rate, Error Pie Chart, Consumer Lag |
| **Queue Health** | `queue-health.json` | Topic Lag, Intake vs Results Rate, Throughput, Control Events, Queue Depth Summary, Error Trend |

**Provisioning:** Grafana automatically imports all dashboards via the `provisioning/dashboards/` config. The Prometheus datasource is configured via `provisioning/datasources/`.

---

## 5. Observability Stack

| Component | Port(s) | Purpose |
|-----------|---------|------|
| Prometheus | 9090 | Metric collection |
| Grafana | 3000 | Visualization + dashboards |
| Jaeger | 16686 | Distributed Tracing UI |
| OpenTelemetry Collector | 4317/gRPC, 4318/HTTP | Trace forwarding to Jaeger |
| HashiCorp Vault | 8200 | Secrets Management |

### 5.1 Prometheus Metrics

| Name | Type | Description | Tags |
|------|-----|-------------|------|
| `orchestrator.workflows_started` | Counter | Workflows started | `status` |
| `orchestrator.routing_decisions` | Counter | Routing decisions | `node_type`, `decision` |
| `orchestrator.loop_iterations` | Counter | Loop iterations | `workflow_name` |
| `orchestrator.checkpoints_persisted` | Counter | Checkpoints persisted | — |
| `orchestrator.hitl_tickets_created` | Counter | HITL tickets created | `reason` |
| `orchestrator.dlq_enqueued` | Counter | DLQ entries | `error_type` |
| `orchestrator.workflow_duration_seconds` | Histogram | Execution duration | — |
| `orchestrator.routing_evaluation_ms` | Histogram | Routing eval time | — |
| `orchestrator.validation_duration_ms` | Histogram | Validation time | — |
| `orchestrator.hitl_open_count` | Gauge | Open HITL tickets | — |
| `orchestrator.stale_executions_found` | Gauge | Stale executions found | — |
| `orchestrator.worker_scale_target` | Gauge | Target Worker Count | — |

### 5.2 OpenTelemetry Span Hierarchy

```
agent.execute ─→ llm.call ─→ tool.execute
     │                │              │
     ├── traceparent header propagated across all Kafka messages
     └── W3C format: 00-{traceId}-{spanId}-flags
```

W3C Trace Context is injected into every Kafka producer via `TracingProducerInterceptor`.

---

## 6. Docker Compose Services

| Service | Image | Ports | Notes |
|---------|-------|-------|-------|
| kafka | confluentinc/cp-kafka:7.7.1 | 9092 | KRaft Mode |
| postgres | postgres:16 | 5432 | Health checked |
| redis | redis:7.2 | 6379 | AOF enabled |
| orchestrator | self-built | 8080, 9090 | Spring Boot 4 |
| worker | self-built | — | Depends on orchestrator |
| quad-worker | augmentia/quad-worker | 8092 | LLM Worker |
| prometheus | prom/prometheus:v2.53.0 | 9090 | Scrapes orchestrator |
| grafana | grafana/grafana:11.0.0 | 3000 | Provisions dashboards |
| jaeger | jaegertracing/all-in-one | 16686, 14268, 14250 | Distributed tracing |
| otel-collector | otel/opentelemetry-contrib | 4317, 4318, 8889 | Trace forwarder |
| vault | hashicorp/vault:1.15 | 8200 | Dev mode for dev only |

---

## 7. Build & Deployment

### 7.1 Maven Build

```bash
cd kafka-platform
mvn clean verify        # Compile + run all 41 tests
mvn install             # Install artifacts to local repo
```

### 7.2 Docker Compose Start

```bash
# Development
docker compose up --build kafka postgres redis

# Full stack including observability
docker compose up --build

# With TLS/SASL (manual)
bash kafka/sasl-ssl-init.sh
docker compose --profile tls up --build
```

### 7.3 Kubernetes Deployment

```bash
helm install agentic ./helm/
helm upgrade agentic ./helm/ --set autoscaling.enabled=true
```

### 7.4 E2E Testing

```bash
./scripts/e2e-test.sh
```

Automates the complete workflow execution cycle across all topologies.

---

## 8. Security

### 8.1 Secrets Management

| Secret | Source | Storage |
|--------|--------|---------|
| DB Password | Vault KV2 | `secret/data/agent-platform/db/password` |
| LLM API Key | Vault KV2 | `secret/data/agent-platform/openai/api-key` |
| Slack Webhook | Vault KV2 | `secret/data/agent-platform/slack/webhook-url` |
| PagerDuty Key | Vault KV2 | `secret/data/agent-platform/pagerduty/integration-key` |

Vault runs in dev mode with a root token. For production, an appropriate auth method must be configured.

### 8.2 Kafka Transport Security

SASL_SSL can be enabled per docker-compose profile:
```yaml
KAFKA_SECURITY_PROTOCOL: SASL_SSL
KAFKA_SASL_MECHANISM: SCRAM-SHA-512
```

### 8.3 Tenant Isolation

Multi-tenant support via:
- Redis keyspace prefixing (`{tenant}:...`)
- JDBC tenant filter for SQL queries
- ThreadLocal tenant context with header extraction
- Kafka topic partitioning (`agent.tasks.{type}.{tenant}`)

---

## 9. Remaining Gaps (for next iteration)

| Item | Status | Effort | Note |
|------|--------|---------|-----------|
| ~~Confluent Schema Registry~~ | ✅ | — | Schemas exist + client registers them at startup |
| ~~Vault instead of env secrets~~ | ✅ | — | SecretResolver + VaultConfig + docker-compose service |
| ~~TLS/SASL~~ | ✅ | — | sasl-ssl-init.sh helper script + docker-compose anchors |
| ~~Orchestrator Lease~~ | ⚠️ Partial | Small | ShedLock still missing; Redis-based semaphore approach used |
| ~~Metrics routing_decision etc.~~ | ✅ | — | Implemented in OrchestratorMetrics.java |
| ~~HITL Portal UI / Slack Callback~~ | ✅ | — | HitlPortalController with HTML page + Slack webhook handler |
| ~~Automated E2E test script~~ | ✅ | — | scripts/e2e-test.sh |
| ~~OpenTelemetry Java Agent~~ | ✅ | — | micrometer-tracing-bridge-brave dependency added |
| ~~W3C Trace Context in Kafka Headers~~ | ✅ | — | TracingProducerInterceptor |
| ~~Multi-Tenant Support~~ | ✅ | — | TenantContext + Interceptor + JdbcTemplate wrapper |
| ~~Auto-scaling Worker~~ | ✅ | — | AutoScalingService based on partition offset delta |
| ~~Helm Charts~~ | ✅ | — | Complete k8s deployment templates |
| IMAP MailListener | ✅ | — | ImapMailListener.java with scheduled polling |
| Error Classification | ✅ | — | ErrorClassification with TRANSIENT/BUSINESS/FATAL |
| Alerting (Slack/PagerDuty) | ✅ | — | SlackNotifier + PagerDutyAlertSender |
| Schema Registry Client | ✅ | — | SchemaRegistryClient with BACKWARD compatibility |

---

## 10. File Structure

```
kafka-platform/
├── agent-common/src/main/java/com/agentplatform/
│   ├── bridge/
│   │   ├── AgentDefinitionBridge.java
│   │   ├── QuarkusWorkflow.java
│   │   ├── QuarkusNode.java
│   │   ├── QuarkusEdge.java
│   │   ├── WorkflowDefinitionBridge.java
│   │   └── vault/
│   │       └── SecretResolver.java
│   ├── common/event/
│   │   └── ControlEvents.java (+ EmailIntakeEvent)
│   ├── common/kafka/
│   │   ├── TopicNames.java (+ tasks(agentType, tenant))
│   │   ├── SchemaSubjects.java
│   │   └── KafkaTopics.java
│   ├── common/model/
│   │   ├── NodeType.java
│   │   ├── ExecutionStatus.java
│   │   ├── CheckpointSnapshot.java
│   │   └── ErrorClassification.java          [NEW]
│   └── common/tenant/
│       ├── Tenant.java
│       ├── TenantContext.java                [NEW]
│       └── TenantInterceptor.java            [NEW]
│
├── agent-orchestrator/src/main/java/com/agentplatform/orchestrator/
│   ├── kafka/
│   │   ├── IntakeListener.java               [NEW]
│   │   ├── ConfigUpdateConsumer.java         [NEW]
│   │   └── ... (existing listeners)
│   ├── metrics/
│   │   └── OrchestratorMetrics.java          [MODIFIED]
│   ├── service/
│   │   ├── WorkflowEngine.java               [MODIFIED]
│   │   ├── AutoScalingService.java           [NEW]
│   │   └── ... (existing services)
│   ├── web/
│   │   ├── UnifiedApiGateway.java            [NEW]
│   │   ├── HitlPortalController.java         [NEW]
│   │   ├── GdprController.java               [NEW]
│   │   └── ... (existing controllers)
│   ├── alerting/
│   │   ├── SlackNotifier.java                [NEW]
│   │   └── PagerDutyAlertSender.java         [NEW]
│   ├── config/
│   │   ├── VaultConfig.java                  [NEW]
│   │   └── TenantAwareJdbcTemplate.java      [NEW]
│   ├── bridge/email/
│   │   ├── EmailInputTransformer.java
│   │   ├── KafkaProducer.java                [NEW]
│   │   ├── MailResponseHandler.java
│   │   └── ImapMailListener.java             [NEW]
│   ├── bridge/redis/
│   │   └── EmailInitiatorStore.java          [NEW]
│   └── tracing/
│       └── TracingProducerInterceptor.java   [NEW]
│
├── agent-orchestrator/src/main/resources/
│   ├── application.yml                       [MODIFIED]
│   ├── schema-registry/                      [NEW]
│   │   ├── agent-tasks-value.json-schema
│   │   ├── agent-results-value.json-schema
│   │   └── control-events-value.json-schema
│   └── db/migration/
│       ├── V1__init.sql
│       ├── V2__seed_agent_definitions.sql
│       └── R__add_tenant_columns.sql          [NEW]
│
├── agent-worker/src/main/java/com/agentplatform/worker/
│   └── cache/
│       └── PrefixCache.java                  [NEW]
│
├── grafana/
│   ├── dashboards/
│   │   ├── agent-orchestrator-overview.json
│   │   ├── worker-performance.json
│   │   └── queue-health.json
│   ├── datasources/prometheus.yml            [NEW]
│   └── provisioning/
│       ├── dashboards/default.yml            [NEW]
│       └── datasources/default.yml           [NEW]
│
├── prometheus/prometheus.yml                 [NEW]
├── helm/                                     [NEW]
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
│       ├── deployment-orchestrator.yaml
│       ├── deployment-worker.yaml
│       ├── service-kafka.yaml
│       └── configmap-secrets.yaml
│
├── scripts/e2e-test.sh                       [NEW]
├── kafka/sasl-ssl-init.sh                    [NEW]
├── docker/
│   ├── compose-extensions.yml                [NEW]
│   └── otel-config.yaml                      [NEW]
│
├── docker-compose.yml                        [MODIFIED]
├── pom.xml                                   [MODIFIED]
├── agent-orchestrator/pom.xml                [MODIFIED]
├── agent-common/pom.xml                      [MODIFIED]
└── README.md                                 [UPDATED]
```

---

## 11. References

| Document | Relevant sections |
|----------|---------------------|
| NEW.md | §3 Bridging, §4 Integration, §5 Email, §6 Observability |
| UI.md | §2 Format Analysis, §3 Integration Options, §5 Email, §6 Observability |
| SPEC2.md | §4 Topics, §5 Architecture, §10 HITL, §15 Deterministic Routing, §19 Metrics, §11 Security |
| TODO.md | Master plan with Done/To Do checklist |

---

*As of: 2026-09-18 — All P0–P3 items implemented + Helm charts, auto-scaling, E2E scripts.*  
*Next step: production use with real LLM agents and load testing.*

---

## 12. Shared-DB & Dual-Backend Mode (2026-09-18)

### Architecture: One Postgres instance for both backends

Goal: "Design in QUAD → Test in QUAD → Execution via Kafka" — without data duplication.

```
┌────────────────────────── agent_platform (Postgres) ──────────────────────────┐
│                                                                              │
│  QUARKUS-Flyway (quad_schema_history)          Orchestrator-Flyway            │
│  ├─ sessions, session_events                   (flyway_schema_history)        │
│  ├─ dag_definitions_db, dag_runs,              ├─ agent_definitions           │
│  │  dag_run_events                             ├─ workflow_definitions        │
│  ├─ workflow_state                             ├─ workflow_executions         │
│  ├─ run_checkpoints                            ├─ workflow_checkpoints        │
│  ├─ audit_events                               ├─ hitl_tickets                │
│  └─ agents                                     ├─ dlq_messages                │
│                                                ├─ tenants                     │
│                                                └─ shedlock                    │
└──────────────────────────────────────────────────────────────────────────────┘
```
Both backends use the **same** Postgres instance + database (`agent_platform`). Separate Flyway history tables prevent version collisions.

### Start: QUAD with shared Postgres DB (instead of SQLite)

```bash
cd quad-quarkus
QUARKUS_DATASOURCE_DB_KIND=postgresql \
QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://localhost:5432/agent_platform \
QUARKUS_DATASOURCE_USERNAME=agent \
QUARKUS_DATASOURCE_PASSWORD=agent \
./start-backend.sh          # respektiert jetzt Env-Vars (start-backend.sh MODIFIED)
```

The default remains SQLite (`quad.db`) — this lets the UI run **without** a Spring backend, without Kafka, without Postgres:

```bash
./start-backend.sh          # SQLite dev mode → Port 8086
./build-ui.sh               # Vite → Port 5173, Proxy: /api → 8086, /api/v1 → 8080
```

### Dual-Backend UI Flow

| Step | Backend | API | Effect |
|---------|---------|-----|---------|
| Design workflow + agent | 🟣 Quarkus | `/api/ui/*` | in QUAD DB (or shared Postgres) |
| Test/execution | 🟣 Quarkus | `/api/ui/workflows/{id}/execute` | QUAD WorkflowEngine |
| Promote to Kafka | 🟢 Kafka | `POST /api/v1/unified/workflows/from-quad` | Converts flat→nested DAG, stores in `workflow_definitions`, **starts immediately** |
| Status | 🟢 Kafka | `GET /api/v1/workflows/{executionId}` | Status polling via checkpoints |
| Agent registry | 🟢 Kafka | `GET/POST /api/v1/agents` | reads `agent_definitions` |

### UnifiedApiGateway (converts + persists + starts atomically)

`POST /api/v1/unified/workflows/from-quad` accepts the flat format `{name, nodes[], edges[]}`:
1. `WorkflowDefinitionBridge.convert()` → nested `DagNode[]`
2. `WorkflowDefinitionService.create()` → persists **in-process** into the shared `workflow_definitions` (no more HTTP round trip to Quarkus!)
3. Auto-registration of referenced agents via `ensureStub()`
4. `WorkflowStartService.start()` → starts the Kafka execution immediately
5. Response: `{converted, nodeCount, kafkaId, name, version, executionId, duplicate}`

### New REST Endpoints (Orchestrator)

| Endpoint | Method | Description |
|----------|---------|--------------|
| `/api/v1/agents` | GET | List of the Kafka `agent_definitions` (for fetchKafkaAgents) |
| `/api/v1/agents` | POST | Upsert of an agent definition |
| `/api/v1/unified/workflows/from-quad` | POST | Flat→Kafka DAG + persist + start (atomic) |
| `/api/v1/unified/agents/convert` | POST | Quarkus agent→WorkerConfig **and** upsert into `agent_definitions` |

### Security: Dev Without Token

`SecurityConfig.java` now has a toggle `app.security.enabled` (default `false`):
- **Off (dev)**: REST API open — the JWT filter still runs (TenantContext propagation), UI + kafkaApi.ts work without a token.
- **On (prod)**: `APP_SECURITY_ENABLED=true` → only the public whitelist + validated JWTs.

### Files

| File | Change |
|-------|----------|
| `agent-orchestrator/.../db/JdbcAgentDefinitionStore.java` | + `list()`, `upsert()`, `ensureStub()` |
| `agent-orchestrator/.../web/AgentDefinitionController.java` | NEW — GET/POST `/api/v1/agents` |
| `agent-orchestrator/.../bridge/web/UnifiedApiGateway.java` | REWRITE — in-process persist + auto-start + agent upsert |
| `agent-orchestrator/.../security/SecurityConfig.java` | + `app.security.enabled` Toggle |
| `agent-orchestrator/src/main/resources/application.yml` | + `app.security.enabled` |
| `quad-quarkus/.../resources/application.properties` | Flyway table isolated: `quad_schema_history`; env-capable DB config |
| `quad-quarkus/start-backend.sh` | Respects `QUARKUS_DATASOURCE_*` env vars |
| `quad-quarkus/quad-ui/vite.config.ts` | Proxy: `/api/v1`→8080, `/api`→8086 |
| `quad-quarkus/quad-ui/src/services/kafkaApi.ts` | + `fetchKafkaAgents`, `ConvertAndRunResult`, fix response typing |
| `quad-quarkus/quad-ui/src/components/WorkflowComposer.tsx` | Agent loading per backend; Kafka execute auto-starts |

### Verification

- `mvn clean install` → **BUILD SUCCESS** (5 modules, 84 tests)
- `npm run build` → Vite build OK
- `npm run test` → 38 pass / 3 pre-existing failures (AgentPlayground, JsonSchemaPopup)
- `npm run lint` → warnings only (no errors)

*As of: 2026-09-18 — Shared-DB mode + atomic Kafka promote implemented.*
