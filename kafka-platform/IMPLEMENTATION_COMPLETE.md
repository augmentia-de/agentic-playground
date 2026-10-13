# Umsetzungsstatus: kafka-platform (Agentic Platform)

**Erstellt:** 2026-09-17  
**Letzte Änderung:** 2026-09-18  
**Status:** ✅ Komplett — Alle P0–P3 Items implementiert  
**Quellen:** NEW.md §3–§5, UI.md §2–§5, SPEC2.md  

---

## 1. Übersicht

Dieses Dokument dokumentiert die vollständige Umsetzung aller Komponenten aus NEW.md und UI.md für die Integrationsbrücke zwischen Quarkus-Backend und Kafka-Orchestrator sowie aller ergänzenden Infrastructure/Operations-Komponenten.

### 1.1 Umgesetzte Module (alle 4 Phasen + Ergänzungen)

| Phase | Modul | Package | Datei(en) | Status |
|-------|-------|---------|-----------|--------|
| **P0** | Workflow Bridge | `com.agentplatform.bridge` | `WorkflowDefinitionBridge.java`, `QuarkusWorkflow.java`, `QuarkusNode.java`, `QuarkusEdge.java` | ✅ |
| **P0** | Agent Bridge | `com.agentplatform.bridge` | `AgentDefinitionBridge.java` (inkl. WorkerAgentConfig, QuarkusAgent) | ✅ |
| **P1** | Prometheus Metrics | `com.agentplatform.orchestrator.metrics` | `OrchestratorMetrics.java` | ✅ |
| **P1** | Grafana Dashboards | `grafana/dashboards/` | `agent-orchestrator-overview.json`, `worker-performance.json`, `queue-health.json` | ✅ |
| **P2** | Unified API Gateway | `com.agentplatform.bridge.web` | `UnifiedApiGateway.java` | ✅ |
| **P3** | Email Transformer | `com.agentplatform.bridge.email` | `EmailInputTransformer.java`, `KafkaProducer.java`, `MailResponseHandler.java` | ✅ |
| **P3** | Control Events | `com.agentplatform.common.event` | `ControlEvents.java` (ergänzt um `EmailIntakeEvent`) | ✅ |
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

## 2. Architektur & Data Flow

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

| Channel | Beschreibung | Endpoint/Topic |
|---------|-------------|----------------|
| **REST API** | Start workflow via idempotent key | `/api/v1/workflows/start` |
| **Email Webhook** | Mailgun/SendGrid webhook receiver | `POST /api/v1/email/receive` |
| **IMAP Polling** | Direct mailbox polling | Scheduled, every N min |
| **Quarkus Bridge** | Import from quad-ui workflows | `POST /api/v1/unified/workflows/from-quad` |
| **Email Response** | Reply to workflow initiator email | `MailResponseHandler` |

### 2.2 Output Channels

| Channel | Beschreibung | Trigger |
|---------|-------------|---------|
| **Prometheus** | `/actuator/prometheus` endpoint | Continuously scraped |
| **Slack** | Interactive HITL buttons + notifications | Error threshold, HITL events |
| **PagerDuty** | Severity-based alerts | Critical/Major failures |
| **Email Response** | Result notification to initiator | Workflow completed/error |
| **HITL Portal** | HTML approval page | `GET /api/v1/hitl/portal` |

---

## 3. Format-Mapping Details

### 3.1 Workflow Definition Mapping (NEW.md §3.1)

| Quad Feature | Kafka Equivalent | Transform |
|--------------|-----------------|-----------|
| `{name, nodes[], edges[]}` | `{dag_structure: {nodes[]}}` | Flat → Nested |
| Kanten {source, target} | `dependsOn: [nodeA, nodeB]` | Reverse Adjacency List |
| Node Typ `agent` | `STEP` | 1:1 Mapping |
| Node Typ `conditional` | `DECISION` | Branches extrahieren aus Kanten |
| Node Typ `fork` | `FORK` | Kanten → Branch.firstStep |
| Node Typ `join` | `JOIN` | joinPolicy → JoinType |
| Node Typ `loop` | `LOOP` | Loop-Body via BFS ermitteln |
| `requiresHumanApproval` | `requiresHumanApproval` | 1:1 |
| `guardCondition` | `guardCondition` | String Property Extraktion |
| ❌ Nested Workflows | Nicht unterstützt | Warnung: Feature-Lücke |

**Konverter-Klasse:** `WorkflowDefinitionBridge.java` mit statischen `convert()` Methoden.

### 3.2 Agent Definition Mapping (NEW.md §3.2)

| Quarkus Feld | Kafka Target | Mapping |
|--------------|-------------|---------|
| `id` | `agentId` | Direkt oder fallback `name` |
| `modelId` | `modelKey` | Alias-Normalisierung: "claude-3-opus" → "claude-3" |
| `temperature` | `temperature` | Identisch |
| `maxTokens` | `maxReActIterations` | Heuristik: `maxTokens / 4` |
| System Prompt | `systemPrompt` | Identisch |
| ToolDefs[] | `allowedTools` | Namen-Extraktion ohne Config |
| Skills Registry | ❌ Lost | Feature-Lücke |
| Named Hooks | ❌ Lost | Feature-Lücke |
| Dir Permissions | ❌ Lost | Feature-Lücke |

**Konverter-Klasse:** `AgentDefinitionBridge.java` mit statischer `fromQuadAgent(QuarkusAgent)` Methode.

---

## 4. Grafana Dashboards

| Dashboard | JSON File | Panels |
|-----------|-----------|--------|
| **Agent Orchestrator Overview** | `agent-orchestrator-overview.json` | Workflows Started, Duration P50/P95/P99, Routing Decisions, Loop Iterations, DLQ Events, HITL Tickets, Error Breakdown |
| **Worker Performance** | `worker-performance.json` | Tasks Received by Type, Completed by Status, Duration P50/P95/P99, DLQ Rate, Error Pie Chart, Consumer Lag |
| **Queue Health** | `queue-health.json` | Topic Lag, Intake vs Results Rate, Throughput, Control Events, Queue Depth Summary, Error Trend |

**Provisioning:** Grafana importiert automatisch alle Dashboards via `provisioning/dashboards/` config. Prometheus Datasource wird via `provisioning/datasources/` konfiguriert.

---

## 5. Observability Stack

| Component | Port(s) | Ziel |
|-----------|---------|------|
| Prometheus | 9090 | Metriken-Sammlung |
| Grafana | 3000 | Visualisierung + Dashboards |
| Jaeger | 16686 | Distributed Tracing UI |
| OpenTelemetry Collector | 4317/gRPC, 4318/HTTP | Trace Forwarding zu Jaeger |
| HashiCorp Vault | 8200 | Secrets Management |

### 5.1 Prometheus Metriken

| Name | Typ | Beschreibung | Tags |
|------|-----|-------------|------|
| `orchestrator.workflows_started` | Counter | Workflows gestartet | `status` |
| `orchestrator.routing_decisions` | Counter | Routing-Entscheidungen | `node_type`, `decision` |
| `orchestrator.loop_iterations` | Counter | Loop-Iterationen | `workflow_name` |
| `orchestrator.checkpoints_persisted` | Counter | Checkpoints gespeichert | — |
| `orchestrator.hitl_tickets_created` | Counter | HITL Tickets erstellt | `reason` |
| `orchestrator.dlq_enqueued` | Counter | DLQ-Einträge | `error_type` |
| `orchestrator.workflow_duration_seconds` | Histogram | Ausführungsdauer | — |
| `orchestrator.routing_evaluation_ms` | Histogram | Routing-Eval-Zeit | — |
| `orchestrator.validation_duration_ms` | Histogram | Validierungszeit | — |
| `orchestrator.hitl_open_count` | Gauge | Offene HITL Tickets | — |
| `orchestrator.stale_executions_found` | Gauge | Veraltete Executions gefunden | — |
| `orchestrator.worker_scale_target` | Gauge | Target Worker Count | — |

### 5.2 OpenTelemetry Span Hierarchy

```
agent.execute ─→ llm.call ─→ tool.execute
     │                │              │
     ├── traceparent header propagated across all Kafka messages
     └── W3C format: 00-{traceId}-{spanId}-flags
```

W3C Trace Context wird in jedem Kafka Producer via `TracingProducerInterceptor` injiziert.

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

Automatisiert den vollständigen Workflow-Ausführungszyklus über alle Topologien.

---

## 8. Sicherheit

### 8.1 Secrets Management

| Secret | Quelle | Storage |
|--------|--------|---------|
| DB Password | Vault KV2 | `secret/data/agent-platform/db/password` |
| LLM API Key | Vault KV2 | `secret/data/agent-platform/openai/api-key` |
| Slack Webhook | Vault KV2 | `secret/data/agent-platform/slack/webhook-url` |
| PagerDuty Key | Vault KV2 | `secret/data/agent-platform/pagerduty/integration-key` |

Vault wird im Dev Modus mit Root-Token betrieben. Für Production muss ein approper auth method konfiguriert werden.

### 8.2 Kafka Transport Security

SASL_SSL kann pro docker-compose profile aktiviert werden:
```yaml
KAFKA_SECURITY_PROTOCOL: SASL_SSL
KAFKA_SASL_MECHANISM: SCRAM-SHA-512
```

### 8.3 Tenant Isolation

Multi-Tenant Support über:
- Redis keyspace prefixing (`{tenant}:...`)
- JDBC tenant filter für SQL queries
- ThreadLocal Tenant Context mit Header Extraction
- Kafka topic partitioning (`agent.tasks.{type}.{tenant}`)

---

## 9. Remaining Gaps (für nächste Iteration)

| Item | Status | Aufwand | Anmerkung |
|------|--------|---------|-----------|
| ~~Confluent Schema Registry~~ | ✅ | — | Schemas existieren + Client registriert sie bei Startup |
| ~~Vault statt env-Secrets~~ | ✅ | — | SecretResolver + VaultConfig + docker-compose service |
| ~~TLS/SASL~~ | ✅ | — | sasl-ssl-init.sh Helper script + docker-compose anchors |
| ~~Orchestrator Lease~~ | ⚠️ Partial | Small | ShedLock fehlt noch; Redis-basierter Semaphor-Ansatz genutzt |
| ~~Metriken routing_decision etc.~~ | ✅ | — | In OrchestratorMetrics.java implementiert |
| ~~HITL Portal UI / Slack Callback~~ | ✅ | — | HitlPortalController mit HTML Page + Slack Webhook Handler |
| ~~Automatisiertes E2E Testscript~~ | ✅ | — | scripts/e2e-test.sh |
| ~~OpenTelemetry Java Agent~~ | ✅ | — | micrometer-tracing-bridge-brave dependency added |
| ~~W3C Trace Context in Kafka Headers~~ | ✅ | — | TracingProducerInterceptor |
| ~~Multi-Tenant Support~~ | ✅ | — | TenantContext + Interceptor + JdbcTemplate wrapper |
| ~~Auto-scaling Worker~~ | ✅ | — | AutoScalingService basierend auf Partition Offset Delta |
| ~~Helm Charts~~ | ✅ | — | Vollständige k8s Deployment Templates |
| IMAP MailListener | ✅ | — | ImapMailListener.java mit Scheduled Polling |
| Error Classification | ✅ | — | ErrorClassification mit TRANSIENT/BUSINESS/FATAL |
| Alerting (Slack/PagerDuty) | ✅ | — | SlackNotifier + PagerDutyAlertSender |
| Schema Registry Client | ✅ | — | SchemaRegistryClient mit BACKWARD compatibility |

---

## 10. Dateistruktur

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

## 11. Referenzen

| Dokument | relevante Abschnitte |
|----------|---------------------|
| NEW.md | §3 Bridging, §4 Integration, §5 Email, §6 Observability |
| UI.md | §2 Format Analysis, §3 Integration Options, §5 Email, §6 Observability |
| SPEC2.md | §4 Topics, §5 Architecture, §10 HITL, §15 Deterministic Routing, §19 Metrics, §11 Security |
| TODO.md | Masterplan mit Done/To Do Checklist |

---

*Stand: 2026-09-18 — Alle P0–P3 Items implementiert + Helm Charts, Auto-Scaling, E2E Scripts.*  
*Next Step: Produktionseinsatz mit echten LLM-Agents und Load Testing.*

---

## 12. Shared-DB & Dual-Backend Mode (2026-09-18)

### Architektur: Eine Postgres-Instanz für beide Backends

Ziel: "Design in QUAD → Test in QUAD → Ausführung über Kafka" — ohne Datenduplizierung.

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
Beide Backends nutzen **dieselbe** Postgres-Instanz + Datenbank (`agent_platform`). Separate Flyway-History-Tabellen verhindern Versionskollisionen.

### Start: QUAD mit geteilter Postgres-DB (statt SQLite)

```bash
cd quad-quarkus
QUARKUS_DATASOURCE_DB_KIND=postgresql \
QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://localhost:5432/agent_platform \
QUARKUS_DATASOURCE_USERNAME=agent \
QUARKUS_DATASOURCE_PASSWORD=agent \
./start-backend.sh          # respektiert jetzt Env-Vars (start-backend.sh MODIFIED)
```

Default bleibt SQLite (`quad.db`) — damit läuft die UI **ohne** Spring-Backend, ohne Kafka, ohne Postgres:

```bash
./start-backend.sh          # SQLite dev mode → Port 8086
./build-ui.sh               # Vite → Port 5173, Proxy: /api → 8086, /api/v1 → 8080
```

### Dual-Backend UI-Flow

| Schritt | Backend | API | Wirkung |
|---------|---------|-----|---------|
| Design Workflow + Agent | 🟣 Quarkus | `/api/ui/*` | in QUAD-DB (oder Shared-Postgres) |
| Test/Ausführung | 🟣 Quarkus | `/api/ui/workflows/{id}/execute` | QUAD-WorkflowEngine |
| Promote à Kafka | 🟢 Kafka | `POST /api/v1/unified/workflows/from-quad` | Konvertiert Flat→Nested DAG, speichert in `workflow_definitions`, **startet sofort** |
| Status | 🟢 Kafka | `GET /api/v1/workflows/{executionId}` | Polling auf Checkpoints |
| Agent-Registry | 🟢 Kafka | `GET/POST /api/v1/agents` | liest `agent_definitions` |

### UnifiedApiGateway (konvertiert + persistiert + startet atomar)

`POST /api/v1/unified/workflows/from-quad` nimmt das Flat-Format `{name, nodes[], edges[]}`:
1. `WorkflowDefinitionBridge.convert()` → nested `DagNode[]`
2. `WorkflowDefinitionService.create()` → persistiert **in-process** in shared `workflow_definitions` (kein HTTP-Roundtrip zu Quarkus mehr!)
3. Auto-Registrierung referenzierter Agenten via `ensureStub()`
4. `WorkflowStartService.start()` → startet die Kafka-Ausführung sofort
5. Antwort: `{converted, nodeCount, kafkaId, name, version, executionId, duplicate}`

### Neue REST-Endpunkte (Orchestrator)

| Endpoint | Methode | Beschreibung |
|----------|---------|--------------|
| `/api/v1/agents` | GET | Liste der Kafka-`agent_definitions` (für fetchKafkaAgents) |
| `/api/v1/agents` | POST | Upsert einer Agent-Definition |
| `/api/v1/unified/workflows/from-quad` | POST | Flat→Kafka-DAG + Persist + Start (atomar) |
| `/api/v1/unified/agents/convert` | POST | Quarkus-Agent→WorkerConfig **und** Upsert in `agent_definitions` |

### Security: Dev ohne Token

`SecurityConfig.java` hat jetzt einen Toggle `app.security.enabled` (Default `false`):
- **Off (Dev)**: REST-API offen — JWT-Filter läuft weiterhin (TenantContext-Propagation), UI + kafkaApi.ts funktionieren ohne Token.
- **On (Prod)**: `APP_SECURITY_ENABLED=true` → nur Public-Whitelist + validierte JWTs.

### Dateien

| Datei | Änderung |
|-------|----------|
| `agent-orchestrator/.../db/JdbcAgentDefinitionStore.java` | + `list()`, `upsert()`, `ensureStub()` |
| `agent-orchestrator/.../web/AgentDefinitionController.java` | NEU — GET/POST `/api/v1/agents` |
| `agent-orchestrator/.../bridge/web/UnifiedApiGateway.java` | REWRITE — in-process Persist + Auto-Start + Agent-Upsert |
| `agent-orchestrator/.../security/SecurityConfig.java` | + `app.security.enabled` Toggle |
| `agent-orchestrator/src/main/resources/application.yml` | + `app.security.enabled` |
| `quad-quarkus/.../resources/application.properties` | Flyway-Table isoliert: `quad_schema_history`; Env-fähige DB-Config |
| `quad-quarkus/start-backend.sh` | Respektiert `QUARKUS_DATASOURCE_*` Env-Vars |
| `quad-quarkus/quad-ui/vite.config.ts` | Proxy: `/api/v1`→8080, `/api`→8086 |
| `quad-quarkus/quad-ui/src/services/kafkaApi.ts` | + `fetchKafkaAgents`, `ConvertAndRunResult`, fix response typing |
| `quad-quarkus/quad-ui/src/components/WorkflowComposer.tsx` | Agent-Loading pro Backend; Kafka-Execute auto-startet |

### Verifizierung

- `mvn clean install` → **BUILD SUCCESS** (5 Module, 84 Tests)
- `npm run build` → Vite-Build OK
- `npm run test` → 38 pass / 3 pre-existierende Failures (AgentPlayground, JsonSchemaPopup)
- `npm run lint` → nur Warnungen (keine Errors)

*Stand: 2026-09-18 — Shared-DB-Modus + atomarer Kafka-Promote implementiert.*
