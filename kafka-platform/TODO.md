# TODO — Deterministic Kafka Workflow Platform (per SPEC2.md)

**Last modified:** 2026-09-19  
**Build Status:** ✅ `mvn clean verify` — 270 tests, of which 5 are key-gated (skipped without LLM key)  

Stack: **Spring Boot 4.0.8**, Java 21, Spring Kafka, PostgreSQL 16, Redis 7.2.  
Important: **no Spring AI / LLM in this project**. Real agents live in a separate service; this project stays deterministic and only provides the Kafka worker contract as well as mock agents.

## Module
- `agent-common` — shared domain model, DAG/routing core (pure), topics, events, AgentExecutor SPI
- `agent-orchestrator` — DAG engine, checkpoints, handlers, REST API (§16), Flyway, topic provisioning, seeding, alerting, auto-scaling, Vault
- `agent-worker` — deterministic mock agents, TaskProcessor via `agent.tasks.*`, result publisher via `agent.execution.results`

---

## ✅ Completed

### Core Platform
- [x] Create **TODO.md** as master plan
- [x] **Maven skeleton** (parent + common + orchestrator + worker), Boot 4.0.8, Java 21
- [x] **Topics** from §4.1 as constants + schema subjects §4.2
- [x] **Domain model** (NodeType STEP/DECISION/LOOP/FORK/JOIN/END, statuses, DAG, events, CheckpointState)
- [x] **ExpressionEvaluator** for guards, DECISION conditions and LOOP exit (pure, deterministic)
- [x] **RoutingEvaluator** (pure/deterministic §15): edgeCompleted, DECISION branch gating, guards, JOIN types, pruning/skip cascade
- [x] **WorkflowDefinitionValidator** (§17.1): node vocabulary, acyclicity, DECISION→LOOP→defaultTarget, FORK/JOIN pairing
- [x] **Golden/determinism tests** (§17.2): all node types as scenarios, replay=identical
- [x] **DB schema (Flyway)**: agent_definitions, workflow_definitions, workflow_executions, workflow_checkpoints, hitl_tickets, dlq_messages
- [x] **Event mapping REST↔Kafka** (§16.1): start→intake, cancel→cancellation, hitl→hitl, worker results→results
- [x] **Orchestrator handlers**: Intake/DagAdvance, LoopIteration, SweepReaper, Cancellation, HitlResume
- [x] **REST API** (§16): `/workflows/start` (idempotent), status, checkpoints, cancel; definitions; hitl
- [x] **Mock agents** (deterministic): generic hash-based executor for all agent types
- [x] **AgentExecutor seam**: interface in `agent-common`, mock in the worker
- [x] **Topic provisioner** (AdminClient) + consumer groups / single writer
- [x] **docker-compose** (Kafka KRaft, Postgres 16, Redis 7.2) + README run instructions
- [x] **Build & tests** `mvn verify` green
- [x] **Manual E2E run via Kafka**: reference, XOR, N_OF, guard skip, retryable/fatal/needs-HITL, cancel

### Bridge Layer (NEW.md §3, UI.md §2–§3)
- [x] **WorkflowDefinitionBridge** — Quad flat `{nodes[], edges[]}` → Kafka nested DAG
- [x] **AgentDefinitionBridge** — Quarkus rich config → Kafka reduced config
- [x] **UnifiedApiGateway** — `/api/v1/unified/*` router + Quarkus proxy
- [x] **ErrorClassifier** — TRANSIENT/FATAL/BUSINESS taxonomy (§9.5)

### Email Input (NEW.md §5, UI.md §5)
- [x] **EmailInputTransformer** — SMTP/Webhook → Kafka intake
- [x] **ImapMailListener** — IMAP mailbox polling alternative
- [x] **MailResponseHandler** — result events → response emails
- [x] **EmailInitiatorStore** — Redis email→workflow tracking

### Observability (NEW.md §6, UI.md §6)
- [x] **OrchestratorMetrics** — Prometheus counter/timer/gauge beans
- [x] **Grafana dashboards** — 3 JSON dashboard files + provisioning
- [x] **Prometheus config** — prometheus.yml with scrape target
- [x] **OTel/Jaeger config** — docker-compose extensions + otel-config.yaml
- [x] **OpenTelemetry Java Agent wiring** — micrometer-tracing-bridge-brave dependency
- [x] **W3C Trace Context Propagation** — TracingProducerInterceptor
- [x] **SchemaRegistryClient** — JSON schema registration (BACKWARD compatible)
- [x] **Confluent Schema Registry JSON Schemas** — 3 JSON schema files + startup registration

### Security & Alerting
- [x] **SecretResolver** — HashiCorp Vault KV v2 client with cache
- [x] **VaultConfig** — Vault integration wiring
- [x] **SlackNotifier** — interactive HITL alerts with buttons
- [x] **PagerDutyAlertSender** — Critical/Major/Minor Events API v2
- [x] **Resilience4j Circuit Breaker** — llm-provider + worker-dispatch breaker

### Frontend Integration
- [x] **Frontend integration** — kafkaApi.ts + backend selector in WorkflowComposer.tsx
- [x] **Bridge compatibility** — WorkflowDefinitionBridge + QuadUiPayloadCompatibilityTest
- [x] **Gateway on Jackson 3** — `/from-quad` returns HTTP 202 instead of 500
- [x] **Ack mode contract** — `ListenerAckModeContractTest`
- [x] **LLM orchestrator E2E** — `LlmDrivenWorkflowE2ETest` (4 tests, key-gated)

  Checks what a fixed-data test does not cover: whether a workflow's condition
  sees the field that the LLM actually returns. Without `OPENAI_API_KEY`, the
  four tests are skipped, not written green with fabricated data.

  Previously there was a checkmark here without substance: the path from the UI to the
  Kafka backend failed on `R6: no END node`. The canvas offers no
  END node, but the orchestrator requires one when saving. The bridge
  now adds it synthetically at the leaves of the graph. Without this fix,
  **every** UI workflow was rejected.

### Multi-Tenant
- [x] **Multi-Tenant Support** — TenantContext + Interceptor + TenantAwareJdbcTemplate
- [x] **Tenant migration** — R__add_tenant_columns.sql with tenant_id columns
- [x] **Redis Keyspace Prefixing** — EmailInitiatorStore + PrefixCache

### Infrastructure & Deployment
- [x] **Helm Charts** — Full k8s deployment templates (Chart.yaml, values.yaml, 5 Templates)
- [x] **Auto-scaling worker** — AutoScalingService based on partition offset delta
- [x] **HITL Portal UI** — HitlPortalController with HTML page + Slack callback
- [x] **Automated E2E test script** — scripts/e2e-test.sh
- [x] **GdprController** — GDPR export/delete/anonymize APIs
- [x] **ConfigUpdateConsumer** — agent.config.updates topic listener

---

## ⏳ Remaining (next iteration)

| Item | Priority | Effort | Note |
|------|-----------|---------|-----------|
| ~~Orchestrator Lease~~ | ~~High~~ | ~~Medium~~ | ✅ `OrchestratorLeaseService.java` with JDBC ShedLock pattern |
| ~~Production Vault Setup~~ | ~~High~~ | ~~Small~~ | ✅ `VaultConfig.java` with token/AppRole/K8s auth + pre-fill script |
| **Kafka SASL_SSL Production** | High | Medium | sasl-ssl-init.sh generates self-signed certs; production CA + mTLS required |
| ~~JWT/OIDC API Security~~ | ~~Low~~ | ~~Medium~~ | ✅ `JwtAuthenticationFilter.java` + `SecurityConfig.java` with bearer token + tenant propagation |
| **Real LLM Workers** | Medium | Large | make ai-agent-service production-ready (agent def mapping, real LLM E2E run) |
| **CDC / Debezium Integration** | Low | Large | PostgreSQL CDC for dynamic config updates via `agent.config.updates` |
| **Schema Evolution Tests** | Low | Small | JSON Schema BACKWARD/FORWARD compatibility testing |
| **Kubernetes Operator** | Low | Very Large | Custom controller for auto-scale, failover, schema registry management |

---

## 🧭 Item 2 (architecturally clarified, no code) — Dynamic Task Topics & agent→topic Routing

**Situation (verified against the code):**

- The orchestrator publishes task events **dynamically per agent type** via
  `TopicNames.tasks(agentType)` → `agent.tasks.{agent_type}` (agentType is
  lowercased; see `TaskDispatcher.java:129` and
  `agent-common/.../kafka/TopicNames.java`). Any new agentType
  (e.g. `coder`) thus creates an ad-hoc topic `agent.tasks.coder`.
- The QUAD worker previously consumed a **static topic list**:
  `worker.quad.task-topics` (default
  `agent.tasks.researcher,agent.tasks.planner,agent.tasks.coder`),
  wired in `WorkflowTaskListener` via
  `@KafkaListener(topics = "#{'${worker.quad.task-topics}'.split(',')}")`.
- **Resulting gap:** If the orchestrator dispatches an agentType that is not in
  this list, the worker does not listen to that topic — the message stays in the
  topic (or only lands, if the broker has `auto.create.topics.enable=true`,
  in the created topic) and is never processed. In addition, the list must be
  extended manually for every new agent manifest.

**Routing options to close the gap (evaluated, not implemented):**

1. **Topics: wildcard listener (minimally invasive, recommended).**
   `@KafkaListener(topicPattern = "agent.tasks.*")` replaces the static list.
   The worker then automatically consumes every agentType without a config change;
   topic creation continues to happen via the broker (auto-create) or the setup.
   Risk: receiving foreign `agent.tasks.*` topics → keep an agentType whitelist in the
   worker (`worker.quad.agents`).

2. **Topics: dynamic rebinding (no wildcard, fully controlled).**
   A new agentType becomes known via a lifecycle event (e.g. `agent.config.updates`);
   the worker adds the topic at runtime via
   `KafkaListenerEndpointRegistry`/`ConsumerSeekAware` and then sets the offset
   to earliest. More effort, but without wildcard side effects.

3. **Payload routing to a single-sink topic (simplest, most stable rework).**
   The orchestrator publishes all tasks to ONE topic (e.g. `agent.tasks`) with the
   agentType in the payload; the worker routes internally to the matching agents
   (`executeStructured` already selects via `task.agentType()` anyway). No
   topic management, no wildcards. Disadvantage: partitions mix all
   agent types, consumer group loads cannot be separated per type.

4. **Agent config from the DB also closes the root of the problem.**
   Since *Item 1*, the worker reads the AgentConfig from `agent_definitions`
   (property `worker.quad.agent-source=db`). A new entry there (agent_type
   `coder`) is thus configured immediately — only the topic side is still
   missing (options 1–3) to make dispatch fully dynamic.

*Recommended path: combination of O1 + Item 1 DB config.*

---

## 🧭 Item 2 — Status: ✅ Implemented (O1 wildcard listener + unified contract)

**Decided & implemented (2026-09-19):** Orchestrator and QUAD worker share
**one canonical event contract** `TaskEvent`/`ResultEvent` (in `agent-common`,
package `com.agentplatform.common.event`, Jackson 3 serializer `EventJson`; field names
compatible with Jackson 2 readers via `com.fasterxml.jackson.annotation.JsonProperty` —
`timeout_ms`, `result_data`).

- `WorkflowTaskListener` now uses `@KafkaListener(topicPattern = "${worker.quad.task-topic-pattern:agent\\.tasks\\..*}")`.
  Property `worker.quad.task-topics` (static list) → `worker.quad.task-topic-pattern`
  (wildcard, env override `WORKER_TASK_TOPIC_PATTERN`). Explicitly named worker topics
  (`agent.tasks.researcher` etc.) are therefore no longer necessary.
- Legacy `agent-worker` (deterministic mock agents) no longer listens on `agent.tasks.*`:
  default pattern → `legacy\.agent\.tasks\..*` (`agent-worker/.../application.yml`).
  Contributions from the supposed legacy worker are therefore no longer consumed.
- `docker-compose.yml`: legacy `worker` service commented out (explanatory comment).
- Orchestrator migrated: `TaskDispatcher.buildTaskEvent` → `TaskEvent`,
  `WorkflowEngine.onResult`/`DagTopicListener`/`MailResponseHandler` → `ResultEvent`.
- `KafkaTopicE2ETest` builds its container manually (`ContainerProperties(TASK_TOPIC)`)
  and thus remains unaffected by the annotation wildcard — still green.

---

## Build & Run

```bash
# Clean build + all tests
cd kafka-platform && mvn clean verify

# Development stack
docker compose up --build

# Full stack with observability
docker compose --profile full up --build

# Helm chart deploy
helm install agentic ./helm/ --set autoscaling.enabled=true

# E2E integration test
./scripts/e2e-test.sh
```

---

*As of: 2026-10-02 — build green, 270 tests, 5 of them key-gated (no LLM key needed). The line from 2026-09-19 named 101 tests, the one from 2026-09-29 244; the characterization, listener and configuration tests were added. In addition, eleven blockers that prevented operation or startup have been fixed — see README, section "Verified State".*  
*Item 1 (worker AgentConfig from shared DB `agent_definitions` with YAML fallback, property-gated) + Item 3 (worker E2E test: real LLM key `OPENAI_API_KEY`, otherwise deterministic mock fallback) are implemented.*  
*Level 3 (DB E2E) ✅: `JdbcAgentConfigSourceE2ETest` (4 tests) runs a real PostgreSQL 16 (test container, image `postgres:16-alpine`) with the real `V1__init.sql` schema — verifies RowMapper/JSONB/defaults and DB-first precedence over YAML in the factory.*  
*Level 4 (Kafka topic end-to-end) ✅: `KafkaTopicE2ETest` (2 tests) starts a real Kafka broker in-JVM (spring-kafka-test `EmbeddedKafkaKraftBroker`, offline, no Docker) — verifies the complete topic chain orchestrator producer → `agent.tasks.researcher` → `WorkflowTaskListener` (real batch container + MANUAL ack) → TaskExecutor → `agent.execution.results`, plus the DLQ path malformed JSON → `agent.execution.dlq`.*  
*Item 2 (O1) ✅ (2026-09-19): wildcard listener `worker.quad.task-topic-pattern` (default `agent\\.tasks\\..*`), unified canonical contract `TaskEvent`/`ResultEvent` in `agent-common` for orchestrator + QUAD worker (Jackson 3/Jackson 2 wire format compatible), legacy `agent-worker` banished to `legacy\.agent\.tasks\..*` and removed from docker-compose.*  
*Next step: production Vault setup + real LLM worker integration.*

## 🔧 Audit Fixes (2026-09-18)

| Issue | Status | Fix |
|-------|--------|-----|
| `@RequestParam` on constructor param | ✅ Fixed | Changed to `@Value("${quarkus.base-url:...}")` |
| JWT secret missing default | ✅ Fixed | Added safe base64 fallback |
| SecurityConfig/JWT public paths mismatch | ✅ Fixed | `/api/v1/auth/**` added to JwtAuthenticationFilter PUBLIC_PATHS |
| Port 9090 conflict (prometheus vs orchestrator) | ✅ Fixed | Changed orchestrator metrics port to 18090 |
| Duplicate JJWT deps in pom.xml | ✅ Fixed | Removed duplicate entries |
| ApiError unused import in kafkaApi.ts | ✅ Fixed | Replaced with `Record<string, unknown>` |
| UnifiedApiGateway convertAndSaveKafka stub | ✅ Fixed | Now actually calls Kafka REST API to persist converted DAG |
| Missing `AgentDefinitionBridge` import | ✅ Fixed | Added full import + inner class reference |
| `Method` class not found | ✅ Fixed | Replaced all `Method.X` with proper Spring HTTP constants |
| WebClient body() JsonNode incompatibility | ✅ Fixed | Use `objectMapper.writeValueAsString()` for JSON serialization |
| `doQuarkusProxyPost` return type mismatch | ✅ Fixed | Explicit cast `(ResponseEntity<?>)` |

## 📊 System Audit Summary — 2026-09-18

### Frontend → Backend Connectivity
```
┌─────────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│  quad-ui/src/   │     │  UnifiedApiGateway   │     │  Backend Target  │
│                 │     │                      │     │                  │
│ api.ts          ├────▶│ (auto-detect)        ├────▶│ Quarkus :8086    │
│ • /api/ui/*     │     │                      │     │                  │
│                 │     │                      │     │                  │
│ kafkaApi.ts     ├────▶│ /unified/*           ├────▶│ Kafka :8080      │
│ • /api/v1/*     │     │ /quarkus/* proxy     │     │                  │
└─────────────────┘     └──────────────────────┘     └──────────────────┘
```

**Backend Switch:** In WorkflowComposer.tsx, dropdown toggles between 'quarkus' and 'kafka'. When 'kafka' is selected, `handleExecute()` calls `kafkaApi.convertQuadToKafka()` which sends POST to `/api/v1/unified/workflows/from-quad`. The gateway then converts to Kafka nested DAG format and persists via native REST API.

**Data Flow when backend === 'kafka':**
1. User clicks Execute → `handleExecute()` triggers
2. Converts current nodes/edges to Kafka DAG nodes
3. Calls `convertQuadToKafka(name, nodeList, edgeList)` → POST `/api/v1/unified/workflows/from-quad`
4. `UnifiedApiGateway` receives request, sets `X-Backend` to `"auto"`
5. Since not "quarkus", enters conversion branch
6. Parses QuarkusWorkflow from JSON payload
7. Calls `WorkflowDefinitionBridge.convert()` → List of DagNode
8. Builds Kafka-compatible DAG JSON payload
9. Sends POST to `/api/v1/workflow-definitions` (native Kafka endpoint)
10. Returns `{saved, kafkaId, convertedNodes, originalName}` to frontend
11. Frontend displays success message

**Data Flow when backend === 'quarkus':**
1. User clicks Execute → `handleExecute()` triggers
2. Saves workflow via `saveWorkflow()` → PUT/POST `/api/ui/workflows`
3. Executes via `runWorkflowById(id)` → GET `/api/ui/workflows/{id}/runs`
4. Polls status until COMPLETED/FAILED
5. All calls go directly through `api.ts` → Quarkus backend

### Unified API Gateway Endpoints
| Endpoint | Method | Purpose | Targets |
|----------|--------|---------|---------|
| `/api/v1/unified/workflows/from-quad` | POST | Convert+save Quad flat DAG | Native Kafka REST or Quarkus (via header) |
| `/api/v1/unified/agents/convert` | POST | Convert rich AgentDef → WorkerConfig | Local Java conversion |
| `/api/v1/unified/metrics` | GET | Merge Prometheus metrics from both | Both backends |
| `/api/v1/unified/quarkus/{path}` | GET/POST | Transparent proxy to Quarkus | Quarkus :8086 |
| `/api/v1/unified/quarkus/stream/{path}` | GET | SSE stream proxy to Quarkus | Quarkus SSE endpoints |

### Docker Profile Activation Matrix
| Profile | Services Active | Kafka Mode | TLS? | Observability? |
|---------|----------------|------------|------|----------------|
| **dev** (default) | kafka-dev, postgres, redis, vault, prometheus, grafana, orchestrator, worker, quad-worker | PLAINTEXT | ❌ | Basic |
| **tls** | kafka-tls, orchestrator (with tls profile), worker (with tls profile) | SASL_SSL + mTLS | ✅ | ✅ Jaeger+OTel |
| **prod** | Same as tls + replicas configured | SASL_SSL + mTLS | ✅ | ✅ Full |

### Build Statistics
- **Total Modules:** 5 (parent, agent-common, agent-orchestrator, agent-worker, quad-workflow-worker); the end-to-end tests live in their own project `../integration-tests` (`mvn -o test -Pe2e` from there)
- **Total Tests:** 270 (5 key-gated, skipped without `OPENAI_API_KEY`)
- **Java Files:** 100+ across all modules
- **Lines of Code:** ~10k+ (estimated)
- **Maven Build Time:** ~8s average

### Remaining Critical Items
| Item | Priority | Status |
|------|----------|--------|
| CDC / Debezium Integration | 🟢 Low | Not started |
| Schema Evolution Tests | 🟢 Low | Not started |
| Kubernetes Operator | 🟢 Low | Not started |

## 🔄 Shared-DB / Dual-Backend Sprint (2026-09-18)

| Item | Status |
|------|--------|
| Both backends on an identical Postgres instance (`agent_platform`) | ✅ |
| Flyway history isolated (QUAD `quad_schema_history` vs orchestrator `flyway_schema_history`) | ✅ |
| `GET/POST /api/v1/agents` (agent registry for Kafka) | ✅ |
| UnifiedApiGateway: **atomic** convert+persist+start (no HTTP round trip to Quarkus) | ✅ |
| Auto-registration of referenced agents (`ensureStub`) | ✅ |
| `fetchKafkaAgents()` in kafkaApi.ts | ✅ |
| WorkflowComposer: agent loading per backend (Quarkus vs Kafka) | ✅ |
| Kafka execute starts workflow immediately (save gap removed) | ✅ |
| Security toggle `app.security.enabled` (dev: open, prod: JWT) | ✅ |
| Vite proxy `/api/v1`→8080, `/api`→8086 | ✅ |
| start-backend.sh respects `QUARKUS_DATASOURCE_*` | ✅ |

**UI without Spring backend:** `./start-backend.sh` (SQLite) + `npm run dev` → everything runs via Quarkus.
**Shared-DB mode:** QUARKUS_DATASOURCE_* set to PG → both backends use the same DB.
