# TODO — Deterministic Kafka Workflow Platform (nach SPEC2.md)

**Letzte Änderung:** 2026-09-19  
**Build Status:** ✅ `mvn clean verify` — 270 Tests, davon 5 key-gated (übersprungen ohne LLM-Key)  

Stack: **Spring Boot 4.0.8**, Java 21, Spring Kafka, PostgreSQL 16, Redis 7.2.  
Wichtig: **kein Spring AI / LLM in diesem Projekt**. Echte Agents leben in einem separaten Service; dieses Projekt bleibt deterministisch und stellt nur den Kafka-Worker-Contract sowie Mock-Agents bereit.

## Module
- `agent-common` — geteiltes Domänenmodell, DAG/Routing-Kern (rein), Topics, Events, AgentExecutor-SPI
- `agent-orchestrator` — DAG-Engine, Checkpoints, Handler, REST-API (§16), Flyway, Topic-Provisioning, Seeding, Alerting, Auto-Scaling, Vault
- `agent-worker` — deterministic Mock-Agents, TaskProcessor über `agent.tasks.*`, Result-Publisher über `agent.execution.results`

---

## ✅ Completed

### Core Platform
- [x] **TODO.md** als Masterplan anlegen
- [x] **Maven-Skeleton** (parent + common + orchestrator + worker), Boot 4.0.8, Java 21
- [x] **Topics** aus §4.1 als Konstanten + Schema-Subjects §4.2
- [x] **Domänenmodell** (NodeType STEP/DECISION/LOOP/FORK/JOIN/END, Statuses, DAG, Events, CheckpointState)
- [x] **ExpressionEvaluator** für Guards, DECISION-Bedingungen und LOOP-Exit (pur, deterministisch)
- [x] **RoutingEvaluator** (rein/deterministisch §15): edgeCompleted, DECISION-Branch-Gating, Guards, JOIN-Typen, Pruning/Skip-Kaskade
- [x] **WorkflowDefinitionValidator** (§17.1): Node-Vokabular, Azyklizität, DECISION→LOOP→defaultTarget, FORK/JOIN-Paarung
- [x] **Golden-/Determinismus-Tests** (§17.2): alle Node-Typen als Szenarien, Replay=identisch
- [x] **DB-Schema (Flyway)**: agent_definitions, workflow_definitions, workflow_executions, workflow_checkpoints, hitl_tickets, dlq_messages
- [x] **Event-Mapping REST↔Kafka** (§16.1): start→intake, cancel→cancellation, hitl→hitl, worker results→results
- [x] **Orchestrator-Handler**: Intake/DagAdvance, LoopIteration, SweepReaper, Cancellation, HitlResume
- [x] **REST-API** (§16): `/workflows/start` (idempotent), status, checkpoints, cancel; definitions; hitl
- [x] **Mock-Agents** (deterministisch): generischer hash-basierter Executor für alle Agent-Typen
- [x] **AgentExecutor-Seam**: Interface in `agent-common`, Mock im Worker
- [x] **Topic-Provisioner** (AdminClient) + Consumer-Gruppen / Single-Writer
- [x] **docker-compose** (Kafka KRaft, Postgres 16, Redis 7.2) + README-Run-Anleitung
- [x] **Build & Tests** `mvn verify` grün
- [x] **Manueller E2E-Lauf über Kafka**: reference, XOR, N_OF, Guard-Skip, Retryable/Fatal/Needs-HITL, Cancel

### Bridge Layer (NEW.md §3, UI.md §2–§3)
- [x] **WorkflowDefinitionBridge** — Quad flat `{nodes[], edges[]}` → Kafka nested DAG
- [x] **AgentDefinitionBridge** — Quarkus rich config → Kafka reduced config
- [x] **UnifiedApiGateway** — `/api/v1/unified/*` Router + Quarkus Proxy
- [x] **ErrorClassifier** — TRANSIENT/FATAL/BUSINESS Taxonomie (§9.5)

### Email Input (NEW.md §5, UI.md §5)
- [x] **EmailInputTransformer** — SMTP/Webhook → Kafka intake
- [x] **ImapMailListener** — IMAP mailbox polling alternative
- [x] **MailResponseHandler** — Result events → Response Emails
- [x] **EmailInitiatorStore** — Redis email→workflow tracking

### Observability (NEW.md §6, UI.md §6)
- [x] **OrchestratorMetrics** — Prometheus Counter/Timer/Gauge Beans
- [x] **Grafana Dashboards** — 3 JSON Dashboard Files + Provisioning
- [x] **Prometheus Config** — prometheus.yml mit scrape target
- [x] **OTel/Jaeger Config** — docker-compose extensions + otel-config.yaml
- [x] **OpenTelemetry Java Agent wiring** — micrometer-tracing-bridge-brave dependency
- [x] **W3C Trace Context Propagation** — TracingProducerInterceptor
- [x] **SchemaRegistryClient** — JSON Schema Registration (BACKWARD compatible)
- [x] **Confluent Schema Registry JSON Schemas** — 3 JSON Schema Files + startup registration

### Security & Alerting
- [x] **SecretResolver** — HashiCorp Vault KV v2 Client mit Cache
- [x] **VaultConfig** — Vault Integration Wiring
- [x] **SlackNotifier** — Interaktive HITL Alerts mit Buttons
- [x] **PagerDutyAlertSender** — Critical/Major/Minor Events API v2
- [x] **Resilience4j Circuit Breaker** — llm-provider + worker-dispatch Breaker

### Frontend Integration
- [x] **Frontend Integration** — kafkaApi.ts + Backend Selector in WorkflowComposer.tsx
- [x] **Bridge-Kompatibilität** — WorkflowDefinitionBridge + QuadUiPayloadCompatibilityTest
- [x] **Gateway auf Jackson 3** — `/from-quad` liefert HTTP 202 statt 500
- [x] **Ack-Mode-Vertrag** — `ListenerAckModeContractTest`
- [x] **LLM-Orchestrator-E2E** — `LlmDrivenWorkflowE2ETest` (4 Tests, key-gated)

  Prüft, was ein Festdatentest nicht abdeckt: ob die Bedingung eines Workflows
  das Feld sieht, das das LLM wirklich liefert. Ohne `OPENAI_API_KEY` werden die
  vier Tests übersprungen, nicht mit erfundenen Daten grün geschrieben.

  Vorher stand hier ein Häkchen ohne Wirklichkeit: der Weg aus der Oberfläche zum
  Kafka-Backend scheiterte an `R6: no END node`. Der Canvas bietet keinen
  END-Knoten an, der Orchestrator verlangt beim Speichern aber einen. Die Bridge
  ergänzt ihn jetzt synthetisch an den Blättern des Graphen. Ohne diesen Fix war
  **jeder** UI-Workflow abgewiesen.

### Multi-Tenant
- [x] **Multi-Tenant Support** — TenantContext + Interceptor + TenantAwareJdbcTemplate
- [x] **Tenant Migration** — R__add_tenant_columns.sql mit tenant_id Spalten
- [x] **Redis Keyspace Prefixing** — EmailInitiatorStore + PrefixCache

### Infrastructure & Deployment
- [x] **Helm Charts** — Full k8s deployment templates (Chart.yaml, values.yaml, 5 Templates)
- [x] **Auto-scaling Worker** — AutoScalingService basierend auf Partition Offset Delta
- [x] **HITL Portal UI** — HitlPortalController mit HTML Page + Slack Callback
- [x] **Automatisiertes E2E Testscript** — scripts/e2e-test.sh
- [x] **GdprController** — GDPR export/delete/anonymize APIs
- [x] **ConfigUpdateConsumer** — agent.config.updates Topic Listener

---

## ⏳ Remaining (next iteration)

| Item | Priorität | Aufwand | Anmerkung |
|------|-----------|---------|-----------|
| ~~Orchestrator Lease~~ | ~~High~~ | ~~Medium~~ | ✅ `OrchestratorLeaseService.java` mit JDBC ShedLock Pattern |
| ~~Production Vault Setup~~ | ~~High~~ | ~~Small~~ | ✅ `VaultConfig.java` mit Token/AppRole/K8s Auth + Pre-Fill Script |
| **Kafka SASL_SSL Production** | High | Medium | sasl-ssl-init.sh erzeugt selbstsignierte Certs; Production CA + mTLS erforderlich |
| ~~JWT/OIDC API Security~~ | ~~Low~~ | ~~Medium~~ | ✅ `JwtAuthenticationFilter.java` + `SecurityConfig.java` mit Bearer-Token + tenant propagation |
| **Real LLM Workers** | Medium | Large | ai-agent-service produktionsreif machen (Agent-Def Mapping, echter LLM-E2E-Lauf) |
| **CDC / Debezium Integration** | Low | Large | PostgreSQL CDC für dynamische Config Updates via `agent.config.updates` |
| **Schema Evolution Tests** | Low | Small | JSON Schema BACKWARD/FORWARD compatibility testing |
| **Kubernetes Operator** | Low | Very Large | Custom controller für auto-scale, failover, schema registry management |

---

## 🧭 Item 2 (architektonisch geklärt, kein Code) — Dynamische Task-Topics & agent→topic-Routing

**Sachlage (verifiziert am Code):**

- Der Orchestrator publiziert Task-Events **dynamisch pro Agent-Typ** über
  `TopicNames.tasks(agentType)` → `agent.tasks.{agent_type}` (agentType wird
  kleingeschrieben; siehe `TaskDispatcher.java:129` und
  `agent-common/.../kafka/TopicNames.java`). Ein beliebiger neuer agentType
  (z.B. `coder`) erzeugt damit ad hoc ein Topic `agent.tasks.coder`.
- Der QUAD-Worker konsumierte bislang eine **statische Topic-Liste**:
  `worker.quad.task-topics` (Default
  `agent.tasks.researcher,agent.tasks.planner,agent.tasks.coder`),
  verdrahtet im `WorkflowTaskListener` über
  `@KafkaListener(topics = "#{'${worker.quad.task-topics}'.split(',')}")`.
- **Folge-Gap:** Wird vom Orchestrator ein agentType dispatched, der nicht in
  dieser Liste steht, hört der Worker das Topic nicht — die Nachricht hängt im
  Topic (bzw. landet nur, wenn der Broker `auto.create.topics.enable=true` hat,
  im erzeugten Topic) und wird nie verarbeitet. Zusätzlich muss die Liste bei
  jedem neuen Agent-Manifest manuell nachgezogen werden.

**Routing-Optionen zur Schließung des Gaps (bewertet, nicht implementiert):**

1. **Topics: Wildcard-Listener (minimal-invasiv, empfohlen).**
   `@KafkaListener(topicPattern = "agent.tasks.*")` ersetzt die statische Liste.
   Der Worker konsumiert damit automatisch jeden agentType ohne Konfig-Änderung;
   Topic-Erzeugung erfolgt weiterhin über den Broker (Auto-Create) oder das Setup.
   Risiko: Empfang von fremden `agent.tasks.*`-Topics → agentType-Whitelist im
   Worker behalten (`worker.quad.agents`).

2. **Topics: Dynamische Neubindung (kein Wildcard, voll kontrolliert).**
   Neuer agentType wird per Lebenszyklus-Event (z.B. `agent.config.updates`)
   bekannt; der Worker ergänzt das Topic zur Laufzeit über
   `KafkaListenerEndpointRegistry`/`ConsumerSeekAware` und setzt dann den Offset
   auf earliest. Aufwändiger, dafür ohne Wildcard-Nebenwirkungen.

3. **Payload-Routing auf ein Single-Sink-Topic (einfachster, stabilster Umbau).**
   Orchestrator publiziert alle Tasks auf EIN Topic (z.B. `agent.tasks`) mit dem
   agentType im Payload; der Worker routet intern zu den passenden Agents
   (`executeStructured` wählt ohnehin schon per `task.agentType()`). Kein
   Topic-Management, keine Wildcards. Nachteil: Partitionen mischen alle
   Agent-Typen, Consumer-Group-Lasten sind nicht pro Typ trennbar.

4. **Agent-Config aus DB schließt die Wurzel des Problems mit.**
   Seit *Item 1* liest der Worker die AgentConfig aus `agent_definitions`
   (Property `worker.quad.agent-source=db`). Ein neuer Eintrag dort (agent_type
   `coder`) ist damit sofort konfiguriert — es fehlt nur noch die Topic-Seite
   (Optionen 1–3), um den Dispatch vollständig dynamisch zu machen.

*Empfohlener Pfad: Kombination O1 + Item-1-DB-Config.*

---

## 🧭 Item 2 — Status: ✅ Implementiert (O1 Wildcard-Listener + einheitlicher Contract)

**Beschlossen & umgesetzt (2026-09-19):** Orchestrator und QUAD-Worker teilen sich
**einen kanonischen Event-Contract** `TaskEvent`/`ResultEvent` (in `agent-common`,
Paket `com.agentplatform.common.event`, Jackson-3-Serializer `EventJson`; Feldnamen
kompatibel zu Jackson-2-Readern via `com.fasterxml.jackson.annotation.JsonProperty` —
`timeout_ms`, `result_data`).

- `WorkflowTaskListener` nutzt jetzt `@KafkaListener(topicPattern = "${worker.quad.task-topic-pattern:agent\\.tasks\\..*}")`.
  Property `worker.quad.task-topics` (statische Liste) → `worker.quad.task-topic-pattern`
  (Wildcard, Env-Override `WORKER_TASK_TOPIC_PATTERN`). Explizit genannte Worker-Topics
  (`agent.tasks.researcher` usw.) sind damit nicht mehr nötig.
- Legacy `agent-worker` (deterministic Mock-Agents) lauscht nicht mehr auf `agent.tasks.*`:
  Default-Pattern → `legacy\.agent\.tasks\..*` (`agent-worker/.../application.yml`).
  Beiträge des vermeintlichen Legacy-Workers werden damit nicht mehr konsumiert.
- `docker-compose.yml`: Legacy-`worker`-Service auskommentiert (Begründungskommentar).
- Orchestrator migriert: `TaskDispatcher.buildTaskEvent` → `TaskEvent`,
  `WorkflowEngine.onResult`/`DagTopicListener`/`MailResponseHandler` → `ResultEvent`.
- `KafkaTopicE2ETest` baut seinen Container manuell (`ContainerProperties(TASK_TOPIC)`)
  und bleibt damit vom Annotation-Wildcard unberührt — weiter grün.

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

*Stand: 2026-10-02 — Build grün, 270 Tests, 5 davon key-gated (kein LLM-Key nötig). Die Zeile vom 2026-09-19 nannte 101 Tests, die vom 2026-09-29 244; hinzu kamen die Charakterisierungs-, Listener- und Konfigurationstests. Ausserdem sind elf Blocker behoben, die den Betrieb bzw. den Start verhinderten — siehe README, Abschnitt „Verifizierter Stand“.*  
*Item 1 (Worker-AgentConfig aus Shared-DB `agent_definitions` mit YAML-Fallback, Property-gated) + Item 3 (Worker-e2e-Test: echter LLM-Key `OPENAI_API_KEY`, sonst deterministischer Mock-Fallback) sind implementiert.*  
*Ebene 3 (DB-E2E) ✅: `JdbcAgentConfigSourceE2ETest` (4 Tests) fährt ein echtes PostgreSQL 16 (Testcontainer, Image `postgres:16-alpine`) mit dem echten `V1__init.sql`-Schema — verifiziert RowMapper/JSONB/Defaults und DB-first-Vorrang vor YAML in der Factory.*  
*Ebene 4 (Kafka-Topic-Durchstich) ✅: `KafkaTopicE2ETest` (2 Tests) startet einen echten Kafka-Broker in-JVM (spring-kafka-test `EmbeddedKafkaKraftBroker`, offline, kein Docker) — verifiziert die komplette Topic-Kette Orchestrator-Producer → `agent.tasks.researcher` → `WorkflowTaskListener` (echter Batch-Container + MANUAL Ack) → TaskExecutor → `agent.execution.results`, plus den DLQ-Pfad malformed JSON → `agent.execution.dlq`.*  
*Item 2 (O1) ✅ (2026-09-19): Wildcard-Listener `worker.quad.task-topic-pattern` (Default `agent\\.tasks\\..*`), einheitlicher kanonischer Contract `TaskEvent`/`ResultEvent` in `agent-common` für Orchestrator + QUAD-Worker (Jackson-3/Jackson-2-Wire-Format kompatibel), Legacy-`agent-worker` auf `legacy\.agent\.tasks\..*` verbannt und aus docker-compose entfernt.*  
*Nächster Schritt: Production Vault Setup + Real LLM Worker Integration.*

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
- **Total Modules:** 5 (parent, agent-common, agent-orchestrator, agent-worker, quad-workflow-worker); die End-to-End-Tests liegen im eigenen Projekt `../integration-tests` (`mvn -o test -Pe2e` von dort)
- **Total Tests:** 270 (5 key-gated, ohne `OPENAI_API_KEY` übersprungen)
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
| Beide Backends auf identische Postgres-Instanz (`agent_platform`) | ✅ |
| Flyway-History isoliert (QUAD `quad_schema_history` vs Orchestrator `flyway_schema_history`) | ✅ |
| `GET/POST /api/v1/agents` (Agent-Registry für Kafka) | ✅ |
| UnifiedApiGateway: **atomarer** Convert+Persist+Start (kein HTTP-Roundtrip zu Quarkus) | ✅ |
| Auto-Registrierung referenzierter Agenten (`ensureStub`) | ✅ |
| `fetchKafkaAgents()` in kafkaApi.ts | ✅ |
| WorkflowComposer: Agent-Loading pro Backend (Quarkus vs Kafka) | ✅ |
| Kafka-Execute startet Workflow sofort (Save-Lücke entfernt) | ✅ |
| Security-Toggle `app.security.enabled` (Dev: offen, Prod: JWT) | ✅ |
| Vite-Proxy `/api/v1`→8080, `/api`→8086 | ✅ |
| start-backend.sh respektiert `QUARKUS_DATASOURCE_*` | ✅ |

**UI ohne Spring-Backend:** `./start-backend.sh` (SQLite) + `npm run dev` → alles läuft über Quarkus.
**Shared-DB-Modus:** QUARKUS_DATASOURCE_* auf PG gesetzt → beide Backends nutzen dieselbe DB.
