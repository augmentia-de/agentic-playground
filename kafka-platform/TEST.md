# Quad Workflow Worker – End-to-End Testing Guide

## Goal

Verify that the **quad-workflow-worker** sends real LLM requests (OpenRouter/OpenAI) and returns correct results via Kafka. No mock!

---

## Architecture Under Test

```
┌─────────────────┐    ┌──────────────┐    ┌──────────────────┐
│  Orchestrator   │──► │     Kafka    │◄──►│   quad-worker    │
│  REST API       │    │ Topics       │    │   Spring Boot    │
│                 │◄───│              │    │                  │
│ POST /api/v1/   │    │              │    │  Consumer:       │
│   workflows/start│    │              │    │  agent.tasks.*   │
│                 │    │              │    │                  │
│ POST /api/v1/   │    │              │    │  AgentFactory:   │
│   workflow-defs │    │              │    │  WorkerAgent→    │
│                 │    │              │    │  executeStructured│
└─────────────────┘    └──────────────┘    │  ChatModel:      │
                                            │  OpenRouter      │
                                            │  (deepseek-v4)   │
                                            │                  │
                                            │  Publisher:      │
                                            │  agent.execution │
                                            │  .results        │
└─────────────────┘
│  Docker Compose   │
│  Stack            │
└─────────────────┘
```

---

## Infrastructure

| Service | Port(s) | Purpose |
|---------|---------|-------|
| Kafka | 9092 | Message Broker (Confluent 7.7) |
| PostgreSQL | 5432 | Orchestrator DB |
| Redis | 6379 | Orchestrator Cache (idempotency) |
| Orchestrator | 8080 | REST API + Workflow Engine |
| mock-worker | 8091 | Deterministic mock worker |
| **quad-worker** | 8092 | Real LLM worker (test target) |

### Prerequisites

- Docker Desktop with ≥ 4 GB memory
- `.env` file in the project directory with API key:
  ```bash
  OPENAI_API_KEY="sk-or-v1-..."
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  ```
- Maven ≥ 3.9 (for local tests if needed)

---

## Setup Steps

### 1. Start Infrastructure

```bash
cd /home/torsten/dev/my-projects/kafka_workflow

# Infra nur (Kafka, Postgres, Redis)
docker compose up -d kafka postgres redis

# Warten bis Kafka ready
sleep 30
docker compose ps --filter status=healthy
```

### 2. Build & Start the Quad Worker

```bash
cd quad-workflow-worker
mvn clean package -DskipTests -q
cd ..

# Docker Image bauen
docker compose build quad-worker

# Worker starten
OPENAI_API_KEY="<your-key>" docker compose up -d quad-worker

# Health Check
curl -s http://localhost:8092/actuator/health | jq .status
# Expected: {"status":"UP"}

# Logs prüfen (API Key erkannt?)
docker logs quad-worker 2>&1 | grep -i "model\|openrouter\|configured"
```

### 3. Validate Topics

The required topics are created automatically (auto.create.topics.enable=true in Confluent). If not:

```bash
# Topic existiert?
docker exec agent-platform-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Manuell anlegen falls nötig:
docker exec agent-platform-kafka \
  kafka-topics --bootstrap-server localhost:9092 --create \
  --topic agent.tasks.researcher --partitions 3 --replication-factor 1

docker exec agent-platform-kafka \
  kafka-topics --bootstrap-server localhost:9092 --create \
  --topic agent.execution.results --partitions 3 --replication-factor 1
```

---

## Test Scenarios

### T1: Simple Research Task (Baseline)

**Description:** Send a simple `research` task to the researcher agent and check the result from the LLM.

**Procedure:**
```bash
# 1. Workflow Definition erstellen
curl -X POST http://localhost:8080/api/v1/workflow-definitions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-research-flow",
    "version": 1,
    "dag_structure": {
      "nodes": [
        {
          "stepId": "research",
          "nodeType": "STEP",
          "agentType": "researcher",
          "dependsOn": []
        },
        {
          "stepId": "end",
          "nodeType": "END",
          "dependsOn": ["research"]
        }
      ]
    }
  }'

# Response: {"definitionId":"<uuid>","name":"test-research-flow","version":1}

# 2. Workflow starten
curl -X POST http://localhost:8080/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{
    "workflowName": "test-research-flow",
    "version": 1,
    "inputParams": {
      "query": "Liste die Top-5-KI-Trends 2025 mit kurzen Beschreibungen."
    },
    "idempotencyKey": "test-simple-research-001"
  }'

# Response: {"workflowExecutionId":"<uuid>","duplicate":false}
```

**Expected Behavior:**
1. Orchestrator publishes task to `agent.tasks.researcher`
2. quad-worker consumes task → builds prompt → calls LLM
3. LLM responds with JSON-structured content
4. quad-worker publishes result to `agent.execution.results`
5. Orchestrator receives result → checkpoint updated

**Validation:**
```bash
# 1. Checkpoints prüfen (HTTP)
EXEC_ID="<from-start-response>"
curl -s "http://localhost:8080/api/v1/workflows/${EXEC_ID}/checkpoints" | jq .

# 2. Kafka Result event konsumieren (manuell zum Debuggen)
docker exec agent-platform-kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic agent.execution.results --from-beginning --max-messages 10

# 3. Prometheus Metrics des Workers
curl -s http://localhost:8092/actuator/prometheus | grep quad_worker
```

**Valid Result Fields:**
```json
{
  "executor": "quad-core",
  "agentType": "researcher",
  "response": "<LLM-Antwort>",
  "confidence": <0.0-1.0>,
  "status": "COMPLETED"
}
```

---

### T2: Multi-Step Research with Decision Gate

**Description:** Research task with a confidence gate: if confidence < 0.7 → retry.

**Workflow:**
```
research → quality_gate(DECISION)
               ├─ retry → refinement_loop → quality_gate
               └─ pass → end
```

**Procedure:** See the JSON payload below. This is about routing logic — the worker simply returns COMPLETED, and the orchestrator decides about a retry based on confidence.

**Test Commands:** See T1. Difference: the quality of the first result determines whether a retry happens.

---

### T3: Error Handling – Timeout Simulation

**Description:** Validate that timeouts are correctly classified as `FAILED_RETRYABLE`.

**Test Method:** Temporarily reduce the LLM timeout from 120s to 100ms and execute a task.

**Expected:**
- Task is picked up by the worker
- LLM call times out after ~100ms
- Result published with `status: FAILED_RETRYABLE`
- Orchestrator sets checkpoint to `RETRYING`
- After sweep → new attempt started

**Check:**
```bash
# Metrics prüfen
curl -s http://localhost:8092/actuator/prometheus | grep "quad_worker_task_duration"
```

---

### T4: Multiple Parallel Tasks (Fork/Join)

**Description:** Send two researcher tasks in parallel and see whether both are processed.

**Orchestrated Sequence:** The orchestrator's fork/join is not necessary for this test — you simply start two workflows in parallel with different `idempotencyKey`s.

**Command:**
```bash
curl -s -X POST http://localhost:8080/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{"workflowName":"test-research-flow","version":1,"inputParams":{"query":"Query A"},"idempotencyKey":"parallel-test-A"}'

curl -s -X POST http://localhost:8080/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{"workflowName":"test-research-flow","version":1,"inputParams":{"query":"Query B"},"idempotencyKey":"parallel-test-B"}'
```

**Check:** Both should receive different executionIds and be processed independently.

---

### T5: Idempotency Check

**Description:** Start the same workflow twice with the same `idempotencyKey`.

**Command:**
```bash
# Erster Call
curl -s -X POST http://localhost:8080/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{"workflowName":"test-research-flow","version":1,"inputParams":{"query":"Duplicate Test"},"idempotencyKey":"idem-test-001"}'

# Zweiter Call (selber Key)
curl -s -X POST http://localhost:8080/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{"workflowName":"test-research-flow","version":1,"inputParams":{"query":"Duplicate Test"},"idempotencyKey":"idem-test-001"}'
```

**Expected:** Second call → `"duplicate": true`. Only one task is executed, but no error is thrown.

---

### T6: Cost Tracking (Token Usage)

**Description:** Verify that token usage and costs are tracked.

**Prerequisite:** quad-core exposes TokenUsage in AgentResult (upstream fix needed, currently often `promptTokens=0`).

**Check:**
```bash
curl -s "http://localhost:8080/api/v1/workflows/<exec-id>/checkpoints" | jq '.[].resultData.token_usage'
```

---

## Monitoring & Observability

### Prometheus Metrics (Quad Worker)

Endpoint: `http://localhost:8092/actuator/prometheus`

Important metrics:
```text
# HELP quad_worker_tasks_received_total Total tasks received from Kafka
quad_worker_tasks_received_total 42.0

# HELP quad_worker_tasks_completed_total Total tasks completed successfully
quad_worker_tasks_completed_total{agentType="researcher",status="completed"} 38.0
quad_worker_tasks_completed_total{agentType="researcher",status="failed"} 4.0

# HELP quad_worker_dlq_total Total messages sent to DLQ
quad_worker_dlq_total 2.0

# HELP quad_worker_task_duration_seconds Task execution duration
quad_worker_task_duration_seconds_sum 120.5
quad_worker_task_duration_seconds_count 42.0

# HELP quad_worker_llm_duration_seconds LLM API call duration
quad_worker_llm_duration_seconds_sum 115.2
quad_worker_llm_duration_seconds_count 40.0
```

### Health Checks

```bash
# Worker health
curl -s http://localhost:8092/actuator/health | jq .

# Orchester health  
curl -s http://localhost:8080/actuator/health | jq .
```

### Logging Format

Every log entry contains:
```
[INFO] Processing task: workflow=<wfId>, taskId=<taskId>, agentType=researcher, step=research
[INFO] Agent created for type=researcher, model=simple
[INFO] Processing result: workflow=<wfId>, step=research, confidence=0.85, status=COMPLETED
```

---

## Troubleshooting

### LLM Calls Failed

```bash
# Prüfen ob API Key gesetzt ist
docker inspect quad-worker | grep OPENAI_API_KEY

# Logs prüfen
docker logs quad-worker 2>&1 | grep -i "openrouter\|error\|exception"

# Direkt gegen OpenRouter testen
curl https://openrouter.ai/api/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY" | jq ".data[:3]"
```

### Task Not Being Processed by the Worker

```bash
# Topic existiert?
docker exec agent-platform-kafka \
  kafka-topics --bootstrap-server localhost:9092 --describe

# Messages im Topic?
docker exec agent-platform-kafka \
  kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic agent.tasks.researcher

# Worker Consumer Group Status
docker exec agent-platform-kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group quad-workflow-worker
```

### Worker Crashed / OOM

```bash
docker logs quad-worker 2>&1 | tail -50
# Bei OOM: Memory erhöhen in docker-compose.yml mem_limit: 1024m
```

---

## Results & Conclusion

### Expected Results on Successful Execution

| Criterion | Expectation | Status |
|-----------|-----------|--------|
| Create workflow definition | 201 Created | ✅ |
| Start workflow | 202 Accepted | ✅ |
| Task visible on Kafka topic | Yes | ✅ |
| Worker consumes task | Visible in logs | ✅ |
| LLM call successful | 200 OK from OpenRouter | ⏳ to be verified |
| Result in `agent.execution.results` | Yes | ⏳ |
| Checkpoint updated | Yes | ⏳ |
| Prometheus metrics incremented | Yes | ⏳ |
| Idempotency checked | duplicate: true | ✅ |

### Known Limitations

1. **TokenUsage**: `quad-core` exposes TokenUsage incompletely as yet (promptTokens often 0). Upstream fix #3 required for correct cost metrics.
2. **Rate limits**: default limiter at 100 tokens/min / 5 concurrent calls. May need to be increased with several parallel workflows.
3. **Timeout**: default 120s per task. May need to be increased for complex prompts/long LLM calls.
