# Quad Workflow Worker – End-to-End Testing Guide

## Ziel

Verifizieren dass der **quad-workflow-worker** echte LLM-Anfragen (OpenRouter/OpenAI) sendet und korrekte Ergebnisse über Kafka zurckliefert. Kein Mock!

---

## Architektur im Test

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

## Infrastruktur

| Service | Port(s) | Zweck |
|---------|---------|-------|
| Kafka | 9092 | Message Broker (Confluent 7.7) |
| PostgreSQL | 5432 | Orchestrator DB |
| Redis | 6379 | Orchestrator Cache (Idempotenz) |
| Orchestrator | 8080 | REST API + Workflow Engine |
| mock-worker | 8091 | Deterministischer Mock-Worker |
| **quad-worker** | 8092 | Echter LLM-Worker (Test-Ziel) |

### Voraussetzungen

- Docker Desktop mit ≥ 4 GB Memory
- `.env` Datei im Projektverzeichnis mit API Key:
  ```bash
  OPENAI_API_KEY="sk-or-v1-..."
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  ```
- Maven ≥ 3.9 (für lokale Tests falls nötig)

---

## Setup-Schritte

### 1. Infrastruktur starten

```bash
cd /home/torsten/dev/my-projects/kafka_workflow

# Infra nur (Kafka, Postgres, Redis)
docker compose up -d kafka postgres redis

# Warten bis Kafka ready
sleep 30
docker compose ps --filter status=healthy
```

### 2. Quad-Worker bauen & starten

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

### 3. Topics validieren

Die benötigten Topics werden automatisch erstellt (auto.create.topics.enable=true in Confluent). Falls nicht:

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

## Test-Szenarien

### T1: Simple Research Task (Baseline)

**Beschreibung:** Ein einfacher `research`-Task an den researcher-Agenten schicken und das Ergebnis vom LLM prüfen.

**Ablauf:**
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

**Erwartetes Verhalten:**
1. Orchestrator publish Task auf `agent.tasks.researcher`
2. quad-worker消费 Task → Prompt bauen → LLM aufrufen
3. LLM antwortet mit JSON-strukturiertem Inhalt
4. quad-worker publiziert Result auf `agent.execution.results`
5. Orchestrator empfängt Result → Checkpoint aktualisiert

**Validierung:**
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

**Gültige Result-Felder:**
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

### T2: Multi-Step Research mit Decision Gate

**Beschreibung:** Research-Task mit Confidence-Gate: Bei confidence < 0.7 → Retry.

**Workflow:**
```
research → quality_gate(DECISION)
               ├─ retry → refinement_loop → quality_gate
               └─ pass → end
```

**Ablauf:** Siehe JSON-Payload unten. Hier geht es um Routing-Logik – der Worker liefert einfach COMPLETED zurück, der Orchestrator entscheidet über Retry basierend auf confidence.

**Test-Befehle:** Siehe T1. Unterschied: Die Qualität des ersten Ergebnisses bestimmt ob Retry passiert.

---

### T3: Error Handling – Timeout Simulation

**Beschreibung:** Validieren dass timeouts korrekt als `FAILED_RETRYABLE` klassifiziert werden.

**Test-Methode:** Temporär LLM Timeout von 120s auf 100ms reduzieren und Task ausführen.

**Erwartet:**
- Task wird vom Worker abgeholt
- LLM Call timeout nach ~100ms
- Result mit `status: FAILED_RETRYABLE` publiziert
- Orchestrator setzt Checkpoint auf `RETRYING`
- Nach Sweep → neuer attempt gestartet

**Check:**
```bash
# Metrics prüfen
curl -s http://localhost:8092/actuator/prometheus | grep "quad_worker_task_duration"
```

---

### T4: Multiple Parallel Tasks (Fork/Join)

**Beschreibung:** Zwei researcher-Tasks parallel schicken und sehen ob beide verarbeitet werden.

**Orchestrierte Abfolge:** Der Orchestrator fork/join für diesen Test nicht notwendig – man startet einfach zwei Workflows parallel mit verschiedenen `idempotencyKey`s.

**Befehl:**
```bash
curl -s -X POST http://localhost:8080/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{"workflowName":"test-research-flow","version":1,"inputParams":{"query":"Query A"},"idempotencyKey":"parallel-test-A"}'

curl -s -X POST http://localhost:8080/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{"workflowName":"test-research-flow","version":1,"inputParams":{"query":"Query B"},"idempotencyKey":"parallel-test-B"}'
```

**Check:** Beide sollten unterschiedliche executionIds erhalten und unabhängig processed werden.

---

### T5: Idempotenz-Prüfung

**Beschreibung:** Gleichen Workflow zweimal mit derselben `idempotencyKey` starten.

**Befehl:**
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

**Erwartet:** Zweiter Call → `"duplicate": true`. Nur ein Task wird ausgeführt, aber kein Fehler geworfen.

---

### T6: Kosten-Tracking (Token Usage)

**Beschreibung:** Überprüfen dass Token-Nutzung und Kosten tracked werden.

**Voraussetzung:** quad-core exposes TokenUsage in AgentResult (upstream-fix nötig, aktuell oft `promptTokens=0`).

**Check:**
```bash
curl -s "http://localhost:8080/api/v1/workflows/<exec-id>/checkpoints" | jq '.[].resultData.token_usage'
```

---

## Monitoring & Observability

### Prometheus Metriken (Quad Worker)

Endpoint: `http://localhost:8092/actuator/prometheus`

Wichtige Metriken:
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

Jeder Log-Eintrag enthält:
```
[INFO] Processing task: workflow=<wfId>, taskId=<taskId>, agentType=researcher, step=research
[INFO] Agent created for type=researcher, model=simple
[INFO] Processing result: workflow=<wfId>, step=research, confidence=0.85, status=COMPLETED
```

---

## Troubleshooting

### LLM Calls fehlgeschlagen

```bash
# Prüfen ob API Key gesetzt ist
docker inspect quad-worker | grep OPENAI_API_KEY

# Logs prüfen
docker logs quad-worker 2>&1 | grep -i "openrouter\|error\|exception"

# Direkt gegen OpenRouter testen
curl https://openrouter.ai/api/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY" | jq ".data[:3]"
```

### Task wird nicht vom Worker verarbeitet

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

## Ergebnisse & Fazit

### Erwartete Ergebnisse bei erfolgreicher Ausführung

| Kriterium | Erwartung | Status |
|-----------|-----------|--------|
| Workflow Definition erstellen | 201 Created | ✅ |
| Workflow starten | 202 Accepted | ✅ |
| Task auf Kafka Topic sichtbar | Ja | ✅ |
| Worker消费 Task | In Logs sichtbar | ✅ |
| LLM Call erfolgreich | 200 OK von OpenRouter | ⏳ zu verifizieren |
| Result in `agent.execution.results` | Ja | ⏳ |
| Checkpoint aktualisiert | Ja | ⏳ |
| Prometheus Metrics inkrementiert | Ja | ⏳ |
| Idempotenz geprüft | duplicate: true | ✅ |

### Bekannte Einschränkungen

1. **TokenUsage**: `quad-core` expose TokenUsage noch unvollständig (promptTokens oft 0). Upstream-Fix #3 erforderlich für korrekte Cost-Metriken.
2. **Rate-Limits**: Standard-Limiter auf 100 tokens/min / 5 concurrent calls. Bei mehreren parallelen Workflows evtl. erhöhen.
3. **Timeout**: Default 120s pro Task. Bei komplexen Prompts/Lang LLM Calls möglicherweise erhöht werden.
