# System Specification: Event-Driven AI Agent Platform

> **Version**: 2.0  
> **Status**: Draft  
> **Last Updated**: 2026-09-16  
> **Decisions Incorporated**: Orchestrator-zentrale DAG Engine, Kafka Delayed Messages, Confluent Schema Registry, Hybrid DLQ Recovery, HashiCorp Vault, Redis single-node, Deterministic Routing (Loops, Conditions, Fork/Join), STEP-Guards, Branch-Failure-Semantik, REST-API, HITL-Expiry

---

## 1. Executive Summary

The **Event-Driven AI Agent Platform** is an enterprise-grade, highly available, asynchronous multi-agent orchestrator. The platform uses **Apache Kafka** as its central event bus, **Java 21 (LTS)** with **Spring Boot 3.3 / Spring Kafka** for execution and orchestration runtimes, **PostgreSQL 16** for durable workflow states and dynamic agent configuration, **Redis 7.2** for ephemeral runtime state and task deduplication, and **HashiCorp Vault** for secrets management.

### 1.1 Architectural Principles

1. **Fully Asynchronous Execution**: Communication between agents, orchestrators, and system services is non-blocking and decoupled via Kafka topics.
2. **Orchestrator-Central DAG Engine**: A single orchestrator service owns DAG traversal, dispatches tasks to agent workers, and updates checkpoints atomically.
3. **Database-Driven Dynamic Agent Configuration**: Agent parameters are loaded from PostgreSQL, cached in-memory, and refreshed via CDC (Debezium) or Kafka config-update events.
4. **Resilience & Fault Tolerance**: Mandatory idempotency keys, exponential backoffs, Dead Letter Queues (DLQ) with hybrid recovery, circuit breaking for LLM providers, and atomic checkpointing.
5. **Schema Evolution via Confluent Schema Registry**: All Kafka message schemas are versioned with Backward-Compatibility enforcement.
6. **End-to-End Observability**: Distributed tracing via OpenTelemetry with W3C Trace Context propagation across Kafka headers.
7. **Human-in-the-Loop (HITL) Alerting**: Escalation via Slack, PagerDuty, and Email with interactive approval workflows.
8. **Security by Design**: HashiCorp Vault for secrets, RBAC for HITL roles, Kafka ACLs, and network segmentation.
9. **Deterministic Routing**: All control-flow decisions (conditions, loops, fork/join) are pure functions of the persisted DAG state. No randomness, no external HTTP, and no wall-clock time in the routing path — re-running the evaluator over the same state always yields the same next set of fireable steps.

---

## 2. Technology Stack

| Component | Technology | Version | Purpose |
| :--- | :--- | :--- | :--- |
| **Language Runtime** | Java OpenJDK | 21 (LTS) | Virtual Threads (Project Loom) |
| **Build System** | Apache Maven | 3.9+ | Multi-Module Build |
| **Framework** | Spring Boot / Spring Kafka | 3.3.x | IOC, Kafka integration, JPA, REST |
| **Event Broker** | Apache Kafka (Strimzi) | 3.7+ | Event bus, message streaming |
| **Schema Registry** | Confluent Schema Registry | 7.6+ | Schema versioning, compatibility enforcement |
| **Primary Database** | PostgreSQL | 16.x | Workflow state, config, checkpoints |
| **Cache / State** | Redis | 7.2.x (single-node) | Dedup, locks, ephemeral context |
| **CDC Engine** | Debezium / PgOutput | 2.5+ | DB change capture for config updates |
| **Secrets Management** | HashiCorp Vault | 1.15+ | Dynamic secrets, DB credentials, API keys |
| **Metrics & Tracing** | OpenTelemetry / Micrometer | 1.38+ | Distributed tracing, Prometheus metrics |
| **Alerting** | Slack Webhooks / PagerDuty API | Native REST | HITL escalation notifications |

---

## 3. Architecture Overview

```
+---------------------------------------------------------------------------------------------------+
|                                        API Gateway / Frontend                                     |
+---------------------------------------------------------------------------------------------------+
        |
        v
+---------------------------------------------------------------------------------------------------+
|                                 Kafka Event Bus (Cluster)                                         |
|  agent.execution.intake       agent.tasks.<agent_type>       agent.execution.results             |
|  agent.control.timeouts       agent.control.cancellation     agent.control.hitl                  |
|  agent.execution.dlq          telemetry.agent.metrics        agent.config.updates                |
+---------------------------------------------------------------------------------------------------+
        ^                                    ^                                    ^
        |                                    |                                    |
+---------------+                  +---------------+                    +---------------+
| Orchestrator  |                  | Agent Workers |                    | Timeout & HITL|
| Engine (Java) |                  | (Generic Java)|                    | Service (Java)|
+-------+-------+                  +-------+-------+                    +-------+-------+
        |                                    |                                    |
        v                                    v                                    v
+-----------------+                +-----------------+                  +-----------------+
| PostgreSQL 16   |                | HashiCorp Vault |                  |     Redis 7.2   |
| (State/DAG/CFG) |                | (Secrets/Keys)  |                  | (Dedup/Locks)   |
+-----------------+                +-----------------+                  +-----------------+
```

---

## 4. Kafka Topic Topology

### 4.1 Topic Definitions

| Topic Name | Partitions | Cleanup Policy | Key Strategy | Retention | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `agent.execution.intake` | 12 | `delete` | `workflow_id` | 7 Days | Incoming workflow trigger |
| `agent.tasks.<agent_type>` | 12 | `delete` | `workflow_id` | 7 Days | Agent-specific task queues |
| `agent.execution.results` | 12 | `delete` | `workflow_id` | 7 Days | Execution results & status |
| `agent.control.timeouts` | 6 | `delete` | `task_id` | 3 Days | Delayed timeout events |
| `agent.control.cancellation` | 6 | `delete` | `workflow_id` | 1 Day | Cancellation signals |
| `agent.control.hitl` | 6 | `delete` | `workflow_id` | 14 Days | Human review escalation |
| `agent.execution.dlq` | 6 | `delete` | `workflow_id` | 30 Days | Dead letter queue |
| `agent.control.loop` | 6 | `delete` | `workflow_id` | 3 Days | Loop iteration advancement (non-agent) |
| `agent.control.sweep` | 1 | `delete` | `sweep_id` | 1 Day | Periodic deterministic routing sweep / reaper ticks |
| `agent.config.updates` | 1 | `compact` | `agent_type` | Compacted | Dynamic config broadcast |

### 4.2 Schema Registry Strategy

All event payloads are registered in **Confluent Schema Registry** using **JSON Schema** format with **BACKWARD** compatibility mode.

| Subject Name Pattern | Format | Compatibility | Description |
| :--- | :--- | :--- | :--- |
| `agent.tasks.<agent_type>-value` | JSON Schema | BACKWARD | Task assignment events |
| `agent.execution.results-value` | JSON Schema | BACKWARD | Result events |
| `agent.control.timeouts-value` | JSON Schema | BACKWARD | Timeout events |
| `agent.control.hitl-value` | JSON Schema | BACKWARD | HITL escalation events |
| `agent.control.loop-value` | JSON Schema | BACKWARD | Loop iteration advance events |
| `agent.control.sweep-value` | JSON Schema | BACKWARD | Routing sweep tick events |
| `agent.config.updates-value` | JSON Schema | BACKWARD | Config update broadcasts |

**Versioning Rules**:
- New fields MUST have default values to maintain backward compatibility.
- Fields MUST NOT be removed without a major version bump.
- Enum values MAY only be appended, never removed.
- Consumers MUST use `SKIP_UNKNOWN_PROPERTIES` deserialization mode.

### 4.3 Key Payload Schemas

#### Task Assignment Event (`agent.tasks.<agent_type>`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "AgentTaskEvent",
  "type": "object",
  "properties": {
    "header": {
      "type": "object",
      "properties": {
        "workflowId": { "type": "string", "format": "uuid" },
        "taskId": { "type": "string", "format": "uuid" },
        "parentTaskId": { "type": ["string", "null"], "format": "uuid" },
        "traceParent": { "type": "string" },
        "attempt": { "type": "integer", "minimum": 1, "default": 1 },
        "maxAttempts": { "type": "integer", "minimum": 1, "default": 3 },
        "createdAt": { "type": "string", "format": "date-time" },
        "timeoutMs": { "type": "integer", "default": 60000, "minimum": 1000 }
      },
      "required": ["workflowId", "taskId", "traceParent", "attempt", "createdAt"]
    },
    "agentConfig": {
      "type": "object",
      "properties": {
        "agentType": { "type": "string" },
        "version": { "type": "integer", "minimum": 1 },
        "model": { "type": "string", "default": "gpt-4o" },
        "temperature": { "type": "number", "minimum": 0, "maximum": 2, "default": 0.2 }
      },
      "required": ["agentType", "version"]
    },
    "payload": {
      "type": "object",
      "properties": {
        "inputPrompt": { "type": "string", "minLength": 1 },
        "contextReferences": {
          "type": "array",
          "items": { "type": "string" },
          "default": []
        },
        "parameters": { "type": "object", "default": {} }
      },
      "required": ["inputPrompt"]
    }
  },
  "required": ["header", "agentConfig", "payload"]
}
```

#### Result Event (`agent.execution.results`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "AgentResultEvent",
  "type": "object",
  "properties": {
    "header": {
      "type": "object",
      "properties": {
        "workflowId": { "type": "string", "format": "uuid" },
        "taskId": { "type": "string", "format": "uuid" },
        "agentType": { "type": "string" },
        "status": {
          "type": "string",
          "enum": ["COMPLETED", "FAILED_RETRYABLE", "FAILED_FATAL", "NEEDS_HITL"]
        },
        "completedAt": { "type": "string", "format": "date-time" },
        "executionDurationMs": { "type": "integer", "minimum": 0 }
      },
      "required": ["workflowId", "taskId", "agentType", "status", "completedAt"]
    },
    "output": {
      "type": "object",
      "properties": {
        "resultData": { "type": ["object", "null"], "default": null },
        "errorMessage": { "type": ["string", "null"], "default": null },
        "stackTrace": { "type": ["string", "null"], "default": null }
      }
    },
    "usage": {
      "type": "object",
      "properties": {
        "promptTokens": { "type": "integer", "minimum": 0, "default": 0 },
        "completionTokens": { "type": "integer", "minimum": 0, "default": 0 },
        "totalCostUsd": { "type": "number", "minimum": 0, "default": 0.0 }
      },
      "default": {}
    }
  },
  "required": ["header", "output"]
}
```

#### Timeout Event (`agent.control.timeouts`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "TimeoutEvent",
  "type": "object",
  "properties": {
    "taskId": { "type": "string", "format": "uuid" },
    "workflowId": { "type": "string", "format": "uuid" },
    "deadlineAt": { "type": "string", "format": "date-time" },
    "escalateToHitl": { "type": "boolean", "default": false }
  },
  "required": ["taskId", "workflowId", "deadlineAt"]
}
```

---

## 5. PostgreSQL Database Schema

```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Agent Definitions
CREATE TABLE agent_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_type VARCHAR(64) NOT NULL UNIQUE,
    description TEXT,
    system_prompt TEXT NOT NULL,
    model_name VARCHAR(64) NOT NULL DEFAULT 'gpt-4o',
    temperature NUMERIC(3, 2) NOT NULL DEFAULT 0.20,
    max_tokens INT NOT NULL DEFAULT 4096,
    tools_config JSONB NOT NULL DEFAULT '[]'::jsonb,
    timeout_seconds INT NOT NULL DEFAULT 120,
    max_retry_attempts INT NOT NULL DEFAULT 3,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_agent_definitions_type ON agent_definitions(agent_type);

-- 2. Workflow Definitions (DAG structure)
CREATE TABLE workflow_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(128) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    dag_structure JSONB NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_workflow_name_version UNIQUE (name, version)
);

-- 3. Workflow Execution Instances
CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    input_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    final_output JSONB,
    error_details JSONB,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_workflow_executions_status ON workflow_executions(status);
CREATE INDEX idx_workflow_executions_def_id ON workflow_executions(workflow_definition_id);

-- 4. Workflow Checkpoints (DAG node execution state)
CREATE TABLE workflow_checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    step_id VARCHAR(64) NOT NULL,
    iteration INT NOT NULL DEFAULT 1,
    node_type VARCHAR(32) NOT NULL DEFAULT 'STEP',
    agent_type VARCHAR(64),
    task_id UUID UNIQUE,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    decision_branch VARCHAR(64),
    input_payload JSONB,
    output_payload JSONB,
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_execution_step_iteration UNIQUE (workflow_execution_id, step_id, iteration)
);

CREATE INDEX idx_checkpoints_execution_id ON workflow_checkpoints(workflow_execution_id);
CREATE INDEX idx_checkpoints_task_id ON workflow_checkpoints(task_id);
CREATE INDEX idx_checkpoints_status ON workflow_checkpoints(status);
CREATE INDEX idx_checkpoints_stale ON workflow_checkpoints(status, started_at)
    WHERE status IN ('IN_PROGRESS', 'PENDING');

-- 5. HITL Tickets
CREATE TABLE hitl_tickets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id),
    checkpoint_id UUID REFERENCES workflow_checkpoints(id),
    reason VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'OPEN',
    assigned_role VARCHAR(64) DEFAULT 'OPERATOR',
    alert_channels JSONB NOT NULL DEFAULT '[]'::jsonb,
    context_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolution_notes TEXT,
    resolved_by VARCHAR(128),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_hitl_tickets_status ON hitl_tickets(status);
CREATE INDEX idx_hitl_tickets_workflow ON hitl_tickets(workflow_execution_id);

-- 6. DLQ Messages (for hybrid recovery)
CREATE TABLE dlq_messages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic VARCHAR(128) NOT NULL,
    original_key VARCHAR(256),
    payload JSONB NOT NULL,
    error_class VARCHAR(256),
    error_message TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_dlq_status ON dlq_messages(status);
CREATE INDEX idx_dlq_next_retry ON dlq_messages(next_retry_at) WHERE status = 'PENDING';
```

### 5.1 DAG Structure Format (`dag_structure` JSONB)

```json
{
  "nodes": [
    {
      "stepId": "research_step",
      "nodeType": "STEP",
      "agentType": "researcher",
      "requiresHumanApproval": false,
      "dependsOn": []
    },
    {
      "stepId": "code_generation_step",
      "nodeType": "STEP",
      "agentType": "coder",
      "requiresHumanApproval": true,
      "dependsOn": ["research_step"]
    },
    {
      "stepId": "review_step",
      "nodeType": "STEP",
      "agentType": "reviewer",
      "requiresHumanApproval": false,
      "dependsOn": ["code_generation_step"]
    }
  ]
}
```

### 5.2 Node Type Vocabulary

| `nodeType` | Meaning | Routing Role | Key Attributes |
| :--- | :--- | :--- | :--- |
| `STEP` | Executable agent task | Dispatches to `agent.tasks.<agent_type>` | `agentType`, `timeoutMs`, `guardCondition?`, `requiresHumanApproval?` |
| `DECISION` | Pure branch node (no agent call) | Evaluates conditions, activates exactly one branch | `branches[{condition, target}]`, `defaultTarget` |
| `LOOP` | Bounded iteration around one agent step | Re-dispatches body up to `maxIterations` | `bodyAgentType`, `maxIterations`, `exitCondition` |
| `FORK` | Parallel split | Activates all branch heads concurrently | `branches[{branchId, firstStep}]` |
| `JOIN` | Synchronization barrier | Waits for branch completion (AND / XOR / N_OF) | `joinType`, `joinN` |
| `END` | Workflow termination marker | Marks execution `COMPLETED` | — |

**Invariants enforced by the workflow-definition validator at upload time**:
- `dependsOn` graph between non-LOOP nodes is acyclic (cycles rejected).
- A `DECISION` branch may only target exactly one `LOOP` (bounded by `maxIterations`); the `LOOP`'s exit edge MUST point to the `DECISION`'s `defaultTarget`.
- Every `FORK` branch has exactly one `JOIN` rendezvous; every `JOIN` references its upstream branches.
- Branch-owned steps MUST use `step_id = "b.<branchId>.<logicalName>"`.

### 5.3 Advanced DAG Example (DECISION + LOOP + FORK/JOIN)

```json
{
  "nodes": [
    {
      "stepId": "research_step",
      "nodeType": "STEP",
      "agentType": "researcher",
      "dependsOn": []
    },
    {
      "stepId": "quality_gate",
      "nodeType": "DECISION",
      "branches": [
        {
          "condition": "context['research_step'].resultData.confidence < 0.8",
          "target": "refinement_loop"
        }
      ],
      "defaultTarget": "parallel_fork",
      "dependsOn": ["research_step"]
    },
    {
      "stepId": "refinement_loop",
      "nodeType": "LOOP",
      "bodyAgentType": "refiner",
      "maxIterations": 3,
      "exitCondition": "context['refinement_loop'].resultData.confidence >= 0.8",
      "dependsOn": ["quality_gate"]
    },
    {
      "stepId": "parallel_fork",
      "nodeType": "FORK",
      "branches": [
        { "branchId": "code", "firstStep": "b.code.gen" },
        { "branchId": "test", "firstStep": "b.test.prep" }
      ],
      "dependsOn": ["quality_gate", "refinement_loop"]
    },
    {
      "stepId": "b.code.gen",
      "nodeType": "STEP",
      "agentType": "coder",
      "dependsOn": ["parallel_fork"]
    },
    {
      "stepId": "b.test.prep",
      "nodeType": "STEP",
      "agentType": "test_writer",
      "dependsOn": ["parallel_fork"]
    },
    {
      "stepId": "merge_gate",
      "nodeType": "JOIN",
      "joinType": "AND",
      "joinBranches": ["code", "test"],
      "dependsOn": ["b.code.gen", "b.test.prep"]
    },
    {
      "stepId": "final_review",
      "nodeType": "STEP",
      "agentType": "reviewer",
      "requiresHumanApproval": true,
      "dependsOn": ["merge_gate"]
    },
    {
      "stepId": "end",
      "nodeType": "END",
      "dependsOn": ["final_review"]
    }
  ]
}
```

---

## 6. Redis Data Structures

Redis runs as a **single-node instance** (HA via Sentinel planned for future iteration).

```
Redis Key Patterns:
+------------------------------------+------------------------+---------------------------------------+
| Key Pattern                        | Data Type              | Purpose / TTL                         |
+------------------------------------+------------------------+---------------------------------------+
| lock:task:{taskId}                 | String (SET NX EX)     | Prevents duplicate execution (TTL: 5m) |
| dedup:task:{taskId}                | String                 | Idempotency check (TTL: 24h)          |
| context:workflow:{workflowId}      | Hash                   | DAG output accumulation (TTL: 7d)     |
| active_timer:{taskId}              | String                 | Heartbeat marker (TTL: Task Timeout)  |
| rate_limit:llm:{provider}          | String (Sorted Set)    | Sliding window rate limiter            |
+------------------------------------+------------------------+---------------------------------------+
```

**Note**: Redlock is not used for single-node. `SET NX EX` is sufficient. Redlock will be enabled when Redis Sentinel/Cluster is introduced.

---

## 7. Orchestrator-Central DAG Engine

The orchestrator is the single owner of DAG traversal. Agent workers never communicate directly with each other.

### 7.1 DAG Execution Flow

```
[Start Workflow Request]
        |
        v
[Create workflow_execution record (PENDING)]
        |
        v
[Load DAG from workflow_definitions]
        |
        v
[Initialize all step checkpoints (PENDING)]
        |
        v
[Find steps with empty dependsOn → READY]
        |
        v
+---> [Dispatch READY tasks to Kafka agent.tasks.<type>]
|           |
|           v
|     [Agent Worker executes]
|           |
|           v
|     [Result published to agent.execution.results]
|           |
|           v
|     [Orchestrator consumes result]
|           |
|           v
|     [Update checkpoint: COMPLETED / FAILED]
|           |
|           v
|     [Check: all downstream steps now READY?]
|           |
|           +-- YES --> [Dispatch next batch]
|           |
|           +-- NO  --> [Wait]
|           |
|           +-- ALL DONE --> [Mark workflow COMPLETED]
|
+--- [On FAILED_RETRYABLE]: retry with backoff
+--- [On FAILED_FATAL / max retries]: create HITL ticket
+--- [On CANCELLED]: skip remaining steps
```

### 7.2 Orchestrator Core Responsibilities

1. **DAG Parsing**: Load `dag_structure` from `workflow_definitions`, resolve dependency graph, detect cycles.
2. **Task Dispatch**: For each READY step, create a checkpoint, produce to `agent.tasks.<type>`, and schedule timeout.
3. **Result Processing**: Consume `agent.execution.results`, update checkpoint status, trigger next steps.
4. **Checkpoint Management**: Atomic DB transactions: update checkpoint + update workflow status + check readiness of downstream steps.
5. **Timeout Scheduling**: Produce `TimeoutEvent` to `agent.control.timeouts` with `deadlineAt = now + timeoutSeconds`.
6. **Cancellation Handling**: Listen on `agent.control.cancellation`, mark checkpoints as `CANCELLED`, skip downstream steps.

### 7.3 Step Status State Machine

```
PENDING --> IN_PROGRESS --> COMPLETED
                |                |
                v                v
            FAILED -----------> IN_PROGRESS (retry)
                |
                v
           WAITING_HITL --> APPROVED --> IN_PROGRESS
                |
                v
            CANCELLED
```

---

## 8. Timeout Handling via Kafka Delayed Messages

Timeout events are produced to `agent.control.timeouts` with a **future timestamp** in the message key or headers. The Timeout Monitor consumer uses `consumer.seek()` with timestamp-based offset positioning to only consume messages whose timestamp has been reached.

### 8.1 Implementation Strategy

1. **Producer Side**: When dispatching a task, the orchestrator produces a `TimeoutEvent` to `agent.control.timeouts` with:
   - Key: `taskId`
   - Header `delivery-timestamp`: `System.currentTimeMillis() + timeoutMs`
   - The topic is configured with **no retention-based cleanup** for the delay window

2. **Consumer Side (Timeout Monitor)**:
   - On startup, the consumer seeks to offsets where `timestamp >= now - tolerance`
   - A scheduled poll runs every N seconds, seeking forward to `now` timestamp
   - Messages with `deadlineAt <= now` are processed; messages not yet due are skipped via seek

3. **When Timeout Fires**:
   - Query `workflow_checkpoints` for the task's current status
   - If `IN_PROGRESS`: trigger retry or escalate to HITL
   - If `COMPLETED` or `FAILED`: no-op (idempotent)

### 8.2 Configuration

```yaml
app:
  timeout:
    check-interval-ms: 5000
    tolerance-ms: 1000
    max-retries-before-hitl: 2
```

---

## 9. Resilience & Fault Tolerance

### 9.1 At-Least-Once Delivery & Idempotency

- All Kafka consumers use `ENABLE_AUTO_COMMIT = false`.
- Offsets are committed manually after atomic DB transactions succeed.
- Consumers check `dedup:task:{taskId}` in Redis (`SET NX EX`) before executing.
- Database `workflow_checkpoints.task_id` has a UNIQUE constraint as final guard.

### 9.2 Exponential Backoff

```
delay = min(baseDelay * 2^retryCount, maxDelay)
jitter = random(0, delay * 0.1)
```

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `baseDelay` | 1s | Initial retry delay |
| `maxDelay` | 60s | Maximum retry delay |
| `jitterFactor` | 0.1 | Random jitter percentage |

### 9.3 Circuit Breaker Configuration

Using Resilience4j with the following configuration:

```yaml
resilience4j:
  circuitbreaker:
    instances:
      llm-provider:
        slidingWindowSize: 10
        minimumNumberOfCalls: 5
        failureRateThreshold: 50
        waitDurationInOpenState: 30s
        permittedNumberOfCallsInHalfOpenState: 3
        automaticTransitionFromOpenToHalfOpenEnabled: true
        recordExceptions:
          - java.io.IOException
          - java.util.concurrent.TimeoutException
          - org.springframework.web.client.HttpServerErrorException
        ignoreExceptions:
          - com.platform.worker.exception.BusinessLogicException
```

When circuit opens:
1. Workers call `consumer.pause()` on their assignment partitions
2. After `waitDurationInOpenState`, circuit transitions to `HALF_OPEN`
3. A probe task is dispatched; success closes circuit, failure reopens it

### 9.4 DLQ Recovery (Hybrid Strategy)

**Automatic Recovery** (for transient errors):
1. Failed messages are written to `dlq_messages` table AND `agent.execution.dlq` Kafka topic
2. A DLQ Replay Service polls `dlq_messages WHERE status = 'PENDING' AND next_retry_at <= now`
3. Messages are re-dispatched to original topic with incremented `retry_count`
4. After `max_retries` (default: 5), status changes to `MANUAL_REVIEW`

**Manual Recovery** (for fatal errors):
1. Admin UI/CLI queries `dlq_messages WHERE status = 'MANUAL_REVIEW'`
2. Admin can inspect payload, edit, and re-dispatch or mark as `RESOLVED`
3. All actions are audit-logged

**DLQ Replay Service Configuration**:
```yaml
app:
  dlq:
    replay-interval-ms: 30000
    max-retries: 5
    backoff-base-seconds: 10
    batch-size: 50
```

### 9.5 Error Taxonomy & Routing-Decision (Worker-Seite)

Jeder Fehler im Worker wird eindeutig klassifiziert, bevor er auf `agent.execution.results`, `agent.execution.dlq` oder `agent.control.hitl` weitergeleitet wird:

| Fehlerklasse | Beispiele | Kafka-Routing | Retry im Worker |
| :--- | :--- | :--- | :--- |
| `TRANSIENT` | LLM Timeout, 429/Rate-Limit, 5xx Provider, `IOException`, DB-Conn-Peak | Retry mit Backoff (max. `maxRetryAttempts` aus AgentConfig) | ja |
| `FATAL` | Provider 4xx (InvalidRequest/Auth), ungültiges Eingabeformat, Guardrail-Verstoß | **direkt** `agent.execution.dlq` (kein Retry sinnvoll) | nein |
| `BUSINESS` | Logikfehler im Task (falsche Antwort, Validation) | `FAILED_RETRYABLE` mit max. N Versuchen, danach `NEEDS_HITL` | ja (begrenzt) |

**Klassifikationsregel (implementiert im `ErrorClassifier`):**
1. `FATAL` gewinnt immer (4xx-Providerfehler, Schema-/Payload-Fehler).
2. `BUSINESS`, wenn der Fehler in der Task-Logik/Validation liegt.
3. sonst `TRANSIENT`.

**Status-Mapping auf Result-Events:**

| Worker-Entscheidung | `status` in `agent.execution.results` | Routing des Orchestrators |
| :--- | :--- | :--- |
| Erfolg | `COMPLETED` | `evaluateRouting` (next steps) |
| Retry möglich | `FAILED_RETRYABLE` | Retry-Counter, Backoff, Re-Dispatch |
| Retry nicht sinnvoll | `FAILED_FATAL` | DLQ + HITL-Eskalation |
| Validierung / HITL | `NEEDS_HITL` | `agent.control.hitl` + Ticket |

**Single-Writer-Warnung (RQ §15.6):** Die Fehlerklassifikation passiert im Worker (Producer-Seite), die **Zustandsänderung** (Checkpoint/Retry/HITL) ausschließlich im `DagAdvanceHandler` bzw. `SweepReaperHandler` — der Worker schreibt selbst nie Routing-State.

---

## 10. Human-in-the-Loop (HITL) & Alerting

### 10.1 Escalation Triggers

A workflow transitions to `PAUSED_HITL` when:
- A task exceeds max retry attempts
- A timeout occurs and automated retries fail
- A guardrail service flags output as unsafe/low-confidence
- A DAG step has `requiresHumanApproval: true`

### 10.2 Alert Flow

```
[HITL Event]
    |
    +---> Write ticket to hitl_tickets (PostgreSQL)
    +---> Publish to agent.control.hitl (Kafka)
    +---> HITL Dispatcher Service
              |
              +---> Slack Webhook (interactive buttons)
              +---> PagerDuty API (high-severity trigger)
              +---> Email (backup notification)
```

### 10.3 Slack Interactive Payload

```json
{
  "text": "*AI Agent Workflow Interrupted - Action Required*",
  "blocks": [
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*Workflow:* `wf-9876-abc`\n*Step:* `code_generation_step`\n*Reason:* Execution failed after 3 retries (LLM Rate Limit)."
      }
    },
    {
      "type": "actions",
      "elements": [
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Retry Step" },
          "style": "primary",
          "action_id": "hitl_retry",
          "value": "{\"ticket_id\":\"t-123\",\"action\":\"retry\"}"
        },
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Skip & Continue" },
          "action_id": "hitl_skip",
          "value": "{\"ticket_id\":\"t-123\",\"action\":\"skip\"}"
        },
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Abort Workflow" },
          "style": "danger",
          "action_id": "hitl_abort",
          "value": "{\"ticket_id\":\"t-123\",\"action\":\"abort\"}"
        }
      ]
    }
  ]
}
```

### 10.4 RBAC for HITL Actions

| Role | Retry | Skip | Abort | View Context |
| :--- | :--- | :--- | :--- | :--- |
| `OPERATOR` | Yes | Yes | No | Limited |
| `ADMIN` | Yes | Yes | Yes | Full |
| `VIEWER` | No | No | No | Read-only |

Roles are enforced via Vault JWT/OIDC tokens validated by the HITL API.

### 10.5 HITL-Ticket-Lifecycle & Expiry

Jedes Ticket durchläuft einen expliziten, deterministischen Zustandsautomaten:

```
       created: OPEN ──────────► APPROVED (→ Re-Dispatch) 
            │                       │
            ├── retry ──► APPROVED  ├── skip   ──► SKIPPED (→ nächster Step)
            ├── skip  ──► SKIPPED   ├── abort  ──► ABORTED (→ workflow CANCELLED)
            └── abort ─► ABORTED    └── timeout ─► EXPIRED (→ Re-Eskalation)
                        ▲                              │
                        └──────── ESKALATIONSLEVEL ────┘
```

| Übergang | Auslöser | Wirkung |
| :--- | :--- | :--- |
| `OPEN → APPROVED` | Operator klickt `Retry Step` | Checkpoint wieder `IN_PROGRESS`, Task wird re-dispatched |
| `OPEN → SKIPPED` | Operator klickt `Skip & Continue` | Checkpoint `SKIPPED`, `evaluateRouting` läuft weiter |
| `OPEN → ABORTED` | Operator klickt `Abort Workflow` | Alle offenen Checkpoints `CANCELLED`, Execution `CANCELLED` |
| `OPEN → EXPIRED` | `ttl` erreicht (Default 24h, konfigurierbar) | Neue Eskalation mit höherer Dringlichkeit (PagerDuty hoch) |

**Expiry-Mechanismus (deterministisch):** Der `SweepReaperHandler` vergleicht bei jedem Tick `now() - created_at > ttl` für `OPEN`-Tickets (Wall-Clock nur hier erlaubt, §15.6). Nach `maxEscalationsPerTicket` (Default 3) auf `EXPIRED` wird das Ticket `RESOLVED` (humane Endentscheidung) und der Workflow bleibt in `PAUSED_HITL`, bis ein Admin manuell eingreift.

---

## 11. Security

### 11.1 HashiCorp Vault Integration

| Secret Type | Vault Path | Rotation | Purpose |
| :--- | :--- | :--- | :--- |
| PostgreSQL Credentials | `secret/data/db/postgres` | Dynamic (Vault DB engine) | DB authentication |
| Redis Password | `secret/data/db/redis` | Static, manual rotation | Redis AUTH |
| LLM API Keys | `secret/data/llm/{provider}` | Manual | OpenAI, Anthropic keys |
| Slack Webhook URL | `secret/data/alerting/slack` | Manual | Webhook secret |
| PagerDuty Routing Key | `secret/data/alerting/pagerduty` | Manual | PD integration |

### 11.2 Kafka Security

- **Authentication**: SASL/SCRAM or mTLS (configured via Strimzi operator)
- **Authorization**: Kafka ACLs per topic:
  - `agent-orchestrator`: WRITE on `agent.tasks.*`, `agent.control.*`, READ on `agent.execution.results`
  - `agent-worker`: READ on `agent.tasks.*`, WRITE on `agent.execution.results`
  - `agent-alerting`: READ on `agent.control.hitl`, WRITE on `agent.control.hitl`
- **Encryption**: TLS in transit for all Kafka communication

### 11.3 Application-Level Security

- JWT/OIDC tokens for API Gateway authentication
- Vault agent sidecar injects secrets as environment variables or shared volume
- All secrets are logged redacted (custom `logback` encoder)
- No secrets in application properties, environment variables, or code

---

## 12. Observability & Monitoring

### 12.1 Distributed Tracing

- W3C `traceparent` header injected into Kafka record headers via `TracingProducerInterceptor`
- Spans propagated across: REST calls, Kafka messages, DB queries, LLM API calls
- OpenTelemetry Java Agent auto-instrumentation via `-javaagent`

### 12.2 Key Prometheus Metrics

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `kafka_consumer_lag_records` | Gauge | Consumer group lag per topic/partition |
| `agent_task_execution_duration_seconds` | Histogram | Execution time per agent type |
| `llm_token_usage_total` | Counter | Accumulated tokens per model |
| `llm_api_call_duration_seconds` | Histogram | LLM provider response time |
| `llm_api_call_errors_total` | Counter | LLM failures by error type |
| `workflow_execution_total` | Counter | Total workflows by status |
| `workflow_step_duration_seconds` | Histogram | Per-step execution time |
| `hitl_ticket_open_count` | Gauge | Active unresolved tickets |
| `circuit_breaker_state` | Gauge | Resilience4j state per provider |
| `dlq_messages_pending_count` | Gauge | Unprocessed DLQ messages |
| `dlq_replay_success_total` | Counter | Successful DLQ replays |
| `dlq_replay_failure_total` | Counter | Failed DLQ replays |
| `routing_decision_total` | Counter | DECISION evaluations, labelled by trigger (condition/guard/loop-exit) |
| `routing_pruned_steps_total` | Counter | Steps skipped via pruning/guard (SKIPPED/PRUNED) |
| `loop_iteration_total` | Counter | Loop iterations per step |
| `loop_exit_reason_total` | Counter | Loop exits by reason: `EXIT_CONDITION` / `MAX_ITERATIONS` |
| `join_wait_duration_seconds` | Histogram | Time a JOIN waits for its branches |
| `hitl_ticket_escalation_total` | Counter | HITL re-escalations after expiry |

### 12.3 Grafana Dashboards

1. **Workflow Overview**: Active workflows, success/failure rates, P50/P95/P99 execution times
2. **Agent Worker Health**: Consumer lag, task throughput, error rates per agent type
3. **LLM Provider Health**: Token usage, cost tracking, latency, circuit breaker state
4. **HITL Operational**: Open tickets, resolution times, escalation frequency
5. **DLQ Monitor**: Pending messages, replay success rate, error distribution

---

## 13. Maven Project Structure

```
ai-agent-platform/
├── pom.xml                          (Parent BOM)
├── agent-domain/
│   ├── pom.xml
│   └── src/main/java/com/platform/domain/
│       ├── model/                   (JPA Entities)
│       ├── dto/                     (JSON Schema DTOs)
│       └── event/                   (Kafka Event Models)
├── agent-orchestrator/
│   ├── pom.xml
│   └── src/main/java/com/platform/orchestrator/
│       ├── engine/                  (DAG Engine, Graph Parser, Routing Evaluator)
│       ├── service/                 (Checkpoint, Dispatch, Timeout, Loop State)
│       └── consumer/                (DagAdvanceHandler, LoopIterationHandler, SweepReaperHandler, CancellationHandler)
├── agent-worker/
│   ├── pom.xml
│   └── src/main/java/com/platform/worker/
│       ├── config/                  (Dynamic Config Registry)
│       ├── consumer/                (Task Consumer)
│       ├── llm/                     (LLM Client, Circuit Breaker)
│       └── dedup/                   (Redis Dedup Service)
├── agent-alerting/
│   ├── pom.xml
│   └── src/main/java/com/platform/alerting/
│       ├── dispatcher/              (Multi-Channel Dispatcher)
│       ├── slack/                   (Slack Webhook Client)
│       ├── pagerduty/               (PD API Client)
│       └── consumer/                (HITL Event Consumer)
├── agent-dlq-replay/
│   ├── pom.xml
│   └── src/main/java/com/platform/dlq/
│       ├── service/                 (DLQ Replay Logic)
│       └── consumer/                (DLQ Topic Consumer)
└── agent-common/
    ├── pom.xml
    └── src/main/java/com/platform/common/
        ├── vault/                   (Vault Config, Secret Resolver)
        ├── tracing/                 (OpenTelemetry Helpers)
        └── config/                  (Shared Application Properties)
```

### 13.1 Root pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.platform</groupId>
    <artifactId>ai-agent-platform</artifactId>
    <version>2.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>AI Agent Platform Parent</name>

    <modules>
        <module>agent-common</module>
        <module>agent-domain</module>
        <module>agent-orchestrator</module>
        <module>agent-worker</module>
        <module>agent-alerting</module>
        <module>agent-dlq-replay</module>
    </modules>

    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <spring-boot.version>3.3.3</spring-boot.version>
        <spring-kafka.version>3.2.3</spring-kafka.version>
        <opentelemetry.version>1.38.0</opentelemetry.version>
        <resilience4j.version>2.2.0</resilience4j.version>
        <confluent.version>7.6.0</confluent.version>
        <vault.java.version>4.6.0</vault.java.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>io.github.resilience4j</groupId>
                <artifactId>resilience4j-spring-boot3</artifactId>
                <version>${resilience4j.version}</version>
            </dependency>
            <dependency>
                <groupId>io.confluent</groupId>
                <artifactId>kafka-json-schema-serializer</artifactId>
                <version>${confluent.version}</version>
            </dependency>
            <dependency>
                <groupId>org.hashicorp.vault</groupId>
                <artifactId>vault-java-driver</artifactId>
                <version>${vault.java.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
                <configuration>
                    <source>21</source>
                    <target>21</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## 14. Core Java Implementation

### 14.1 Dynamic Agent Config Registry

```java
package com.platform.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class DynamicAgentConfigRegistry {

    private static final Logger log = LoggerFactory.getLogger(DynamicAgentConfigRegistry.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Map<String, AgentConfig> configCache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public DynamicAgentConfigRegistry(JdbcTemplate jdbcTemplate,
                                       ObjectMapper objectMapper,
                                       MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void initialize() {
        log.info("Loading initial agent definitions from PostgreSQL...");
        reloadAllConfigs();
        scheduler.scheduleAtFixedRate(this::reloadAllConfigs, 5, 5, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdown();
    }

    public void reloadAllConfigs() {
        String sql = """
            SELECT agent_type, system_prompt, model_name, temperature,
                   max_tokens, tools_config, timeout_seconds, max_retry_attempts, version
            FROM agent_definitions
            """;

        jdbcTemplate.query(sql, rs -> {
            String agentType = rs.getString("agent_type");
            AgentConfig config = new AgentConfig(
                agentType,
                rs.getString("system_prompt"),
                rs.getString("model_name"),
                rs.getDouble("temperature"),
                rs.getInt("max_tokens"),
                rs.getString("tools_config"),
                rs.getInt("timeout_seconds"),
                rs.getInt("max_retry_attempts"),
                rs.getInt("version")
            );
            configCache.put(agentType, config);
        });

        meterRegistry.gauge("agent.config.loaded_count", configCache.size());
        log.info("Loaded {} agent configurations.", configCache.size());
    }

    public AgentConfig getConfig(String agentType) {
        AgentConfig config = configCache.get(agentType);
        if (config == null) {
            meterRegistry.counter("agent.config.miss_total", "agentType", agentType).increment();
            throw new IllegalArgumentException("No configuration for agent type: " + agentType);
        }
        return config;
    }

    @KafkaListener(topics = "agent.config.updates", groupId = "agent-worker-config-group")
    public void onConfigUpdateEvent(String message) {
        try {
            ConfigUpdatePayload payload = objectMapper.readValue(message, ConfigUpdatePayload.class);
            reloadSingleConfig(payload.agentType());
            log.info("Dynamic config update applied for agent type: {}", payload.agentType());
        } catch (Exception e) {
            log.error("Failed to process config update event", e);
            meterRegistry.counter("agent.config.update_errors_total").increment();
        }
    }

    private void reloadSingleConfig(String agentType) {
        String sql = """
            SELECT agent_type, system_prompt, model_name, temperature,
                   max_tokens, tools_config, timeout_seconds, max_retry_attempts, version
            FROM agent_definitions WHERE agent_type = ?
            """;

        jdbcTemplate.query(sql, rs -> {
            AgentConfig config = new AgentConfig(
                rs.getString("agent_type"),
                rs.getString("system_prompt"),
                rs.getString("model_name"),
                rs.getDouble("temperature"),
                rs.getInt("max_tokens"),
                rs.getString("tools_config"),
                rs.getInt("timeout_seconds"),
                rs.getInt("max_retry_attempts"),
                rs.getInt("version")
            );
            configCache.put(agentType, config);
        }, agentType);
    }

    public record AgentConfig(
        String agentType,
        String systemPrompt,
        String modelName,
        double temperature,
        int maxTokens,
        String toolsConfigJson,
        int timeoutSeconds,
        int maxRetryAttempts,
        int version
    ) {}

    public record ConfigUpdatePayload(String agentType, int newVersion) {}
}
```

### 14.2 Orchestrator DAG Engine

```java
package com.platform.orchestrator.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DagEngine {

    private static final Logger log = LoggerFactory.getLogger(DagEngine.class);

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public DagEngine(JdbcTemplate jdbcTemplate,
                     KafkaTemplate<String, String> kafkaTemplate,
                     ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void startWorkflow(String workflowExecutionId) {
        String dagJson = jdbcTemplate.queryForObject(
            "SELECT wd.dag_structure FROM workflow_executions we " +
            "JOIN workflow_definitions wd ON we.workflow_definition_id = wd.id " +
            "WHERE we.id = ?",
            String.class, workflowExecutionId
        );

        JsonNode dag = objectMapper.valueToTree(objectMapper.convertValue(dagJson, Object.class));
        JsonNode nodes = dag.get("nodes");

        // Initialize all step checkpoints as PENDING
        for (JsonNode node : nodes) {
            String stepId = node.get("stepId").asText();
            String agentType = node.get("agentType").asText();
            UUID taskId = UUID.randomUUID();

            jdbcTemplate.update("""
                INSERT INTO workflow_checkpoints
                    (workflow_execution_id, step_id, agent_type, task_id, status, created_at)
                VALUES (?, ?, ?, ?, 'PENDING', ?)
                ON CONFLICT (workflow_execution_id, step_id) DO NOTHING
                """,
                workflowExecutionId, stepId, agentType, taskId, Instant.now()
            );
        }

        // Update workflow status
        jdbcTemplate.update(
            "UPDATE workflow_executions SET status = 'RUNNING', started_at = ? WHERE id = ?",
            Instant.now(), workflowExecutionId
        );

        // Dispatch READY steps (no dependencies)
        dispatchReadySteps(workflowExecutionId, nodes);
    }

    private void dispatchReadySteps(String executionId, JsonNode nodes) {
        List<String> readySteps = new ArrayList<>();

        for (JsonNode node : nodes) {
            String stepId = node.get("stepId").asText();
            JsonNode dependsOn = node.get("dependsOn");

            if (dependsOn == null || dependsOn.isEmpty()) {
                readySteps.add(stepId);
                continue;
            }

            // Check if all dependencies are COMPLETED
            boolean allCompleted = true;
            for (JsonNode dep : dependsOn) {
                String depStatus = jdbcTemplate.queryForObject(
                    "SELECT status FROM workflow_checkpoints " +
                    "WHERE workflow_execution_id = ? AND step_id = ?",
                    String.class, executionId, dep.asText()
                );
                if (!"COMPLETED".equals(depStatus)) {
                    allCompleted = false;
                    break;
                }
            }

            if (allCompleted) {
                readySteps.add(stepId);
            }
        }

        for (String stepId : readySteps) {
            dispatchTask(executionId, stepId, nodes);
        }
    }

    private void dispatchTask(String executionId, String stepId, JsonNode nodes) {
        JsonNode node = findNode(nodes, stepId);
        String agentType = node.get("agentType").asText();

        // Mark IN_PROGRESS
        jdbcTemplate.update(
            "UPDATE workflow_checkpoints SET status = 'IN_PROGRESS', started_at = ? " +
            "WHERE workflow_execution_id = ? AND step_id = ?",
            Instant.now(), executionId, stepId
        );

        // Get task_id for timeout scheduling
        UUID taskId = jdbcTemplate.queryForObject(
            "SELECT task_id FROM workflow_checkpoints " +
            "WHERE workflow_execution_id = ? AND step_id = ?",
            UUID.class, executionId, stepId
        );

        // Build and send task event
        String taskEvent = buildTaskEvent(executionId, taskId, agentType);
        kafkaTemplate.send("agent.tasks." + agentType, executionId, taskEvent);

        // Schedule timeout
        String timeoutEvent = String.format(
            "{\"taskId\":\"%s\",\"workflowId\":\"%s\",\"deadlineAt\":\"%s\"}",
            taskId, executionId,
            Instant.now().plusSeconds(120).toString()
        );
        kafkaTemplate.send("agent.control.timeouts", taskId.toString(), timeoutEvent);

        log.info("Dispatched task {} for step {} in execution {}", taskId, stepId, executionId);
    }

    public void handleResult(String executionId, UUID taskId, String status, String outputData) {
        String stepId = jdbcTemplate.queryForObject(
            "SELECT step_id FROM workflow_checkpoints WHERE task_id = ?",
            String.class, taskId
        );

        if ("COMPLETED".equals(status)) {
            jdbcTemplate.update(
                "UPDATE workflow_checkpoints SET status = 'COMPLETED', output_payload = ?::jsonb, completed_at = ? " +
                "WHERE task_id = ?",
                outputData, Instant.now(), taskId
            );
        } else if ("FAILED_RETRYABLE".equals(status)) {
            handleRetry(executionId, taskId, stepId);
            return;
        } else {
            // FAILED_FATAL or NEEDS_HITL
            createHitlTicket(executionId, taskId, stepId, status);
            return;
        }

        // Check if workflow is complete
        String dagJson = jdbcTemplate.queryForObject(
            "SELECT wd.dag_structure FROM workflow_executions we " +
            "JOIN workflow_definitions wd ON we.workflow_definition_id = wd.id " +
            "WHERE we.id = ?",
            String.class, executionId
        );
        JsonNode dag = objectMapper.valueToTree(objectMapper.convertValue(dagJson, Object.class));

        dispatchReadySteps(executionId, dag.get("nodes"));
        checkWorkflowCompletion(executionId);
    }

    private void handleRetry(String executionId, UUID taskId, String stepId) {
        int retryCount = jdbcTemplate.queryForObject(
            "SELECT retry_count FROM workflow_checkpoints WHERE task_id = ?",
            Integer.class, taskId
        );

        int maxRetries = 3; // Loaded from agent config

        if (retryCount >= maxRetries) {
            createHitlTicket(executionId, taskId, stepId, "MAX_RETRIES_EXCEEDED");
            return;
        }

        long delaySeconds = Math.min((long) Math.pow(2, retryCount), 60);
        jdbcTemplate.update(
            "UPDATE workflow_checkpoints SET retry_count = retry_count + 1, status = 'PENDING' WHERE task_id = ?",
            taskId
        );

        // Re-dispatch after delay (simplified; production uses scheduler)
        String taskEvent = buildTaskEvent(executionId, taskId,
            jdbcTemplate.queryForObject(
                "SELECT agent_type FROM workflow_checkpoints WHERE task_id = ?",
                String.class, taskId
            )
        );
        kafkaTemplate.send("agent.tasks." +
            jdbcTemplate.queryForObject(
                "SELECT agent_type FROM workflow_checkpoints WHERE task_id = ?",
                String.class, taskId
            ),
            executionId, taskEvent
        );
    }

    private void createHitlTicket(String executionId, UUID taskId, String stepId, String reason) {
        jdbcTemplate.update(
            "UPDATE workflow_checkpoints SET status = 'WAITING_HITL' WHERE task_id = ?",
            taskId
        );

        jdbcTemplate.update(
            "UPDATE workflow_executions SET status = 'PAUSED_HITL' WHERE id = ?",
            executionId
        );

        UUID ticketId = UUID.randomUUID();
        jdbcTemplate.update("""
            INSERT INTO hitl_tickets (id, workflow_execution_id, reason, status, context_data, created_at)
            VALUES (?, ?, ?, 'OPEN', ?::jsonb, ?)
            """,
            ticketId, executionId, reason,
            String.format("{\"taskId\":\"%s\",\"stepId\":\"%s\"}", taskId, stepId),
            Instant.now()
        );

        log.warn("HITL ticket {} created for step {} in execution {}", ticketId, stepId, executionId);
    }

    private void checkWorkflowCompletion(String executionId) {
        long pendingCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM workflow_checkpoints " +
            "WHERE workflow_execution_id = ? AND status NOT IN ('COMPLETED', 'SKIPPED', 'CANCELLED')",
            Long.class, executionId
        );

        if (pendingCount == 0) {
            jdbcTemplate.update(
                "UPDATE workflow_executions SET status = 'COMPLETED', completed_at = ? WHERE id = ?",
                Instant.now(), executionId
            );
            log.info("Workflow {} completed successfully.", executionId);
        }
    }

    private String buildTaskEvent(String executionId, UUID taskId, String agentType) {
        return String.format(
            "{\"header\":{\"workflowId\":\"%s\",\"taskId\":\"%s\",\"traceParent\":\"%s\"," +
            "\"attempt\":1,\"maxAttempts\":3,\"createdAt\":\"%s\",\"timeoutMs\":120000}," +
            "\"agentConfig\":{\"agentType\":\"%s\",\"version\":1}," +
            "\"payload\":{\"inputPrompt\":\"Execute task\"}}",
            executionId, taskId, UUID.randomUUID(), Instant.now(), agentType
        );
    }

    private JsonNode findNode(JsonNode nodes, String stepId) {
        for (JsonNode node : nodes) {
            if (stepId.equals(node.get("stepId").asText())) {
                return node;
            }
        }
        throw new IllegalArgumentException("Step not found: " + stepId);
    }
}
```

### 14.3 Agent Task Consumer

```java
package com.platform.worker.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.worker.config.DynamicAgentConfigRegistry;
import com.platform.worker.config.DynamicAgentConfigRegistry.AgentConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class GenericAgentTaskConsumer {

    private static final Logger log = LoggerFactory.getLogger(GenericAgentTaskConsumer.class);

    private final DynamicAgentConfigRegistry configRegistry;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public GenericAgentTaskConsumer(DynamicAgentConfigRegistry configRegistry,
                                    StringRedisTemplate redisTemplate,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ObjectMapper objectMapper,
                                    MeterRegistry meterRegistry) {
        this.configRegistry = configRegistry;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(
        topics = "#{'${app.kafka.agent-topics}'.split(',')}",
        groupId = "agent-worker-execution-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAgentTask(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String taskId = null;
        Instant startTime = Instant.now();
        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            JsonNode taskEvent = objectMapper.readTree(record.value());
            taskId = taskEvent.get("header").get("taskId").asText();
            String agentType = taskEvent.get("agentConfig").get("agentType").asText();

            log.info("Processing task {} for agent type {}", taskId, agentType);

            // 1. Redis dedup check
            String dedupKey = "dedup:task:" + taskId;
            Boolean isFirst = redisTemplate.opsForValue()
                .setIfAbsent(dedupKey, "IN_PROGRESS", Duration.ofHours(24));
            if (Boolean.FALSE.equals(isFirst)) {
                log.warn("Duplicate task detected: {}. Skipping.", taskId);
                meterRegistry.counter("worker.dedup_skip_total").increment();
                ack.acknowledge();
                return;
            }

            // 2. Acquire distributed lock
            String lockKey = "lock:task:" + taskId;
            Boolean locked = redisTemplate.opsForValue()
                .setIfAbsent(lockKey, "LOCKED", Duration.ofMinutes(5));
            if (Boolean.FALSE.equals(locked)) {
                log.warn("Could not acquire lock for task {}. Skipping.", taskId);
                ack.acknowledge();
                return;
            }

            try {
                // 3. Fetch config
                AgentConfig config = configRegistry.getConfig(agentType);

                // 4. Execute LLM call
                ExecutionResult result = executeAgentLogic(taskEvent, config);

                // 5. Publish result
                publishResult(
                    taskEvent.get("header").get("workflowId").asText(),
                    taskId, agentType, "COMPLETED", result.outputData()
                );

                meterRegistry.counter("worker.task_success_total",
                    "agentType", agentType).increment();
                log.info("Task {} completed successfully.", taskId);

            } finally {
                redisTemplate.delete(lockKey);
            }

            ack.acknowledge();

        } catch (Exception e) {
            log.error("Error processing task {}", taskId, e);
            meterRegistry.counter("worker.task_failure_total").increment();
            handleExecutionFailure(record, e);
            ack.acknowledge();
        } finally {
            sample.stop(meterRegistry.timer("worker.task_execution_duration"));
        }
    }

    private ExecutionResult executeAgentLogic(JsonNode taskEvent, AgentConfig config) {
        log.info("Invoking model {} with temperature {}", config.modelName(), config.temperature());
        // Placeholder: replace with actual LLM client call
        return new ExecutionResult("{\"status\":\"SUCCESS\",\"generatedText\":\"Sample output\"}");
    }

    private void publishResult(String workflowId, String taskId, String agentType,
                               String status, String data) {
        try {
            JsonNode result = objectMapper.createObjectNode()
                .putObject("header")
                .put("workflowId", workflowId)
                .put("taskId", taskId)
                .put("agentType", agentType)
                .put("status", status)
                .put("completedAt", Instant.now().toString());
            // In production: use proper ObjectMapper construction
            String payload = objectMapper.writeValueAsString(result);
            kafkaTemplate.send("agent.execution.results", workflowId, payload);
        } catch (Exception e) {
            log.error("Failed to publish result for task {}", taskId, e);
            meterRegistry.counter("worker.result_publish_errors_total").increment();
        }
    }

    private void handleExecutionFailure(ConsumerRecord<String, String> record, Exception e) {
        String errorClass = e.getClass().getSimpleName();
        String errorMessage = e.getMessage();

        // Classify: transient vs fatal
        boolean isTransient = e instanceof java.io.IOException
                           || e instanceof java.util.concurrent.TimeoutException
                           || e instanceof org.springframework.web.client.HttpServerErrorException;

        if (isTransient) {
            log.warn("Transient error, will retry via normal Kafka retry. Error: {}", errorClass);
        } else {
            log.error("Fatal error, routing to DLQ. Error: {}", errorClass);
            kafkaTemplate.send("agent.execution.dlq", record.key(), record.value());
        }
    }

    public record ExecutionResult(String outputData) {}
}
```

### 14.4 DLQ Replay Service

```java
package com.platform.dlq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public DlqReplayService(JdbcTemplate jdbcTemplate, KafkaTemplate<String, String> kafkaTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${app.dlq.replay-interval-ms:30000}")
    public void replayPendingMessages() {
        List<Map<String, Object>> pending = jdbcTemplate.queryForList(
            "SELECT id, topic, original_key, payload, retry_count, max_retries " +
            "FROM dlq_messages " +
            "WHERE status = 'PENDING' AND next_retry_at <= ? " +
            "ORDER BY created_at ASC " +
            "LIMIT ?",
            Instant.now(), 50
        );

        for (Map<String, Object> msg : pending) {
            String id = msg.get("id").toString();
            String topic = (String) msg.get("topic");
            String key = (String) msg.get("original_key");
            String payload = msg.get("payload").toString();
            int retryCount = (int) msg.get("retry_count");
            int maxRetries = (int) msg.get("max_retries");

            try {
                kafkaTemplate.send(topic, key, payload).get();
                jdbcTemplate.update(
                    "UPDATE dlq_messages SET status = 'REPLAYED', resolved_at = ? WHERE id = ?",
                    Instant.now(), id
                );
                log.info("Replayed DLQ message {} to topic {}", id, topic);
            } catch (Exception e) {
                if (retryCount + 1 >= maxRetries) {
                    jdbcTemplate.update(
                        "UPDATE dlq_messages SET status = 'MANUAL_REVIEW', " +
                        "retry_count = retry_count + 1, last_attempt_at = ? WHERE id = ?",
                        Instant.now(), id
                    );
                    log.warn("DLQ message {} exceeded max retries, escalated to MANUAL_REVIEW", id);
                } else {
                    long backoff = (long) Math.pow(10, retryCount + 1);
                    jdbcTemplate.update(
                        "UPDATE dlq_messages SET retry_count = retry_count + 1, " +
                        "next_retry_at = ?, last_attempt_at = ? WHERE id = ?",
                        Instant.now().plusSeconds(backoff), Instant.now(), id
                    );
                    log.warn("DLQ replay failed for {}, retry {} scheduled", id, retryCount + 1);
                }
            }
        }
    }
}
```

---

## 15. Deterministic Workflow Routing: Loops, Conditions, Fork/Join

### 15.1 Ziel & Determinismus-Garantien

Alle Routing-Entscheidungen (welche Steps feuern als Nächstes, welche Branches werden gewählt, welche Schleifen laufen weiter) werden über eine **einzige pure Funktion** berechnet:

```
fireableSteps = evaluateRouting(persistedCheckpointState, dagDefinitionVersion)
```

Die Funktion ist frei von Nebeneffekten: Sie liest ausschließlich das persistierte Zustandsbild (`workflow_checkpoints`) und die zum Start gepinnte DAG-Definition. Identische Eingabezustände ergeben **immer** identische Mengen an zu dispatchenden Steps.

| # | Garantie | Mechanismus |
| :--- | :--- | :--- |
| G1 | **Single-Writer pro Workflow** | Alle Routing-Topics sind mit `workflow_id` keyed → pro Workflow immer dieselbe Partition. Genau ein aktiver Orchestrator besitzt die Partition (K8s-Lease / Spring JDBC-Lock). |
| G2 | **Atomare Übergänge** | Zustandsübergang + Bestimmung der nächsten Steps + Dispatch-Send in **einer** DB-Transaktion. Kafka-Offset wird erst nach `COMMIT` committet. |
| G3 | **Idempotentes Re-Execute** | Der Evaluator über den selben persistenten Zustand liefert dieselben Fireable-Steps. Doppelte/erneut gelieferte Events sind No-Ops dank `uq_execution_step_iteration` (Unique-Constraint der Dispatch-Ops). |
| G4 | **Keine Nicht-Determinismus-Quellen im Routing-Pfad** | Keine Wanduhr, kein `random()`, kein externes HTTP in Bedingungs-/Schleifen-/Join-Auswertung. Zeit taucht nur in Timeout- und Sweep-Events auf (dort als Grenzwert, nicht als Routing-Input). |
| G5 | **Gepingte DAG-Version** | Routing nutzt immer die `workflow_definitions`-Version, mit der die Execution gestartet wurde. Laufende Definitionen ändern sich nicht unter einer Execution. |
| G6 | **Termination** | Loops sind durch `maxIterations` beschränkt. Nicht-LOOP-Zyklen werden beim Upload durch den Workflow-Validator abgelehnt. |

### 15.2 Zustandsmodell der Knoten

Jeder Schritt im DAG hat genau **eine** Checkpoint-Zeile pro Iteration in `workflow_checkpoints` (erweitert um `iteration`, `node_type`, `decision_branch`).

| Status | Bedeutung | Kann von Evaluator gesetzt werden |
| :--- | :--- | :--- |
| `PENDING` | Noch nicht dispatched | nein — Verbleibt bis `evaluateRouting` feuert |
| `IN_PROGRESS` | Task ist an Worker ausgegangen | ja (beim Dispatch) |
| `COMPLETED` | Step erfolgreich | ja |
| `FAILED` | Nicht mehr retrybar (fatal) | ja |
| `WAITING_HITL` | Menschliche Entscheidung nötig | ja |
| `SKIPPED` | Durch DECISION-Pruning abgewählt | ja (deterministisch kaskadierend) |
| `PRUNED` | Fork-Branch verworfen (XOR) | ja |
| `CANCELLED` | Workflow abgebrochen / Join nicht mehr erreichbar | ja |

Ordnungsregel für Zustandsübergänge: `PENDING → IN_PROGRESS → {COMPLETED, FAILED, WAITING_HITL}`, danach optional `→ SKIPPED/PRUNED/CANCELLED` nur durch den Evaluator. Kein Handler außer `evaluateRouting` darf Routing-Status schreiben.

### 15.3 Der Deterministic Routing Evaluator (Kern)

```
level(s) = 1 + max( level(s') for s' in s.dependsOn )        // 0 für Wurzeln

evaluateRouting(state, dag):
  result = []
  ready = [s in dag.nodes if s.status == PENDING]

  for s in sort(ready, by=(level, stepId)):                   // deterministische Reihenfolge
    if s is branch-step and branchOf(s) is PRUNED:
        mark(s, PRUNED); continue
    if satisfied(s, state):
        result.append(s)

  return result

satisfied(s):
  switch s.nodeType:
    STEP/LOOP/END:  all(edgeCompleted(d → s) for d in s.dependsOn)
    DECISION:       all(edgeCompleted(d → s) for d in s.dependsOn)
    FORK:           atLeastOne(edgeCompleted(d → s) for d in s.dependsOn)
    JOIN:
      incoming = [d in s.dependsOn if d.status not in {PRUNED, CANCELLED}]
      effective = [d in incoming if d.status in {COMPLETED, SKIPPED}]
      switch joinType:
        AND:    all(incoming ⊆ effective)
        XOR:    any(d in COMPLETED for d in incoming)
        N_OF:   count(effective) >= joinN

edgeCompleted(d → s):
  if d.status != COMPLETED: return false
  // DECISION-Kanten nur als aktive Kante
  if d.nodeType == DECISION:
    return d.decision_branch == s.stepId
  return true
```

**STEP-Guards (bedingtes Ausführen eines Schrittes):** Ein `STEP` kann optional `guardCondition` besitzen (`{condition}`). Die Auswertung erfolgt **pur in `evaluateRouting`**, direkt vor dem Feuern, gegen den persistierten Kontext (`input_params`, `context:workflow:{id}`, Completed-Outputs):

```
STEP:  all(edgeCompleted(d → s) ...)
       UND guardCheck(s):
         wenn s.guardCondition definiert:
             guardErfüllt = eval(s.guardCondition, persistedContext)
             wenn guardErfüllt == false  →  mark(s, SKIPPED)   // Skip-durch, kein Task
```

Ein `SKIPPED`-Guard-Step zählt downstream wie ein `PRUNED`-Step als „effektiv erfüllt" (§15.5 AND-JOIN): Er wird übersprungen, ohne den Fluss zu blockieren. Die **Entscheidung wird persistiert** (`status=SKIPPED`, Grund in `decision_branch` = `guard:false`) → Replay-durchgänge identisch.

**Guards sind deterministisch geschrieben:** Es gelten dieselben Purity-Regeln wie bei DECISION (G4): nur persisted Context, kein `now()`, kein `random()`, keine externen Calls.

**Kernpunkte für Determinismus:**
- **Reihenfolge ist fixiert**: Sortierung nach `(level, stepId)` — kein Parallelismus im Evaluator selbst, keine Thread-timing-Abhängigkeit.
- **Kanten-Aktivierung bei DECISION**: Ein `DECISION` persistiert `decision_branch` (den gewählten Target-Step). Nur die Kante zu diesem Target ist „aktiv“ — alle anderen ausgehenden Kanten erfüllen `edgeCompleted` nie. Loop-Back (DECISION → LOOP) ist dadurch syntaxfrei modellierbar, ohne dass der nicht gewählte Pfad feuert (vgl. Trace §15.7).
- **Pruning ist kaskadierend und deterministisch**: Wenn eine DECISION Branch X wählt, werden die nicht gewählten Branch-Köpfe (`PRUNED`) und deren Nachfolger rekursiv `SKIPPED` markiert — gleiche Sortierung, gleiche Reihenfolge.
- **Skips zählen für JOIN als „effektiv erfüllt“**: Ein durch Pruning geprunter Skip zählt als Completed-Äquivalent („keine echte Arbeit mehr“), zählt aber **nicht** als erfolgreiche Arbeit bei AND.
- **Entscheidungen werden persistent**: `decision_branch` und `iteration` auf der Checkpoint-Zeile → Replay nach Crash liefert identische Ergebnisse.

### 15.4 Loops (bounded)

- Der `LOOP`-Knoten in der DAG-Definition ist **zustandslos**: Pro Iteration existiert genau eine Checkpoint-Zeile `(execution_id, step_id, iteration=i)`, `node_type='LOOP'`, `agent_type=bodyAgentType`.
- Nach Abschluss von Iteration i (COMPLETED) schreibt der **LoopIterationHandler** den Zustand, dann:
  1. Wird `exitCondition` gegen `context['LOOP-stepId'].resultData` ausgewertet — **pur**, nur auf bereits persistierten Output-Daten.
  2. `true` → Checkpoint LOOP mit `iteration=i` → `COMPLETED`; `evaluateRouting` läuft danach normal weiter (Successor des LOOP feuert).
  3. `false` und `i < maxIterations` → nächster Dispatch als Iteration `i+1` (neue Zeile, neue taskId).
  4. `false` und `i == maxIterations` → `WAITING_HITL` (menschliche Entscheidung: Abbruch oder manuelles Weiterführen der Arbeit).
- **Terminationsbeweis**: Der Zähler `iteration` ist nur in der DB (nie im RAM), der Evaluator kann höchstens um 1 hochzählen bis `maxIterations` → G6.
- **Dispatch-Event**: Loop-Advance läuft über `agent.control.loop` (Partition-Key `workflow_id`); im Payload stehen `stepId` und `iteration`. Der Handler ignoriert Iterationen, deren Checkpoint bereits einen anderen Status als `PENDING/IN_PROGRESS` hat (Idempotenz).

### 15.5 Fork / Join

- **FORK**: Beim Feuern wird jeder Branch-Kopf (`firstStep`) als `PENDING` belassen und in der nächsten `evaluateRouting`-Runde dispatched — die Partitions-Ordnung serialisiert die Dispatches, die Worker führen aber parallel aus.
- **AND-JOIN**: Feuert erst, wenn alle Nicht-`PRUNED`-Dependencies `COMPLETED` (oder durch Pruning `SKIPPED`) sind. Ein Join, dessen alle Dependencies gepruned/cancelled wurden, wird selbst `SKIPPED` (nie hängen bleiben).
- **XOR-JOIN (race)**: Wird der Stand erfüllt, wenn **irgendeine** Branch completed ist; die übrigen Branches werden deterministisch `PRUNED`.
- **N_OF-JOIN**: Feuert bei `joinN` abgeschlossenen Branches; restliche In-Flight-Branches werden nach Abschluss ebenfalls `PRUNED`.
- **Branch-Failure-Semantik** (falls eine eingehende Branch nicht COMPLETED/SKIPPED/PRUNED endet):
  - **`FAILED` (fatal)**: Die Branch gilt als **blockierend**. AND/N_OF/JOIN wird NICHT erfüllt → `evaluateRouting` feuert nie; der SweepReaperHandler (`agent.control.sweep`) erkennt den hängenden JOIN nach Ablauf des Join-Deadlines und setzt ihn `WAITING_HITL`. Der Workflow wartet auf eine menschliche Entscheidung (Retry der Branch / Skip-Join / Abort).
  - **`WAITING_HITL`**: Blockiert ebenfalls; erst nach Auflösung durch `HitlResumeHandler` wird `evaluateRouting` neu ausgewertet.
  - **Alternative Sicht**: `joinOnAny` (`XOR`) ist das einzige Muster, das Verzweigungsfehler „weiterleitet" (die erste erfolgreiche Branch gewinnt, Fehler-Branches werden `PRUNED`).
- Kein Broadcast, kein Join-Probe-Topic nötig: Join-Readiness wird **immer** beim Verarbeiten eines Result-Events im DagAdvanceHandler nachgerechnet — derselbe Handler, gleiche Reihenfolge ⇒ deterministisch.

### 15.6 Nötige Nicht-Agent-Kafka-Message-Handler

Zusätzlich zu den bereits definierten (Timeout-Monitor, Config-Update, DLQ-Replay) sind für resiliente deterministische Routing-Logik folgende Handler nötig:

| Handler | Consumer-Gruppe | Topics (in) | Aufgabe | Determinismus-Rolle |
| :--- | :--- | :--- | :--- | :--- |
| **DagAdvanceHandler** (Result Finalizer) | `orchestrator-dag-group` | `agent.execution.results` | **Einziger** Schreibpfad für Routing: Transition + `evaluateRouting` + Dispatch, alles in einer DB-Transaktion (G2, G3) | Kern — G1–G6 |
| **LoopIterationHandler** | `orchestrator-dag-group` | `agent.control.loop` | Zählt Iterationen, prüft `exitCondition`, dispatcht weiter oder schließt den LOOP | G6 (Termination) + Idempotenz |
| **JoinEvaluator** (Teil des DagAdvance, kein eigenes Topic) | — | `agent.execution.results` | Berechnet Join-Readiness bei jedem Branch-Result deterministisch | Deterministische Join-Semantik (AND/XOR/N_OF) |
| **SweepReaperHandler** | `orchestrator-reaper-group` | `agent.control.sweep` | Periodische Ticks: finde stale `IN_PROGRESS` (Crash-Recovery), hängengebliebene JOIN/LOOP, entscheide retry/HITL — **nur hier** darf Wall-Clock in die Logik einfließen | G4-Begrenzung der Zeitabhängigkeit |
| **CancellationHandler** | `orchestrator-dag-group` | `agent.control.cancellation` | Bricht Workflow deterministisch ab: markiert alle `PENDING`-Steps in Level-/ID-Reihenfolge `CANCELLED`, pruned Forks, schließt JOIN | Deterministisches Pruning |
| **HitlResumeHandler** | `alerting-hitl-group` | `agent.control.hitl` + REST-Callback von Slack/PD | Setzt Retry-/Skip-/Abort-Entscheidungen in Routing-Schritte um und ruft `evaluateRouting` erneut auf | Deterministische Entscheidungskette nach menschlichem Input |
| **DecisionEvaluator (optional, async)** | `orchestrator-condition-group` | `agent.control.condition` | Alternative asynchrone Condition-Auswertung (Default: **aus**, siehe Warnung) | Nur deterministisch, wenn der Evaluator pur ist **und** Antwort-Events pro `workflow_id` geordnet ankommen |

**Wichtige Design-Regel (Single-Writer):** Der Timeout-Monitor darf **selbst kein** Routing-State schreiben. Er erzeugt lediglich Direktiven (z.B. Nachricht auf `agent.execution.results` mit `FAILED_RETRYABLE` oder auf `agent.control.loop`/`sweep`). Jede Zustandsänderung läuft über `DagAdvanceHandler` bzw. `SweepReaperHandler`. Das verhindert zwei konkurrierende Schreiber auf denselben Workflow.

**Entscheidung über `DecisionEvaluator`:** Als Default ist die Decision-Auswertung **synchron im Orchestrator** (im Fireable-Bestimmungs-Schritt der Transaktion). Das ist die einzige Variante, die G4 hart garantiert. Ein asynchroner Handler (`agent.control.condition`) wird nur angeboten, um rechen- oder wartungsintensive Auswertungen auszulasten — dann gelten zusätzliche Bedingungen: Auswertung muss pur sein (kein `now()`, `random()`, externe Calls), Antwort-Events werden mit `workflow_id` keyed und die Zustandsübernahme erfolgt nur durch `DagAdvanceHandler`.

### 15.7 Ablauf-Trace (Beispiel aus §5.3)

```
1. intake → DagAdvance: research_step PENDING → IN_PROGRESS → dispatch + timeout
2. result COMPLETED(research_step) → evaluateRouting:
     quality_gate feuert → dispatch (kein Agent, interne pur Berechnung)
3. quality_gate: condition ausgewertet, z.B. confidence=0.72 < 0.8 → decision_branch = "refinement_loop"
   Kante quality_gate → parallel_fork ist NICHT aktiv → parallel_fork bleibt PENDING
4. LoopIterationHandler: refinement_loop iteration=1 → dispatch refiner
5. result COMPLETED(refinement_loop#1) → exitCondition prüfen:
   false → LoopIterationHandler dispatch iteration=2
6. result COMPLETED(refinement_loop#2, confidence=0.83) → exitCondition true → LOOP#2 = COMPLETED
7. evaluateRouting: Kante refinement_loop → parallel_fork ist erfüllt (FORK = OR) → parallel_fork feuert
8. dispatch b.code.gen UND b.test.prep (Level identisch, stepId-Sortierung) → Worker parallel
9. result COMPLETED(b.test.prep) → DagAdvance: join-Readiness prüfen → b.code.gen fehlt → warten
10. result COMPLETED(b.code.gen) → JOIN merge_gate (AND) voll → dispatch final_review (HITL requires approval)
11. HitlResume: APPROVED → final_review IN_PROGRESS → dispatch
12. result COMPLETED(final_review) → END feuert → workflow_executions.status = COMPLETED
```

Wird der Orchestrator bei Schritt 6 gecrasht, bevor der LOOP-COMPLETED committen ist, wiederholt er bei Restart/Erzeugung des nächsten Sweep-Tick `evaluateRouting` auf dem persistierten Stand (LOOP#2=COMPLETED fehlt) ⇒ er würde den Schritt 6 identisch re-executieren (idempotent, G3). Ein Duplikat-Dispatch wird durch `uq_execution_step_iteration` verhindert.

### 15.8 Routing-Diagramme (konsistent mit §4.1, §4.3, §14, §15.6)

**Konventionen (gelten in allen Diagrammen):**
- Alle `workflow`-skopierten Topics (`agent.tasks.<type>`, `agent.execution.results`, `agent.control.loop`, `agent.control.hitl`, `agent.control.cancellation`) werden mit **`workflow_id`** keyed → ein Workflow landet immer auf derselben Partition (G1, Reihenfolge-Garantie).
- `agent.control.timeouts` keyed mit `task_id`, `agent.control.sweep` keyed mit `sweep_id` (keine Routing-Schreiber, vgl. Single-Writer-Regel §15.6).
- `[Handler]` = Name aus §15.6; Consumer-Gruppen in Klammern. `[R]` = Root-Starter (API-Gateway), `[W]` = Agent Worker.
- Payload-Beispiele folgen den Schemas aus §4.3. Deterministische Node-Auswertung (DECISION/FORK/JOIN) erfolgt **synchron in der Orchestrator-Transaktion** und erzeugt kein eigenes Kafka-Topic, außer sie wird explizit asynchron angehängt (§15.6, `agent.control.condition`).

#### Diagram 1 — DECISION (Condition) routing

```
      [R] HTTP POST /workflows/start (Event-INTAKE, key: wf-9876)
        |
        v
   agent.execution.intake
        |
        v
   [DagAdvanceHandler] (orchestrator-dag-group)  ->  Init research_step (PENDING->IN_PROGRESS)
        |
        v
   agent.tasks.researcher          key: wf-9876  (part. per wf-id)
        |
        v
   [W] researcher worker  ──>  agent.execution.results
                                  key: wf-9876
   {header:{workflowId:wf-9876, taskId:ta-1, agentType:researcher,
            status:COMPLETED}, output:{resultData:{confidence:0.72}}}
        |
        v
   [DagAdvanceHandler]
      research_step -> COMPLETED
      evaluateRouting() -> FIRE quality_gate (DECISION)
        |
        |  Condition synchron ausgewertet (pur):
        |  context['research_step'].confidence < 0.8  -> TRUE
        v
   decision_branch = "refinement_loop"   (persistiert auf der Checkpoint-Zeile)
        |
        v
   agent.tasks.refiner                key: wf-9876   (next node in chosen branch)
   {header:{workflowId:wf-9876, taskId:ta-2, agentType:refiner, attempt:1}}
        |
        v
   [W] refiner worker  ===>  LOOP-Phase, siehe Diagramm 2
   (alternative: condition FALSE -> decision_branch="parallel_fork" -> Fork-Phase, Diagramm 3)
```

#### Diagram 2 — LOOP routing

```
   [W] refiner worker beendet Iteration 1
        |
        v
   agent.execution.results           key: wf-9876
   {header:{...taskId:ta-2, agentType:refiner, status:COMPLETED},
    output:{resultData:{confidence:0.72}}}     // 0.72 < 0.8
        |
        v
   [DagAdvanceHandler]
      refinement_loop iteration=1  -> COMPLETED
      exitCondition: 0.72 >= 0.8  -> FALSE,  1 < maxIterations(3)
        |
        v
   agent.control.loop               key: wf-9876
   {workflowId:wf-9876, stepId:"refinement_loop", iteration:2}
        |
        v
   [LoopIterationHandler] (orchestrator-dag-group)
      checkpoint (wf-9876, refinement_loop, iter=2) PENDING  <- UQ guard,
      taskId=ta-3                                          (no doubles)
        |
        v
   agent.tasks.refiner              key: wf-9876
   {header:{...taskId:ta-3, attempt:1,...}}
        |
        v
   [W] refiner worker  (Iteration 2)  ->  zurück an den Start => Schleife bis:
        +-- exitCondition TRUE  -> refinement_loop COMPLETED -> evaluateRouting
        +-- maxIterations erreicht & exit FALSE -> WAITING_HITL (agent.control.hitl)
```

#### Diagram 3 — FORK / JOIN routing

```
   Decision/Fork-Kante erfüllt (quality_gate decision_branch="parallel_fork" ODER LOOP-Exit)
        |
        v
   [DagAdvanceHandler] -> FIRE parallel_fork (FORK) -> mark COMPLETED (kein Task),
                           evaluateRouting => FIRABLE b.code.gen UND b.test.prep
        |
        +---------------+---------------+
        v                               v
   agent.tasks.coder              agent.tasks.test_writer
   key: wf-9876                   key: wf-9876
   {header:{...taskId:ta-4,       {header:{...taskId:ta-5,
    agentType:coder...}}           agentType:test_writer...}}
        |                               |
        v                               v
   [W] coder worker               [W] test_writer worker  (parallel)
        |                               |
        +---------------+---------------+
                        |
                        v
   agent.execution.results  (2 COMPLETED-Nachrichten, Reihenfolge egal)
   key: wf-9876  {taskId:ta-4,...}   {taskId:ta-5,...}
                        |
        +---------------+---------------+
        v                               v
   [DagAdvanceHandler]  ←-- joinReadiness nach JEDER Ergebnis-Nachricht
     b.code.gen COMPLETED, b.test.prep COMPLETED
                                    |
                                    v
   JOIN merge_gate (AND) erfüllt  ->  dispatch final_review
        |
        v
   agent.tasks.reviewer  ->  [W] reviewer worker  ->  HITL ggf. agent.control.hitl
```

#### Diagram 4 — Alle Node-Typen im Gesamtfluss (STEP / DECISION / LOOP / FORK / JOIN / END)

```
 wf-9876   (eine Partition, alle Topics keyed workflow_id)

  [R] intake (agent.execution.intake)
      |
      v
  research_step  STEP -------- agent.tasks.researcher -------- [W] researcher
      |                                                            |
      |                                                            v
      |<------------------ agent.execution.results (COMPLETED) <---+
      v
  quality_gate DECISION   (sync, pur: decision_branch=refinement_loop)
      |
      +---------------------+
      |                     |
      v                     v
 refinement_loop LoopIterationHandler        parallel_fork FORK (default-Zweig,
  LOOP (iter 1..3)        (orchestrator-dag-group)  wenn decision_branch=nicht gewaehlt)
      |                     |                    |                     |
      |  agent.tasks.refiner (je iter)           +----------+----------+
      |  [W] refiner worker                      |          |
      |  agent.execution.results                v          v
      |  agent.control.loop                   b.code.gen  b.test.prep
      |  (iter+1 oder exit, s. Diagramm 2)     STEP         STEP
      |                                       agent.tasks.coder  agent.tasks.test_writer
      |                                       [W] coder    [W] test_writer
      |  exit TRUE -> LOOP COMPLETED (iter=2)           |      |
      |      +------------------------------------------+------+
      |      v                                          v
      +--> merge_gate  JOIN (AND, in_edges: code,tests) | beide COMPLETED
            |
            v
      final_review STEP (requiresHumanApproval) --- agent.tasks.reviewer --- [W] reviewer
            |                                                        |
            |   NEEDS_HITL                                            v
            |<--- agent.execution.results <--------------------------+
            |
            +-- agent.control.hitl --- [HitlResumeHandler] (APPROVED/RETRY)
            |                              v
            |                    final_review dispatch (REST/Kafka)
            v
      end  END  -> workflow_executions.status='COMPLETED'

  Nebenpfade (nicht im Hauptfluss):
      agent.control.timeouts  key: task_id  -> TimeoutMonitor (nur Direktiven)
      agent.control.sweep     key: sweep_id -> SweepReaperHandler (Recovery/Reaper)
      agent.control.cancellation key: wf-id -> CancellationHandler (Pruning)
```

**Lesehinweis zu Diagramm 4:** Die fünf Pfeile in Richtung `merge_gate` illustrieren, dass `refinement_loop` und `parallel_fork` zwei **alternative** Pfade sind (FORK hat OR-Eingangskanten). In der Ausführung wird je nach `decision_branch` entweder der Loop-Zweig (Diagramm 2) oder der Fork/Join-Zweig (Diagramm 3) durchlaufen — nie beide gleichzeitig im selben Zweig, dank Kanten-Aktivierung §15.3.

---

## 16. REST API Contract

Die API wird vom **API-Gateway / Orchestrator** (Modul `agent-orchestrator`) exponiert. Alle Antworten laufen prinzipiell asynchron über Kafka; das REST-API dient Synchron-Punkten (Start, Status, Definition-Management, HITL-Aktionen).

Basis: `/api/v1`, Auth via JWT/OIDC (Vault-verifiziert), `Content-Type: application/json`.
Fehlerformat (einheitlich):

```json
{ "error": { "code": "WORKFLOW_NOT_FOUND", "message": "...", "workflowId": "..." } }
```

| Method | Path | Body / Query | Response | Sender |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/workflows/start` | `{workflowName, version?, inputParams, idempotencyKey}` | `202 {workflowExecutionId}` | API-Gateway |
| `GET` | `/workflows/{executionId}` | — | `200 {status, definitionId, startedAt, completedAt, checkpoints[]}` | API-Gateway |
| `GET` | `/workflows/{executionId}/checkpoints` | `?stepId=&iteration=` | `200 {checkpoints[]}` | API-Gateway |
| `POST` | `/workflows/{executionId}/cancel` | — | `202` → `agent.control.cancellation` | API-Gateway |
| `GET` | `/workflow-definitions` | `?active=true` | `200 {definitions[]}` | Definition-Store |
| `POST` | `/workflow-definitions` | `{name, version, dag_structure}` | `201 {definitionId}` (validiert) | Definition-Store |
| `GET` | `/hitl/tickets` | `?status=OPEN` | `200 {tickets[]}` | HITL-Service |
| `POST` | `/hitl/tickets/{ticketId}/{action}` | `body {userId, note?}`; `action ∈ {retry, skip, abort}` | `202` → `agent.control.hitl` | HITL-Service |

**Start-Endpoint-Semantik (idempotent):**
- Client sendet `idempotencyKey` (UUID) → dedupliziert über `dedup:workflow:start:{key}` in Redis (TTL 24h).
- Bei Duplikat: `200` mit bereits existierendem `workflowExecutionId` (kein neuer Workflow).
- Wirkung: schreibt Execution `PENDING` + DAG-Checkpoints, publiziert Init auf `agent.execution.intake`.

**Definition-Endpoint-Semantik (`POST /workflow-definitions`):**
- `dag_structure` wird **vor dem INSERT validiert** (§17.1): Node-Typ-Vokabular, Azyklizität, DECISION→LOOP→defaultTarget-Regel, FORK/JOIN-Paarung, edge-activation-irreführende Kanten.
- Neue Versionen sind append-only (`UNIQUE (name, version)`); `is_active` bestimmt, welche Version neue Starts verwenden (G5-Pinning).

### 16.1 Event-Mapping (REST ↔ Kafka)

```
POST /workflows/start ─────────► agent.execution.intake      (key: workflow_id)
GET  /workflows/{id} ◄───────── Orchestator quert DB           (kein Kafka)
POST /workflows/{id}/cancel ───► agent.control.cancellation   (key: workflow_id)
POST /hitl/tickets/{id}/retry ─► agent.control.hitl           (key: workflow_id)
GET  /hitl/tickets ◄─────────── HitlTicket-Query (DB)
```

Synchron-Antworten erfolgen unmittelbar und bestätigen nur die Annahme (`202`). Der tatsächliche Fortschritt wird über `GET /workflows/{id}` (Polling) bezogen; ein optionaler Webhook-Notifier kann pro Status-Übergang posten.

---

## 17. Test- & Verifikationsstrategie (Determinismus-bezogen)

Angesichts der G1–G6-Garantien ist das Testkorsett integraler Bestandteil der Spezifikation.

### 17.1 Workflow-Validator (Unit + Komponente)

| Regel | Verifikation |
| :--- | :--- |
| Node-Typ ∈ {STEP, DECISION, LOOP, FORK, JOIN, END} | Schema-Validierung |
| Kein Zyklus zwischen Nicht-LOOP-Nodes | Topo-Sort (Kahn) |
| DECISION-Ziel nur ein LOOP; LOOP-Exit ⇒ `defaultTarget` der DECISION | Kanten-Validierung |
| FORK-Branch-Köpfe existieren; Jeder FORK hat genau ein paariges JOIN | Paarungs-Validierung |
| `step_id` Konvention `b.<branchId>.<name>` | Regex |

### 17.2 Deterministische Routing-Tests (Golden-File Test)

Zentrale Metapher: **jeder Zustandsübergang wird als Golden-File-Szenario abgebildet** — Input: Checkpoint-Zustand + DAG-Definition; Output: erwartete `fireableSteps`-Menge, `decision_branch`, gesetzte Status. Diese Tests sind **purity-frei** (kein Fake, kein Timing):
- Einer Zeile beschreibt `(checkpointState, dag)`, erwartete Ausgabe-Spalte wird bei Regression verglichen.
- Wiederholung desselben Szenarios (Replay nach Crash) MUSS identische Ergebnisse liefern → Abbildung von G3.

### 17.3 Chaos-/Recovery-Tests

- Kill des Orchestrators mid-DAG → nach Sweep-Tick wird er idempotent fortgesetzt (kein verlorenes/doppeltes Dispatch).
- Doppelte `COMPLETED`-Messages (At-Least-Once) → `uq_execution_step_iteration` blockiert zweiten Dispatch, Retry-Counter bleibt korrekt.
- Ein JOIN mit Fehler-Branch bleibt `PENDING`-frei und eskaliert via Sweep → `WAITING_HITL` (kein Deadlock).

### 17.4 System-/E2E-Tests (Determinismus über Kafka-Ebenen)

- Indem `workflow_id` auf dieselbe Partition fällt, müssen zwei aufeinanderfolgend ausgeführte Starts desselben DAGs exakt dieselbe Step-Reihenfolge / decision_trace zeigen (Determinismus-Hauptkriterium).
- Abweichendes Retry/Timeout-**Timing** wird durch Backoff-Parameter variiert; die Routing-Ergebnisabfolge MUSS identisch bleiben.

---

## 18. Deployment Readiness Checklist

| # | Item | Status | Notes |
| :--- | :--- | :--- | :--- |
| 1 | Kafka Cluster (Strimzi) with topic configs | TODO | Replication factor >= 3, min ISR = 2 |
| 2 | Confluent Schema Registry | TODO | JSON Schema, BACKWARD compatibility |
| 3 | PostgreSQL 16 with Flyway migrations | TODO | Apply all CREATE TABLE scripts |
| 4 | Redis 7.2 single-node | TODO | AOF persistence, `volatile-lru` eviction |
| 5 | HashiCorp Vault with DB secrets engine | TODO | Dynamic PostgreSQL credentials |
| 6 | Vault agent sidecar in Kubernetes | TODO | Injects secrets as env vars |
| 7 | OpenTelemetry Java Agent | TODO | `-javaagent:opentelemetry-javaagent.jar` |
| 8 | Grafana dashboards | TODO | 5 dashboards as defined in §12.3 |
| 9 | Kafka ACLs configured | TODO | Per-service read/write permissions |
| 10 | DLQ Replay Service deployed | TODO | Cron-based polling, manual review UI |
| 11 | HITL Slack/PagerDuty integration | TODO | Webhook URLs in Vault |
| 12 | Consumer group isolation | TODO | Separate groups per service |
| 13 | Circuit breaker thresholds tuned | TODO | Based on LLM provider SLAs |
| 14 | Workflow validator deployed | TODO | DAG cycle rejection, DECISION→LOOP→defaultTarget rule, FORK/JOIN pairing, edge-activation checks |
| 15 | `agent.control.loop` + `agent.control.sweep` topics | TODO | Created with per §4.1 config; sweep producer (CronJob) scheduled |
| 16 | Orchestrator DAG single-writer | TODO | K8s-Lease/spring-integration JDBC lock for active orchestrator |
| 17 | SweepReaper recovery test | TODO | Kill orchestrator mid-DAG, verify idempotent re-execution |
| 18 | REST API (start/status/definitions/hitl) | TODO | Per §16, Vault JWT/OIDC auth |
| 19 | Workflow-Validator (golden-file) | TODO | Per §17.1/17.2, deployed pre-POST workflow-definitions |
| 20 | ErrorClassifier deployed | TODO | Per §9.5 taxonomy in worker |
| 21 | HITL expiry (SweepReaper) | TODO | TTL + escalation policy per §10.5 |
| 22 | Determinism E2E suite | TODO | Golden-file + chaos per §17.3/17.4 |

---

## Appendix A: Environment Configuration

```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
    properties:
      schema.registry.url: ${SCHEMA_REGISTRY_URL}
      security.protocol: SASL_SSL
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: ${KAFKA_SASL_JAAS}
    consumer:
      enable-auto-commit: false
      isolation-level: read_committed
    producer:
      acks: all
      retries: 3
      delivery.timeout.ms: 30000

  datasource:
    url: jdbc:postgresql://${DB_HOST}:5432/${DB_NAME}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}

  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: 6379

app:
  vault:
    uri: ${VAULT_URI}
    token: ${VAULT_TOKEN}
    secret-path: secret/data/platform
  kafka:
    agent-topics: agent.tasks.researcher,agent.tasks.coder,agent.tasks.reviewer
  timeout:
    check-interval-ms: 5000
    tolerance-ms: 1000
  dlq:
    replay-interval-ms: 30000
    max-retries: 5
    backoff-base-seconds: 10

resilience4j:
  circuitbreaker:
    instances:
      llm-provider:
        slidingWindowSize: 10
        minimumNumberOfCalls: 5
        failureRateThreshold: 50
        waitDurationInOpenState: 30s
        permittedNumberOfCallsInHalfOpenState: 3
```

## Appendix B: Glossary

| Term | Definition |
| :--- | :--- |
| **DAG** | Directed Acyclic Graph — the workflow structure defining task dependencies |
| **Checkpoint** | An execution record for a single DAG node/step |
| **HITL** | Human-in-the-Loop — workflow pause requiring manual intervention |
| **DLQ** | Dead Letter Queue — isolation queue for unrecoverable message processing failures |
| **CDC** | Change Data Capture — database event streaming via Debezium |
| **Circuit Breaker** | Resilience pattern that stops calls to a failing service temporarily |
