# System Specification: Event-Driven AI Agent Platform

> **Version**: 2.0  
> **Status**: Draft  
> **Last Updated**: 2026-09-16  
> **Decisions Incorporated**: Orchestrator-Central DAG Engine, Kafka Delayed Messages, Confluent Schema Registry, Hybrid DLQ Recovery, HashiCorp Vault, Redis single-node, Deterministic Routing (Loops, Conditions, Fork/Join), STEP-Guards, Branch-Failure Semantics, REST API, HITL Expiry

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

### 9.5 Error Taxonomy & Routing Decision (Worker Side)

Every error in the worker is classified unambiguously before it is forwarded to `agent.execution.results`, `agent.execution.dlq`, or `agent.control.hitl`:

| Error Class | Examples | Kafka Routing | Retry in Worker |
| :--- | :--- | :--- | :--- |
| `TRANSIENT` | LLM timeout, 429/rate limit, 5xx provider, `IOException`, DB connection peak | Retry with backoff (max. `maxRetryAttempts` from AgentConfig) | yes |
| `FATAL` | Provider 4xx (InvalidRequest/Auth), invalid input format, guardrail violation | **directly** `agent.execution.dlq` (no retry makes sense) | no |
| `BUSINESS` | Logic error in the task (wrong answer, validation) | `FAILED_RETRYABLE` with max. N attempts, then `NEEDS_HITL` | yes (bounded) |

**Classification rule (implemented in `ErrorClassifier`):**
1. `FATAL` always wins (4xx provider errors, schema/payload errors).
2. `BUSINESS` if the error lies in the task logic/validation.
3. otherwise `TRANSIENT`.

**Status mapping to result events:**

| Worker Decision | `status` in `agent.execution.results` | Orchestrator Routing |
| :--- | :--- | :--- |
| Success | `COMPLETED` | `evaluateRouting` (next steps) |
| Retry possible | `FAILED_RETRYABLE` | Retry counter, backoff, re-dispatch |
| Retry not worthwhile | `FAILED_FATAL` | DLQ + HITL escalation |
| Validation / HITL | `NEEDS_HITL` | `agent.control.hitl` + ticket |

**Single-writer warning (RQ §15.6):** Error classification happens in the worker (producer side), while the **state change** (checkpoint/retry/HITL) happens exclusively in the `DagAdvanceHandler` or `SweepReaperHandler` — the worker itself never writes routing state.

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

### 10.5 HITL Ticket Lifecycle & Expiry

Every ticket goes through an explicit, deterministic state machine:

```
       created: OPEN ──────────► APPROVED (→ Re-dispatch) 
            │                       │
            ├── retry ──► APPROVED  ├── skip   ──► SKIPPED (→ next step)
            ├── skip  ──► SKIPPED   ├── abort  ──► ABORTED (→ workflow CANCELLED)
            └── abort ─► ABORTED    └── timeout ─► EXPIRED (→ re-escalation)
                        ▲                              │
                        └──────── ESCALATION LEVEL ────┘
```

| Transition | Trigger | Effect |
| :--- | :--- | :--- |
| `OPEN → APPROVED` | Operator clicks `Retry Step` | Checkpoint back to `IN_PROGRESS`, task is re-dispatched |
| `OPEN → SKIPPED` | Operator clicks `Skip & Continue` | Checkpoint `SKIPPED`, `evaluateRouting` continues |
| `OPEN → ABORTED` | Operator clicks `Abort Workflow` | All open checkpoints `CANCELLED`, execution `CANCELLED` |
| `OPEN → EXPIRED` | `ttl` reached (default 24h, configurable) | New escalation with higher urgency (PagerDuty high) |

**Expiry mechanism (deterministic):** On every tick, the `SweepReaperHandler` compares `now() - created_at > ttl` for `OPEN` tickets (wall-clock allowed only here, §15.6). After `maxEscalationsPerTicket` (default 3) on `EXPIRED`, the ticket becomes `RESOLVED` (human final decision) and the workflow remains in `PAUSED_HITL` until an admin intervenes manually.

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

### 15.1 Goal & Determinism Guarantees

All routing decisions (which steps fire next, which branches are chosen, which loops continue) are computed via a **single pure function**:

```
fireableSteps = evaluateRouting(persistedCheckpointState, dagDefinitionVersion)
```

The function is free of side effects: it reads exclusively the persisted state snapshot (`workflow_checkpoints`) and the DAG definition pinned at start. Identical input states **always** yield identical sets of steps to dispatch.

| # | Guarantee | Mechanism |
| :--- | :--- | :--- |
| G1 | **Single writer per workflow** | All routing topics are keyed with `workflow_id` → always the same partition per workflow. Exactly one active orchestrator owns the partition (K8s lease / Spring JDBC lock). |
| G2 | **Atomic transitions** | State transition + determination of the next steps + dispatch send in **one** DB transaction. The Kafka offset is committed only after `COMMIT`. |
| G3 | **Idempotent re-execute** | The evaluator over the same persistent state yields the same fireable steps. Duplicate/re-delivered events are no-ops thanks to `uq_execution_step_iteration` (unique constraint of the dispatch ops). |
| G4 | **No non-determinism sources in the routing path** | No wall clock, no `random()`, no external HTTP in condition/loop/join evaluation. Time appears only in timeout and sweep events (there as a threshold, not as routing input). |
| G5 | **Pinned DAG version** | Routing always uses the `workflow_definitions` version with which the execution was started. Running definitions do not change under an execution. |
| G6 | **Termination** | Loops are bounded by `maxIterations`. Non-LOOP cycles are rejected at upload by the workflow validator. |

### 15.2 State Model of the Nodes

Every step in the DAG has exactly **one** checkpoint row per iteration in `workflow_checkpoints` (extended by `iteration`, `node_type`, `decision_branch`).

| Status | Meaning | Can be set by evaluator |
| :--- | :--- | :--- |
| `PENDING` | Not yet dispatched | no — remains until `evaluateRouting` fires |
| `IN_PROGRESS` | Task has been dispatched to a worker | yes (at dispatch) |
| `COMPLETED` | Step successful | yes |
| `FAILED` | No longer retryable (fatal) | yes |
| `WAITING_HITL` | Human decision required | yes |
| `SKIPPED` | Deselected via DECISION pruning | yes (deterministically cascading) |
| `PRUNED` | Fork branch discarded (XOR) | yes |
| `CANCELLED` | Workflow aborted / join no longer reachable | yes |

Ordering rule for state transitions: `PENDING → IN_PROGRESS → {COMPLETED, FAILED, WAITING_HITL}`, thereafter optionally `→ SKIPPED/PRUNED/CANCELLED` only by the evaluator. No handler except `evaluateRouting` may write routing status.

### 15.3 The Deterministic Routing Evaluator (Core)

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

**STEP guards (conditional execution of a step):** A `STEP` may optionally have a `guardCondition` (`{condition}`). The evaluation takes place **purely in `evaluateRouting`**, directly before firing, against the persisted context (`input_params`, `context:workflow:{id}`, completed outputs):

```
STEP:  all(edgeCompleted(d → s) ...)
       UND guardCheck(s):
         wenn s.guardCondition definiert:
             guardErfüllt = eval(s.guardCondition, persistedContext)
             wenn guardErfüllt == false  →  mark(s, SKIPPED)   // Skip-durch, kein Task
```

A `SKIPPED` guard step counts downstream like a `PRUNED` step as "effectively satisfied" (§15.5 AND-JOIN): it is skipped without blocking the flow. The **decision is persisted** (`status=SKIPPED`, reason in `decision_branch` = `guard:false`) → replay runs identical.

**Guards are written deterministically:** The same purity rules apply as for DECISION (G4): only persisted context, no `now()`, no `random()`, no external calls.

**Key points for determinism:**
- **Order is fixed**: sorting by `(level, stepId)` — no parallelism in the evaluator itself, no thread-timing dependency.
- **Edge activation at DECISION**: A `DECISION` persists `decision_branch` (the chosen target step). Only the edge to this target is "active" — all other outgoing edges never satisfy `edgeCompleted`. Loop-back (DECISION → LOOP) can thereby be modeled without syntax, without the non-chosen path firing (cf. trace §15.7).
- **Pruning is cascading and deterministic**: If a DECISION chooses branch X, the non-chosen branch heads (`PRUNED`) and their successors are recursively marked `SKIPPED` — same sorting, same order.
- **Skips count for JOIN as "effectively satisfied"**: A skip pruned via pruning counts as a completed equivalent ("no real work left"), but does **not** count as successful work for AND.
- **Decisions are persistent**: `decision_branch` and `iteration` on the checkpoint row → replay after a crash yields identical results.

### 15.4 Loops (bounded)

- The `LOOP` node in the DAG definition is **stateless**: per iteration there is exactly one checkpoint row `(execution_id, step_id, iteration=i)`, `node_type='LOOP'`, `agent_type=bodyAgentType`.
- After completion of iteration i (COMPLETED), the **LoopIterationHandler** writes the state, then:
  1. `exitCondition` is evaluated against `context['LOOP-stepId'].resultData` — **purely**, only on already persisted output data.
  2. `true` → checkpoint LOOP with `iteration=i` → `COMPLETED`; `evaluateRouting` then continues normally (the LOOP's successor fires).
  3. `false` and `i < maxIterations` → next dispatch as iteration `i+1` (new row, new taskId).
  4. `false` and `i == maxIterations` → `WAITING_HITL` (human decision: abort or manually continue the work).
- **Termination proof**: The `iteration` counter exists only in the DB (never in RAM); the evaluator can increment by at most 1 up to `maxIterations` → G6.
- **Dispatch event**: Loop advance runs via `agent.control.loop` (partition key `workflow_id`); the payload contains `stepId` and `iteration`. The handler ignores iterations whose checkpoint already has a status other than `PENDING/IN_PROGRESS` (idempotency).

### 15.5 Fork / Join

- **FORK**: When firing, each branch head (`firstStep`) is left as `PENDING` and dispatched in the next `evaluateRouting` round — the partition order serializes the dispatches, but the workers execute in parallel.
- **AND-JOIN**: Fires only when all non-`PRUNED` dependencies are `COMPLETED` (or, via pruning, `SKIPPED`). A join whose dependencies were all pruned/cancelled itself becomes `SKIPPED` (never gets stuck).
- **XOR-JOIN (race)**: The state is satisfied when **any** branch is completed; the remaining branches are deterministically `PRUNED`.
- **N_OF-JOIN**: Fires when `joinN` branches are completed; the remaining in-flight branches are likewise `PRUNED` after completion.
- **Branch failure semantics** (if an incoming branch does not end COMPLETED/SKIPPED/PRUNED):
  - **`FAILED` (fatal)**: The branch counts as **blocking**. AND/N_OF/JOIN is NOT satisfied → `evaluateRouting` never fires; the SweepReaperHandler (`agent.control.sweep`) detects the hanging JOIN after the join deadline has elapsed and sets it to `WAITING_HITL`. The workflow waits for a human decision (retry the branch / skip join / abort).
  - **`WAITING_HITL`**: Also blocks; only after resolution by `HitlResumeHandler` is `evaluateRouting` re-evaluated.
  - **Alternative view**: `joinOnAny` (`XOR`) is the only pattern that "forwards" branch failures (the first successful branch wins, failing branches become `PRUNED`).
- No broadcast, no join-probe topic needed: join readiness is **always** recomputed when processing a result event in the DagAdvanceHandler — the same handler, the same order ⇒ deterministic.

### 15.6 Required Non-Agent Kafka Message Handlers

In addition to those already defined (timeout monitor, config update, DLQ replay), the following handlers are required for resilient deterministic routing logic:

| Handler | Consumer Group | Topics (in) | Task | Determinism Role |
| :--- | :--- | :--- | :--- | :--- |
| **DagAdvanceHandler** (result finalizer) | `orchestrator-dag-group` | `agent.execution.results` | **Only** write path for routing: transition + `evaluateRouting` + dispatch, all in one DB transaction (G2, G3) | Core — G1–G6 |
| **LoopIterationHandler** | `orchestrator-dag-group` | `agent.control.loop` | Counts iterations, checks `exitCondition`, dispatches onward or closes the LOOP | G6 (termination) + idempotency |
| **JoinEvaluator** (part of DagAdvance, no separate topic) | — | `agent.execution.results` | Computes join readiness deterministically on every branch result | Deterministic join semantics (AND/XOR/N_OF) |
| **SweepReaperHandler** | `orchestrator-reaper-group` | `agent.control.sweep` | Periodic ticks: find stale `IN_PROGRESS` (crash recovery), stuck JOIN/LOOP, decide retry/HITL — **only here** may wall-clock enter the logic | G4 limitation of time dependency |
| **CancellationHandler** | `orchestrator-dag-group` | `agent.control.cancellation` | Aborts the workflow deterministically: marks all `PENDING` steps `CANCELLED` in level/ID order, prunes forks, closes JOIN | Deterministic pruning |
| **HitlResumeHandler** | `alerting-hitl-group` | `agent.control.hitl` + REST callback from Slack/PD | Turns retry/skip/abort decisions into routing steps and calls `evaluateRouting` again | Deterministic decision chain after human input |
| **DecisionEvaluator (optional, async)** | `orchestrator-condition-group` | `agent.control.condition` | Alternative asynchronous condition evaluation (default: **off**, see warning) | Deterministic only if the evaluator is pure **and** response events for `workflow_id` arrive in order |

**Important design rule (single writer):** The timeout monitor must **itself not** write routing state. It merely creates directives (e.g., a message on `agent.execution.results` with `FAILED_RETRYABLE` or on `agent.control.loop`/`sweep`). Every state change runs via `DagAdvanceHandler` or `SweepReaperHandler`. This prevents two competing writers on the same workflow.

**Decision on `DecisionEvaluator`:** By default, decision evaluation is **synchronous in the orchestrator** (in the fireable-determination step of the transaction). This is the only variant that hard-guarantees G4. An asynchronous handler (`agent.control.condition`) is offered only to offload compute- or maintenance-intensive evaluations — in that case additional conditions apply: evaluation must be pure (no `now()`, `random()`, external calls), response events are keyed with `workflow_id`, and state adoption occurs only via `DagAdvanceHandler`.

### 15.7 Flow Trace (Example from §5.3)

```
1. intake → DagAdvance: research_step PENDING → IN_PROGRESS → dispatch + timeout
2. result COMPLETED(research_step) → evaluateRouting:
     quality_gate fires → dispatch (no agent, internal pure computation)
3. quality_gate: condition evaluated, e.g. confidence=0.72 < 0.8 → decision_branch = "refinement_loop"
   edge quality_gate → parallel_fork is NOT active → parallel_fork remains PENDING
4. LoopIterationHandler: refinement_loop iteration=1 → dispatch refiner
5. result COMPLETED(refinement_loop#1) → check exitCondition:
   false → LoopIterationHandler dispatch iteration=2
6. result COMPLETED(refinement_loop#2, confidence=0.83) → exitCondition true → LOOP#2 = COMPLETED
7. evaluateRouting: edge refinement_loop → parallel_fork is satisfied (FORK = OR) → parallel_fork fires
8. dispatch b.code.gen AND b.test.prep (same level, stepId sorting) → workers parallel
9. result COMPLETED(b.test.prep) → DagAdvance: check join readiness → b.code.gen missing → wait
10. result COMPLETED(b.code.gen) → JOIN merge_gate (AND) full → dispatch final_review (HITL requires approval)
11. HitlResume: APPROVED → final_review IN_PROGRESS → dispatch
12. result COMPLETED(final_review) → END fires → workflow_executions.status = COMPLETED
```

If the orchestrator crashes at step 6 before the LOOP-COMPLETED has been committed, on restart/creation of the next sweep tick it repeats `evaluateRouting` on the persisted state (LOOP#2=COMPLETED missing) ⇒ it would re-execute step 6 identically (idempotent, G3). A duplicate dispatch is prevented by `uq_execution_step_iteration`.

### 15.8 Routing Diagrams (consistent with §4.1, §4.3, §14, §15.6)

**Conventions (apply to all diagrams):**
- All `workflow`-scoped topics (`agent.tasks.<type>`, `agent.execution.results`, `agent.control.loop`, `agent.control.hitl`, `agent.control.cancellation`) are keyed with **`workflow_id`** → a workflow always lands on the same partition (G1, ordering guarantee).
- `agent.control.timeouts` keyed with `task_id`, `agent.control.sweep` keyed with `sweep_id` (no routing writers, cf. single-writer rule §15.6).
- `[Handler]` = name from §15.6; consumer groups in parentheses. `[R]` = root starter (API gateway), `[W]` = agent worker.
- Payload examples follow the schemas from §4.3. Deterministic node evaluation (DECISION/FORK/JOIN) takes place **synchronously in the orchestrator transaction** and produces no separate Kafka topic, unless it is explicitly attached asynchronously (§15.6, `agent.control.condition`).

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
        |  Condition evaluated synchronously (pure):
        |  context['research_step'].confidence < 0.8  -> TRUE
        v
   decision_branch = "refinement_loop"   (persisted on the checkpoint row)
        |
        v
   agent.tasks.refiner                key: wf-9876   (next node in chosen branch)
   {header:{workflowId:wf-9876, taskId:ta-2, agentType:refiner, attempt:1}}
        |
        v
   [W] refiner worker  ===>  LOOP-Phase, siehe Diagramm 2
   (alternative: condition FALSE -> decision_branch="parallel_fork" -> fork phase, Diagram 3)
```

#### Diagram 2 — LOOP routing

```
   [W] refiner worker finishes iteration 1
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
   [W] refiner worker  (iteration 2)  ->  back to the start => loop until:
        +-- exitCondition TRUE  -> refinement_loop COMPLETED -> evaluateRouting
        +-- maxIterations reached & exit FALSE -> WAITING_HITL (agent.control.hitl)
```

#### Diagram 3 — FORK / JOIN routing

```
   Decision/fork edge satisfied (quality_gate decision_branch="parallel_fork" OR LOOP exit)
        |
        v
   [DagAdvanceHandler] -> FIRE parallel_fork (FORK) -> mark COMPLETED (no task),
                           evaluateRouting => FIRABLE b.code.gen AND b.test.prep
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
   agent.execution.results  (2 COMPLETED messages, order irrelevant)
   key: wf-9876  {taskId:ta-4,...}   {taskId:ta-5,...}
                        |
        +---------------+---------------+
        v                               v
   [DagAdvanceHandler]  ←-- joinReadiness after EVERY result message
     b.code.gen COMPLETED, b.test.prep COMPLETED
                                    |
                                    v
   JOIN merge_gate (AND) satisfied  ->  dispatch final_review
        |
        v
   agent.tasks.reviewer  ->  [W] reviewer worker  ->  HITL if applicable agent.control.hitl
```

#### Diagram 4 — All Node Types in the Overall Flow (STEP / DECISION / LOOP / FORK / JOIN / END)

```
 wf-9876   (one partition, all topics keyed workflow_id)

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
 refinement_loop LoopIterationHandler        parallel_fork FORK (default branch,
  LOOP (iter 1..3)        (orchestrator-dag-group)  if decision_branch=not chosen)
      |                     |                    |                     |
      |  agent.tasks.refiner (per iter)           +----------+----------+
      |  [W] refiner worker                      |          |
      |  agent.execution.results                v          v
      |  agent.control.loop                   b.code.gen  b.test.prep
      |  (iter+1 or exit, see Diagram 2)     STEP         STEP
      |                                       agent.tasks.coder  agent.tasks.test_writer
      |                                       [W] coder    [W] test_writer
      |  exit TRUE -> LOOP COMPLETED (iter=2)           |      |
      |      +------------------------------------------+------+
      |      v                                          v
      +--> merge_gate  JOIN (AND, in_edges: code,tests) | both COMPLETED
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

  Side paths (not in the main flow):
      agent.control.timeouts  key: task_id  -> TimeoutMonitor (directives only)
      agent.control.sweep     key: sweep_id -> SweepReaperHandler (Recovery/Reaper)
      agent.control.cancellation key: wf-id -> CancellationHandler (Pruning)
```

**Reading note on Diagram 4:** The five arrows toward `merge_gate` illustrate that `refinement_loop` and `parallel_fork` are two **alternative** paths (FORK has OR incoming edges). At execution time, depending on `decision_branch`, either the loop branch (Diagram 2) or the fork/join branch (Diagram 3) is traversed — never both simultaneously in the same branch, thanks to edge activation §15.3.

---

## 16. REST API Contract

The API is exposed by the **API gateway / orchestrator** (module `agent-orchestrator`). All responses are in principle asynchronous via Kafka; the REST API serves synchronous points (start, status, definition management, HITL actions).

Base: `/api/v1`, auth via JWT/OIDC (Vault-verified), `Content-Type: application/json`.
Error format (uniform):

```json
{ "error": { "code": "WORKFLOW_NOT_FOUND", "message": "...", "workflowId": "..." } }
```

| Method | Path | Body / Query | Response | Sender |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/workflows/start` | `{workflowName, version?, inputParams, idempotencyKey}` | `202 {workflowExecutionId}` | API gateway |
| `GET` | `/workflows/{executionId}` | — | `200 {status, definitionId, startedAt, completedAt, checkpoints[]}` | API gateway |
| `GET` | `/workflows/{executionId}/checkpoints` | `?stepId=&iteration=` | `200 {checkpoints[]}` | API gateway |
| `POST` | `/workflows/{executionId}/cancel` | — | `202` → `agent.control.cancellation` | API gateway |
| `GET` | `/workflow-definitions` | `?active=true` | `200 {definitions[]}` | Definition store |
| `POST` | `/workflow-definitions` | `{name, version, dag_structure}` | `201 {definitionId}` (validated) | Definition store |
| `GET` | `/hitl/tickets` | `?status=OPEN` | `200 {tickets[]}` | HITL service |
| `POST` | `/hitl/tickets/{ticketId}/{action}` | `body {userId, note?}`; `action ∈ {retry, skip, abort}` | `202` → `agent.control.hitl` | HITL service |

**Start endpoint semantics (idempotent):**
- Client sends `idempotencyKey` (UUID) → deduplicated via `dedup:workflow:start:{key}` in Redis (TTL 24h).
- On duplicate: `200` with the already existing `workflowExecutionId` (no new workflow).
- Effect: writes execution `PENDING` + DAG checkpoints, publishes init to `agent.execution.intake`.

**Definition endpoint semantics (`POST /workflow-definitions`):**
- `dag_structure` is **validated before the INSERT** (§17.1): node type vocabulary, acyclicity, DECISION→LOOP→defaultTarget rule, FORK/JOIN pairing, edge activation (misleading edges).
- New versions are append-only (`UNIQUE (name, version)`); `is_active` determines which version new starts use (G5 pinning).

### 16.1 Event Mapping (REST ↔ Kafka)

```
POST /workflows/start ─────────► agent.execution.intake      (key: workflow_id)
GET  /workflows/{id} ◄───────── Orchestrator queries DB           (no Kafka)
POST /workflows/{id}/cancel ───► agent.control.cancellation   (key: workflow_id)
POST /hitl/tickets/{id}/retry ─► agent.control.hitl           (key: workflow_id)
GET  /hitl/tickets ◄─────────── HitlTicket query (DB)
```

Synchronous responses occur immediately and merely confirm acceptance (`202`). Actual progress is obtained via `GET /workflows/{id}` (polling); an optional webhook notifier can post on each status transition.

---

## 17. Test & Verification Strategy (Determinism-Related)

Given the G1–G6 guarantees, the test harness is an integral part of the specification.

### 17.1 Workflow Validator (Unit + Component)

| Rule | Verification |
| :--- | :--- |
| Node type ∈ {STEP, DECISION, LOOP, FORK, JOIN, END} | Schema validation |
| No cycle between non-LOOP nodes | Topo sort (Kahn) |
| DECISION target only one LOOP; LOOP exit ⇒ `defaultTarget` of the DECISION | Edge validation |
| FORK branch heads exist; every FORK has exactly one paired JOIN | Pairing validation |
| `step_id` convention `b.<branchId>.<name>` | Regex |

### 17.2 Deterministic Routing Tests (Golden-File Test)

Central metaphor: **every state transition is mapped as a golden-file scenario** — input: checkpoint state + DAG definition; output: expected `fireableSteps` set, `decision_branch`, statuses set. These tests are **impurity-free** (no fake, no timing):
- One line describes `(checkpointState, dag)`; the expected output column is compared on regression.
- Repetition of the same scenario (replay after crash) MUST yield identical results → mapping of G3.

### 17.3 Chaos/Recovery Tests

- Kill the orchestrator mid-DAG → after a sweep tick it resumes idempotently (no lost/duplicate dispatch).
- Duplicate `COMPLETED` messages (at-least-once) → `uq_execution_step_iteration` blocks the second dispatch, the retry counter stays correct.
- A JOIN with a failing branch does not remain `PENDING` and escalates via sweep → `WAITING_HITL` (no deadlock).

### 17.4 System/E2E Tests (Determinism Across Kafka Layers)

- Because `workflow_id` falls on the same partition, two consecutively executed starts of the same DAG must show exactly the same step order / decision trace (main determinism criterion).
- Deviating retry/timeout **timing** is varied via backoff parameters; the routing result sequence MUST remain identical.

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
