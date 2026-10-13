## 1. Executive Summary & Core Architecture

The **Event-Driven AI Agent Platform** is an enterprise-grade, highly available, and asynchronous multi-agent orchestrator. The platform uses **Apache Kafka** as its central event bus for command and result propagation, **Java 21 (LTS)** with **Spring Boot 3.3 / Spring Kafka** for execution and orchestration runtimes, **PostgreSQL 16** for durable workflow states, DAG execution checkpoints, and database-driven dynamic agent configuration, and **Redis 7.2** for ephemeral runtime state, task deduplication, and locking.

### 1.1 Architectural Principles
1. **Fully Asynchronous Execution**: Communication between agents, orchestrators, and system services is non-blocking and strictly decoupled via Kafka topics.
2. **Database-Driven Dynamic Agent Configuration**: Agent parameters, prompts, tools, model configurations, and runtime limits are loaded dynamically from PostgreSQL. Changes are broadcasted via Change Data Capture (CDC / Debezium) or config-update events without requiring service redeployment or pod restarts.
3. **Resilience & Fault Tolerance**: Mandatory idempotency keys, exponential backoffs, Dead Letter Queues (DLQ), automated circuit breaking for LLM providers, and atomic checkpointing ensure reliable execution.
4. **End-to-End Observability**: Full distributed tracing using OpenTelemetry, W3C Trace Context propagation across Kafka headers, Prometheus metrics, and Grafana dashboards.
5. **Human-in-the-Loop (HITL) Alerting**: Intelligent fallback and alerting mechanisms via Webhooks, Slack, PagerDuty, and Email when workflows hit manual review thresholds, continuous LLM failures, or unexpected DAG blockages.

---

## 2. Technology Stack & Component Overview


```

+---------------------------------------------------------------------------------------------------+
|                                        API Gateway / Frontend                                     |
+---------------------------------------------------------------------------------------------------+
|
v
+---------------------------------------------------------------------------------------------------+
|                                 Kafka Event Bus (Cluster)                                         |
|  - agent.execution.intake        - agent.tasks.<agent_type>      - agent.execution.results        |
|  - agent.control.timeouts        - agent.control.cancellation    - agent.control.hitl             |
|  - agent.execution.dlq           - telemetry.agent.metrics       - agent.config.updates           |
+---------------------------------------------------------------------------------------------------+
^                                    ^                                    ^
|                                    |                                    |
+---------------+                  +---------------+                    +---------------+
| Orchestrator  |                  | Agent Workers |                    | Timeout & HITL|
| Engine (Java) |                  | (Generic Java)|                    | Service (Java)|
+---------------+                  +---------------+                    +---------------+
|                                    |                                    |
+-----------------+------------------+------------------------------------+
|
v
+--------------------------------------------------+          +----------------------------------+
|               PostgreSQL 16                      |          |            Redis 7.2             |
| - Workflow DAGs & States (Checkpoints)           |          | - Ephemeral Context & Tokens     |
| - Agent Definitions & Versioning                 |          | - Distributed Task Locks         |
| - HITL Escalation Logs                           |          | - Task Deduplication Keys        |
+--------------------------------------------------+          +----------------------------------+

```

| Component | Technology | Version | Purpose |
| :--- | :--- | :--- | :--- |
| **Language Runtime** | Java OpenJDK | 21 (LTS) | Core platform runtime leveraging Virtual Threads (Project Loom) |
| **Build System** | Apache Maven | 3.9+ | Dependency management, compilation, container image generation |
| **Framework** | Spring Boot / Spring Kafka | 3.3.x | Core IOC container, Kafka integration, JPA, REST interfaces |
| **Event Broker** | Apache Kafka / Strimzi | 3.7+ | Core event bus, message queuing, topic streaming |
| **Primary Database** | PostgreSQL | 16.x | Persistent state, dynamic configuration, workflow checkpoints |
| **Cache / In-Memory State** | Redis | 7.2.x | Ephemeral context buffering, rate-limiting, task deduplication |
| **CDC Engine** | Debezium / PgOutput | 2.5+ | Database Change Data Capture for dynamic config updates |
| **Metrics & Tracing** | OpenTelemetry / Micrometer | 1.30+ | Distributed tracing, Prometheus metrics exporter |
| **Human Alerting** | Slack Webhooks / PagerDuty API | Native REST | Escalation notifications for failed or stalled workflows |

---

## 3. Kafka Topic Topology & Schemas

### 3.1 Topic Definitions

| Topic Name | Partitions | Cleanup Policy | Key Strategy | Retention | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `agent.execution.intake` | 12 | `delete` | `workflow_id` | 7 Days | Trigger point for incoming workflow requests |
| `agent.tasks.<agent_type>` | 12 | `delete` | `workflow_id` | 7 Days | Agent-specific execution task queues |
| `agent.execution.results` | 12 | `delete` | `workflow_id` | 7 Days | Execution responses and status updates from workers |
| `agent.control.timeouts` | 6 | `delete` | `task_id` | 3 Days | Scheduled delay events for monitoring hanging tasks |
| `agent.control.cancellation` | 6 | `delete` | `workflow_id` | 1 Day | Signal to cancel active executions across workers |
| `agent.control.hitl` | 6 | `delete` | `workflow_id` | 14 Days | Queue for tasks requiring human review or escalation |
| `agent.execution.dlq` | 6 | `delete` | `workflow_id` | 30 Days | Isolation queue for unrecoverable errors |
| `agent.config.updates` | 1 | `compact` | `agent_type` | Compacted | Broadcast topic for dynamic DB config reloading |

---

### 3.2 Key Payload Schemas

#### Task Assignment Event (`agent.tasks.<agent_type>`)
```json
{
  "$schema": "[https://json-schema.org/draft/2020-12/schema](https://json-schema.org/draft/2020-12/schema)",
  "title": "AgentTaskEvent",
  "type": "object",
  "properties": {
    "header": {
      "type": "object",
      "properties": {
        "workflowId": { "type": "string", "format": "uuid" },
        "taskId": { "type": "string", "format": "uuid" },
        "parentTaskId": { "type": "string", "format": "uuid", "nullable": true },
        "traceParent": { "type": "string" },
        "attempt": { "type": "integer", "minimum": 1 },
        "maxAttempts": { "type": "integer", "minimum": 1 },
        "createdAt": { "type": "string", "format": "date-time" },
        "timeoutMs": { "type": "integer", "default": 60000 }
      },
      "required": ["workflowId", "taskId", "traceParent", "attempt", "createdAt"]
    },
    "agentConfig": {
      "type": "object",
      "properties": {
        "agentType": { "type": "string" },
        "version": { "type": "string" },
        "model": { "type": "string" },
        "temperature": { "type": "number" }
      },
      "required": ["agentType", "version"]
    },
    "payload": {
      "type": "object",
      "properties": {
        "inputPrompt": { "type": "string" },
        "contextReferences": {
          "type": "array",
          "items": { "type": "string" }
        },
        "parameters": { "type": "object" }
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
  "$schema": "[https://json-schema.org/draft/2020-12/schema](https://json-schema.org/draft/2020-12/schema)",
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
        "executionDurationMs": { "type": "integer" }
      },
      "required": ["workflowId", "taskId", "agentType", "status", "completedAt"]
    },
    "output": {
      "type": "object",
      "properties": {
        "resultData": { "type": "object" },
        "errorMessage": { "type": "string", "nullable": true },
        "stackTrace": { "type": "string", "nullable": true }
      }
    },
    "usage": {
      "type": "object",
      "properties": {
        "promptTokens": { "type": "integer" },
        "completionTokens": { "type": "integer" },
        "totalCostUsd": { "type": "number" }
      }
    }
  },
  "required": ["header", "output"]
}

```

---

## 4. PostgreSQL Database Schema

The database stores state definitions, agent configurations, workflow templates, execution logs, and human-in-the-loop tickets.

```sql
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Agent Definitions Table
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

-- 2. Workflow Definitions Table (DAG definition)
CREATE TABLE workflow_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(128) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    dag_structure JSONB NOT NULL, -- Defines nodes, edges, dependencies
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_workflow_name_version UNIQUE (name, version)
);

-- 3. Workflow Execution Instances Table
CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, COMPLETED, FAILED, PAUSED_HITL, CANCELLED
    input_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    final_output JSONB,
    error_details JSONB,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_workflow_executions_status ON workflow_executions(status);

-- 4. Workflow Execution Checkpoints (DAG Nodes Execution)
CREATE TABLE workflow_checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    step_id VARCHAR(64) NOT NULL,
    agent_type VARCHAR(64) NOT NULL,
    task_id UUID NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL, -- IN_PROGRESS, COMPLETED, FAILED, SKIPPED, WAITING_HITL
    input_payload JSONB,
    output_payload JSONB,
    retry_count INT NOT NULL DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT uq_execution_step UNIQUE (workflow_execution_id, step_id)
);

CREATE INDEX idx_checkpoints_execution_id ON workflow_checkpoints(workflow_execution_id);
CREATE INDEX idx_checkpoints_task_id ON workflow_checkpoints(task_id);

-- 5. Human-In-The-Loop (HITL) Alerts & Escalations Table
CREATE TABLE hitl_tickets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id),
    checkpoint_id UUID REFERENCES workflow_checkpoints(id),
    reason VARCHAR(128) NOT NULL, -- TIMEOUT_EXCEEDED, LLM_FAILURE_LIMIT, VALIDATION_FAILED, EXPLICIT_HUMAN_APPROVAL
    status VARCHAR(32) NOT NULL DEFAULT 'OPEN', -- OPEN, APPROVED, REJECTED, RESOLVED, EXPIRED
    assigned_role VARCHAR(64) DEFAULT 'OPERATOR',
    alert_channel_sent VARCHAR(64), -- SLACK, PAGERDUTY, EMAIL
    context_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolution_notes TEXT,
    resolved_by VARCHAR(128),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_hitl_tickets_status ON hitl_tickets(status);

```

---

## 5. Redis Data Structures & Caching Layer

Redis acts as a high-speed ephemeral memory store to handle locks, idempotency verification, state caching, and context isolation.

```
Redis Key Patterns:
+------------------------------------+------------------------+---------------------------------------+
| Key Pattern                        | Data Type              | Purpose / TTL                         |
+------------------------------------+------------------------+---------------------------------------+
| lock:task:{taskId}                 | String (Redlock)       | Prevents multi-consumer duplicate execution (TTL: 5m) |
| dedup:task:{taskId}                | String                 | Idempotency check for result events (TTL: 24h)        |
| context:workflow:{workflowId}      | Hash                   | High-speed accumulation of DAG outputs (TTL: 7d)     |
| active_timer:{taskId}              | String                 | Active execution heartbeat marker (TTL: Task Timeout) |
| rate_limit:llm:{provider}          | String (Sliding Window)| Token/Request rate limiting tracker                   |
+------------------------------------+------------------------+---------------------------------------+

```

---

## 6. Resilience, Timeout & Fault-Tolerance Mechanisms

```
                        +----------------------------+
                        |  Agent Worker Execution    |
                        +-------------+--------------+
                                      |
                            [ Execute LLM Task ]
                                      |
                   +------------------+------------------+
                   |                                     |
               (Success)                              (Failure)
                   |                                     |
                   v                                     v
       [ Post Results to Kafka ]             [ Increment Retry Count ]
                                                         |
                                             +-----------+-----------+
                                             |                       |
                                       (Attempts < Max)      (Attempts >= Max)
                                             |                       |
                                             v                       v
                                   [ Exponential Backoff ]   [ Send to HITL Queue / DLQ ]
                                   [ Re-queue in Kafka   ]   [ Fire Alert Notification  ]

```

1. **At-Least-Once Delivery & Idempotency**:
* All Kafka consumers operate under `ENABLE_AUTO_COMMIT = false`. Offsets are committed manually only after atomic DB transactions (Checkpoint + Redis State write) succeed.
* Consumers check `dedup:task:{taskId}` in Redis before executing task logic.


2. **Timeout Handling Strategy**:
* When an agent task is dispatched, a scheduled delayed message is pushed to `agent.control.timeouts` with a target timestamp $T_{target} = T_{now} + \text{timeoutSeconds}$.
* A dedicated **Timeout Monitor Service** consumes `agent.control.timeouts`.
* If `workflow_checkpoints` shows the step is still `IN_PROGRESS` and $T_{current} \ge T_{target}$, the monitor triggers a retry or escalates to `agent.control.hitl`.


3. **Circuit Breaking**:
* Integrated Resilience4j circuit breakers wrap downstream LLM API requests (e.g., OpenAI/Anthropic calls).
* If error rate exceeds 50% over a 1-minute window, the circuit opens, causing workers to temporarily pause consumption on `agent.tasks.<type>` (`consumer.pause()`) to prevent cascading backpressure failures.



---

## 7. Human-in-the-Loop (HITL) & Alerting Architecture

### 7.1 Escalation Triggers

A workflow transitions to `PAUSED_HITL` and triggers an active human notification when:

* A task fails repeatedly and consumes all max retry attempts.
* A task timeout occurs and automated retries fail to resolve it.
* A guardrail service flags LLM output as unsafe, low-confidence, or malformed.
* A DAG step explicitly specifies `requires_human_approval: true`.

### 7.2 Multi-Channel Alert Routing

```
[ HITL Event Triggered ]
          |
          +---> Write Ticket to `hitl_tickets` Table (PostgreSQL)
          |
          +---> Publish Event to `agent.control.hitl` (Kafka)
          |
          v
+---------------------------------------------------------------------------------------------------+
|                                 HITL Alerting Dispatcher Service                                  |
+-------------------+-----------------------------------+-------------------------------------------+
                    |                                   |
                    v                                   v
         [ Slack Webhook Engine ]              [ PagerDuty Alert API ]
         - Interactive Buttons                 - High-severity trigger
         - Direct DAG resume link              - Escalation policy routing

```

#### Slack Webhook Payload Example:

```json
{
  "text": "🚨 *AI Agent Workflow Interrupted - Action Required*",
  "blocks": [
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*Workflow ID:* `wf-9876-abc`\n*Step:* `code_generation_step`\n*Reason:* Execution failed after 3 retries (LLM Rate Limit / Timeout)."
      }
    },
    {
      "type": "actions",
      "block_id": "hitl_actions",
      "elements": [
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Retry Step" },
          "style": "primary",
          "value": "action=retry&ticket_id=ticket-123"
        },
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Skip & Continue" },
          "value": "action=skip&ticket_id=ticket-123"
        },
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Abort Workflow" },
          "style": "danger",
          "value": "action=abort&ticket_id=ticket-123"
        }
      ]
    }
  ]
}

```

---

## 8. Observability & Monitoring Strategy

1. **Distributed Tracing (OpenTelemetry)**:
* W3C Trace Context (`traceparent`) is injected into Kafka record headers.
* Spans are propagated seamlessly across REST calls, Kafka messages, DB interactions, and external LLM requests.


2. **Key Prometheus Metrics**:

| Metric Name | Type | Description |
| --- | --- | --- |
| `kafka_consumer_lag_records` | Gauge | Consumer group lag per topic/partition |
| `agent_task_execution_duration_seconds` | Histogram | Execution time per agent type and status |
| `llm_token_usage_total` | Counter | Accumulated tokens (prompt & completion) per model |
| `workflow_execution_total` | Counter | Total workflow executions by status (success/failed/hitl) |
| `hitl_ticket_open_count` | Gauge | Active unresolved human escalation tickets |

---

## 9. Maven Project Structure & Core Java Implementation

### 9.1 Multi-Module Project Layout

```
ai-agent-platform/
├── pom.xml
├── agent-core-bom/
│   └── pom.xml
├── agent-domain/
│   ├── pom.xml
│   └── src/main/java/com/platform/domain/ (Entities, DTOs, Event Models)
├── agent-orchestrator/
│   ├── pom.xml
│   └── src/main/java/com/platform/orchestrator/ (DAG Engine, Checkpoint Service)
├── agent-worker/
│   ├── pom.xml
│   └── src/main/java/com/platform/worker/ (Kafka Listeners, LLM Runners, Dynamic Config Loader)
└── agent-alerting/
    ├── pom.xml
    └── src/main/java/com/platform/alerting/ (Slack/PagerDuty Dispatchers, HITL Handlers)

```

---

### 9.2 Core Root `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="[http://maven.apache.org/POM/4.0.0](http://maven.apache.org/POM/4.0.0)"
         xmlns:xsi="[http://www.w3.org/2001/XMLSchema-instance](http://www.w3.org/2001/XMLSchema-instance)"
         xsi:schemaLocation="[http://maven.apache.org/POM/4.0.0](http://maven.apache.org/POM/4.0.0) 
                             [https://maven.apache.org/xsd/maven-4.0.0.xsd](https://maven.apache.org/xsd/maven-4.0.0.xsd)">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.platform</groupId>
    <artifactId>ai-agent-platform</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>AI Agent Platform Parent</name>

    <modules>
        <module>agent-domain</module>
        <module>agent-orchestrator</module>
        <module>agent-worker</module>
        <module>agent-alerting</module>
    </modules>

    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <spring-boot.version>3.3.3</spring-boot.version>
        <spring-kafka.version>3.2.3</spring-kafka.version>
        <opentelemetry.version>1.38.0</opentelemetry.version>
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
                    <compilerArgs>
                        <arg>--enable-preview</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>

```

---

### 9.3 Dynamic Agent Config Loader (Java Implementation)

The following component dynamically fetches agent configurations from PostgreSQL and maintains an in-memory cache updated via Kafka config update broadcasts.

```java
package com.platform.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DynamicAgentConfigRegistry {

    private static final Logger log = LoggerFactory.getLogger(DynamicAgentConfigRegistry.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final Map<String, AgentConfig> configCache = new ConcurrentHashMap<>();

    public DynamicAgentConfigRegistry(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initialize() {
        log.info("Loading initial agent definitions from PostgreSQL...");
        reloadAllConfigs();
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

        log.info("Successfully loaded {} agent configurations into memory.", configCache.size());
    }

    public AgentConfig getConfig(String agentType) {
        AgentConfig config = configCache.get(agentType);
        if (config == null) {
            throw new IllegalArgumentException("No configuration registered for agent type: " + agentType);
        }
        return config;
    }

    @KafkaListener(topics = "agent.config.updates", groupId = "agent-worker-config-group")
    public void onConfigUpdateEvent(String message) {
        try {
            log.info("Received dynamic agent config update event: {}", message);
            ConfigUpdatePayload payload = objectMapper.readValue(message, ConfigUpdatePayload.class);
            
            // Reload specific configuration
            reloadSingleConfig(payload.agentType());
        } catch (Exception e) {
            log.error("Failed to process dynamic config update", e);
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
            log.info("Dynamically updated memory cache for agent type: {}", agentType);
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

---

### 9.4 Kafka Agent Task Consumer (Java Implementation)

```java
package com.platform.worker.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.worker.config.DynamicAgentConfigRegistry;
import com.platform.worker.config.DynamicAgentConfigRegistry.AgentConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class GenericAgentTaskConsumer {

    private static final Logger log = LoggerFactory.getLogger(GenericAgentTaskConsumer.class);

    private final DynamicAgentConfigRegistry configRegistry;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public GenericAgentTaskConsumer(DynamicAgentConfigRegistry configRegistry,
                                    StringRedisTemplate redisTemplate,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ObjectMapper objectMapper) {
        this.configRegistry = configRegistry;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "#{'${app.kafka.agent-topics}'.split(',')}",
        groupId = "agent-worker-execution-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAgentTask(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String taskId = null;
        try {
            TaskEvent taskEvent = objectMapper.readValue(record.value(), TaskEvent.class);
            taskId = taskEvent.header().taskId();
            String agentType = taskEvent.agentConfig().agentType();

            log.info("Processing task ID: {} for agent type: {}", taskId, agentType);

            // 1. Redis Deduplication Check
            String dedupKey = "dedup:task:" + taskId;
            Boolean isFirstExecution = redisTemplate.opsForValue().setIfAbsent(dedupKey, "IN_PROGRESS", Duration.ofHours(24));
            if (Boolean.FALSE.equals(isFirstExecution)) {
                log.warn("Duplicate task execution detected for task ID: {}. Skipping execution.", taskId);
                ack.acknowledge();
                return;
            }

            // 2. Fetch Dynamic Config from DB-backed Registry
            AgentConfig config = configRegistry.getConfig(agentType);

            // 3. Execute Agent Task Logic (LLM API Call)
            ExecutionResult result = executeAgentLogic(taskEvent, config);

            // 4. Publish Result to Kafka
            publishResult(taskEvent.header().workflowId(), taskId, agentType, "COMPLETED", result.outputData());

            // 5. Commit Kafka Offset
            ack.acknowledge();
            log.info("Successfully completed task ID: {}", taskId);

        } catch (Exception e) {
            log.error("Error executing task ID: {}", taskId, e);
            handleExecutionFailure(record, e);
            ack.acknowledge(); // Acknowledge to move past failed record after routing to DLQ/Retry
        }
    }

    private ExecutionResult executeAgentLogic(TaskEvent taskEvent, AgentConfig config) {
        // Placeholder for LLM service invocation using dynamic config parameters
        log.info("Invoking model {} with temperature {}", config.modelName(), config.temperature());
        return new ExecutionResult("{\"status\": \"SUCCESS\", \"generatedText\": \"Sample AI output\"}");
    }

    private void publishResult(String workflowId, String taskId, String agentType, String status, String data) {
        try {
            String payload = String.format(
                "{\"header\":{\"workflowId\":\"%s\",\"taskId\":\"%s\",\"agentType\":\"%s\",\"status\":\"%s\"},\"output\":%s}",
                workflowId, taskId, agentType, status, data
            );
            kafkaTemplate.send("agent.execution.results", workflowId, payload);
        } catch (Exception e) {
            log.error("Failed to publish execution result to Kafka", e);
        }
    }

    private void handleExecutionFailure(ConsumerRecord<String, String> record, Exception e) {
        // Route to DLQ or HITL topic based on exception severity
        kafkaTemplate.send("agent.execution.dlq", record.key(), record.value());
    }

    public record TaskHeader(String workflowId, String taskId, String traceParent) {}
    public record AgentConfigHeader(String agentType) {}
    public record TaskEvent(TaskHeader header, AgentConfigHeader agentConfig, Object payload) {}
    public record ExecutionResult(String outputData) {}
}

```

---

## 10. Summary & Deployment Readiness Checklist

1. **Kafka Cluster Setup**: Configure topics with appropriate replication factors ($\ge 3$), min-in-sync replicas ($2$), and explicit retention rules.
2. **PostgreSQL Migrations**: Apply Flyway / Liquibase scripts for the initial schema setup (`agent_definitions`, `workflow_executions`, `workflow_checkpoints`, `hitl_tickets`).
3. **Redis Cluster**: Ensure Redis is configured with AOF/RDB persistence and memory eviction set to `volatile-lru`.
4. **OpenTelemetry Agent**: Pass `-javaagent:opentelemetry-javaagent.jar` to Java container runtimes for automatic distributed tracing instrumentation across Kafka and HTTP clients.
5. **Human Notification Channels**: Register Slack Webhook endpoints and PagerDuty routing keys in Spring application secrets.
"""

# Write to file

with open("ai_agent_system_spec.md", "w") as f:
f.write(markdown_content)

print("Specification written to ai_agent_system_spec.md")

```

```text?code_stdout&code_event_index=1
Traceback (most recent call last):
  File "<xbox-string>", line 545
    SELECT agent_type, system_prompt, model_name, temperature, 
IndentationError: unexpected indent


```

```python
markdown_content = r"""# System Specification: Event-Driven AI Agent Platform

## 1. Executive Summary & Core Architecture

The **Event-Driven AI Agent Platform** is an enterprise-grade, highly available, and asynchronous multi-agent orchestrator. The platform uses **Apache Kafka** as its central event bus for command and result propagation, **Java 21 (LTS)** with **Spring Boot 3.3 / Spring Kafka** for execution and orchestration runtimes, **PostgreSQL 16** for durable workflow states, DAG execution checkpoints, and database-driven dynamic agent configuration, and **Redis 7.2** for ephemeral runtime state, task deduplication, and locking.

### 1.1 Architectural Principles
1. **Fully Asynchronous Execution**: Communication between agents, orchestrators, and system services is non-blocking and strictly decoupled via Kafka topics.
2. **Database-Driven Dynamic Agent Configuration**: Agent parameters, prompts, tools, model configurations, and runtime limits are loaded dynamically from PostgreSQL. Changes are broadcasted via Change Data Capture (CDC / Debezium) or config-update events without requiring service redeployment or pod restarts.
3. **Resilience & Fault Tolerance**: Mandatory idempotency keys, exponential backoffs, Dead Letter Queues (DLQ), automated circuit breaking for LLM providers, and atomic checkpointing ensure reliable execution.
4. **End-to-End Observability**: Full distributed tracing using OpenTelemetry, W3C Trace Context propagation across Kafka headers, Prometheus metrics, and Grafana dashboards.
5. **Human-in-the-Loop (HITL) Alerting**: Intelligent fallback and alerting mechanisms via Webhooks, Slack, PagerDuty, and Email when workflows hit manual review thresholds, continuous LLM failures, or unexpected DAG blockages.

---

## 2. Technology Stack & Component Overview


```

+---------------------------------------------------------------------------------------------------+
|                                        API Gateway / Frontend                                     |
+---------------------------------------------------------------------------------------------------+
|
v
+---------------------------------------------------------------------------------------------------+
|                                 Kafka Event Bus (Cluster)                                         |
|  - agent.execution.intake        - agent.tasks.<agent_type>      - agent.execution.results        |
|  - agent.control.timeouts        - agent.control.cancellation    - agent.control.hitl             |
|  - agent.execution.dlq           - telemetry.agent.metrics       - agent.config.updates           |
+---------------------------------------------------------------------------------------------------+
^                                    ^                                    ^
|                                    |                                    |
+---------------+                  +---------------+                    +---------------+
| Orchestrator  |                  | Agent Workers |                    | Timeout & HITL|
| Engine (Java) |                  | (Generic Java)|                    | Service (Java)|
+---------------+                  +---------------+                    +---------------+
|                                    |                                    |
+-----------------+------------------+------------------------------------+
|
v
+--------------------------------------------------+          +----------------------------------+
|               PostgreSQL 16                      |          |            Redis 7.2             |
| - Workflow DAGs & States (Checkpoints)           |          | - Ephemeral Context & Tokens     |
| - Agent Definitions & Versioning                 |          | - Distributed Task Locks         |
| - HITL Escalation Logs                           |          | - Task Deduplication Keys        |
+--------------------------------------------------+          +----------------------------------+

```

| Component | Technology | Version | Purpose |
| :--- | :--- | :--- | :--- |
| **Language Runtime** | Java OpenJDK | 21 (LTS) | Core platform runtime leveraging Virtual Threads (Project Loom) |
| **Build System** | Apache Maven | 3.9+ | Dependency management, compilation, container image generation |
| **Framework** | Spring Boot / Spring Kafka | 3.3.x | Core IOC container, Kafka integration, JPA, REST interfaces |
| **Event Broker** | Apache Kafka / Strimzi | 3.7+ | Core event bus, message queuing, topic streaming |
| **Primary Database** | PostgreSQL | 16.x | Persistent state, dynamic configuration, workflow checkpoints |
| **Cache / In-Memory State** | Redis | 7.2.x | Ephemeral context buffering, rate-limiting, task deduplication |
| **CDC Engine** | Debezium / PgOutput | 2.5+ | Database Change Data Capture for dynamic config updates |
| **Metrics & Tracing** | OpenTelemetry / Micrometer | 1.30+ | Distributed tracing, Prometheus metrics exporter |
| **Human Alerting** | Slack Webhooks / PagerDuty API | Native REST | Escalation notifications for failed or stalled workflows |

---

## 3. Kafka Topic Topology & Schemas

### 3.1 Topic Definitions

| Topic Name | Partitions | Cleanup Policy | Key Strategy | Retention | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `agent.execution.intake` | 12 | `delete` | `workflow_id` | 7 Days | Trigger point for incoming workflow requests |
| `agent.tasks.<agent_type>` | 12 | `delete` | `workflow_id` | 7 Days | Agent-specific execution task queues |
| `agent.execution.results` | 12 | `delete` | `workflow_id` | 7 Days | Execution responses and status updates from workers |
| `agent.control.timeouts` | 6 | `delete` | `task_id` | 3 Days | Scheduled delay events for monitoring hanging tasks |
| `agent.control.cancellation` | 6 | `delete` | `workflow_id` | 1 Day | Signal to cancel active executions across workers |
| `agent.control.hitl` | 6 | `delete` | `workflow_id` | 14 Days | Queue for tasks requiring human review or escalation |
| `agent.execution.dlq` | 6 | `delete` | `workflow_id` | 30 Days | Isolation queue for unrecoverable errors |
| `agent.config.updates` | 1 | `compact` | `agent_type` | Compacted | Broadcast topic for dynamic DB config reloading |

---

### 3.2 Key Payload Schemas

#### Task Assignment Event (`agent.tasks.<agent_type>`)
```json
{
  "$schema": "[https://json-schema.org/draft/2020-12/schema](https://json-schema.org/draft/2020-12/schema)",
  "title": "AgentTaskEvent",
  "type": "object",
  "properties": {
    "header": {
      "type": "object",
      "properties": {
        "workflowId": { "type": "string", "format": "uuid" },
        "taskId": { "type": "string", "format": "uuid" },
        "parentTaskId": { "type": "string", "format": "uuid", "nullable": true },
        "traceParent": { "type": "string" },
        "attempt": { "type": "integer", "minimum": 1 },
        "maxAttempts": { "type": "integer", "minimum": 1 },
        "createdAt": { "type": "string", "format": "date-time" },
        "timeoutMs": { "type": "integer", "default": 60000 }
      },
      "required": ["workflowId", "taskId", "traceParent", "attempt", "createdAt"]
    },
    "agentConfig": {
      "type": "object",
      "properties": {
        "agentType": { "type": "string" },
        "version": { "type": "string" },
        "model": { "type": "string" },
        "temperature": { "type": "number" }
      },
      "required": ["agentType", "version"]
    },
    "payload": {
      "type": "object",
      "properties": {
        "inputPrompt": { "type": "string" },
        "contextReferences": {
          "type": "array",
          "items": { "type": "string" }
        },
        "parameters": { "type": "object" }
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
  "$schema": "[https://json-schema.org/draft/2020-12/schema](https://json-schema.org/draft/2020-12/schema)",
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
        "executionDurationMs": { "type": "integer" }
      },
      "required": ["workflowId", "taskId", "agentType", "status", "completedAt"]
    },
    "output": {
      "type": "object",
      "properties": {
        "resultData": { "type": "object" },
        "errorMessage": { "type": "string", "nullable": true },
        "stackTrace": { "type": "string", "nullable": true }
      }
    },
    "usage": {
      "type": "object",
      "properties": {
        "promptTokens": { "type": "integer" },
        "completionTokens": { "type": "integer" },
        "totalCostUsd": { "type": "number" }
      }
    }
  },
  "required": ["header", "output"]
}

```

---

## 4. PostgreSQL Database Schema

The database stores state definitions, agent configurations, workflow templates, execution logs, and human-in-the-loop tickets.

```sql
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Agent Definitions Table
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

-- 2. Workflow Definitions Table (DAG definition)
CREATE TABLE workflow_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(128) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    dag_structure JSONB NOT NULL, -- Defines nodes, edges, dependencies
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_workflow_name_version UNIQUE (name, version)
);

-- 3. Workflow Execution Instances Table
CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, COMPLETED, FAILED, PAUSED_HITL, CANCELLED
    input_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    final_output JSONB,
    error_details JSONB,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_workflow_executions_status ON workflow_executions(status);

-- 4. Workflow Execution Checkpoints (DAG Nodes Execution)
CREATE TABLE workflow_checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    step_id VARCHAR(64) NOT NULL,
    agent_type VARCHAR(64) NOT NULL,
    task_id UUID NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL, -- IN_PROGRESS, COMPLETED, FAILED, SKIPPED, WAITING_HITL
    input_payload JSONB,
    output_payload JSONB,
    retry_count INT NOT NULL DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT uq_execution_step UNIQUE (workflow_execution_id, step_id)
);

CREATE INDEX idx_checkpoints_execution_id ON workflow_checkpoints(workflow_execution_id);
CREATE INDEX idx_checkpoints_task_id ON workflow_checkpoints(task_id);

-- 5. Human-In-The-Loop (HITL) Alerts & Escalations Table
CREATE TABLE hitl_tickets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id),
    checkpoint_id UUID REFERENCES workflow_checkpoints(id),
    reason VARCHAR(128) NOT NULL, -- TIMEOUT_EXCEEDED, LLM_FAILURE_LIMIT, VALIDATION_FAILED, EXPLICIT_HUMAN_APPROVAL
    status VARCHAR(32) NOT NULL DEFAULT 'OPEN', -- OPEN, APPROVED, REJECTED, RESOLVED, EXPIRED
    assigned_role VARCHAR(64) DEFAULT 'OPERATOR',
    alert_channel_sent VARCHAR(64), -- SLACK, PAGERDUTY, EMAIL
    context_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolution_notes TEXT,
    resolved_by VARCHAR(128),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_hitl_tickets_status ON hitl_tickets(status);

```

---

## 5. Redis Data Structures & Caching Layer

Redis acts as a high-speed ephemeral memory store to handle locks, idempotency verification, state caching, and context isolation.

```
Redis Key Patterns:
+------------------------------------+------------------------+---------------------------------------+
| Key Pattern                        | Data Type              | Purpose / TTL                         |
+------------------------------------+------------------------+---------------------------------------+
| lock:task:{taskId}                 | String (Redlock)       | Prevents multi-consumer duplicate execution (TTL: 5m) |
| dedup:task:{taskId}                | String                 | Idempotency check for result events (TTL: 24h)        |
| context:workflow:{workflowId}      | Hash                   | High-speed accumulation of DAG outputs (TTL: 7d)     |
| active_timer:{taskId}              | String                 | Active execution heartbeat marker (TTL: Task Timeout) |
| rate_limit:llm:{provider}          | String (Sliding Window)| Token/Request rate limiting tracker                   |
+------------------------------------+------------------------+---------------------------------------+

```

---

## 6. Resilience, Timeout & Fault-Tolerance Mechanisms

```
                        +----------------------------+
                        |  Agent Worker Execution    |
                        +-------------+--------------+
                                      |
                            [ Execute LLM Task ]
                                      |
                   +------------------+------------------+
                   |                                     |
               (Success)                              (Failure)
                   |                                     |
                   v                                     v
       [ Post Results to Kafka ]             [ Increment Retry Count ]
                                                         |
                                             +-----------+-----------+
                                             |                       |
                                       (Attempts < Max)      (Attempts >= Max)
                                             |                       |
                                             v                       v
                                   [ Exponential Backoff ]   [ Send to HITL Queue / DLQ ]
                                   [ Re-queue in Kafka   ]   [ Fire Alert Notification  ]

```

1. **At-Least-Once Delivery & Idempotency**:
* All Kafka consumers operate under `ENABLE_AUTO_COMMIT = false`. Offsets are committed manually only after atomic DB transactions (Checkpoint + Redis State write) succeed.
* Consumers check `dedup:task:{taskId}` in Redis before executing task logic.


2. **Timeout Handling Strategy**:
* When an agent task is dispatched, a scheduled delayed message is pushed to `agent.control.timeouts` with a target timestamp $T_{target} = T_{now} + \text{timeoutSeconds}$.
* A dedicated **Timeout Monitor Service** consumes `agent.control.timeouts`.
* If `workflow_checkpoints` shows the step is still `IN_PROGRESS` and $T_{current} \ge T_{target}$, the monitor triggers a retry or escalates to `agent.control.hitl`.


3. **Circuit Breaking**:
* Integrated Resilience4j circuit breakers wrap downstream LLM API requests (e.g., OpenAI/Anthropic calls).
* If error rate exceeds 50% over a 1-minute window, the circuit opens, causing workers to temporarily pause consumption on `agent.tasks.<type>` (`consumer.pause()`) to prevent cascading backpressure failures.



---

## 7. Human-in-the-Loop (HITL) & Alerting Architecture

### 7.1 Escalation Triggers

A workflow transitions to `PAUSED_HITL` and triggers an active human notification when:

* A task fails repeatedly and consumes all max retry attempts.
* A task timeout occurs and automated retries fail to resolve it.
* A guardrail service flags LLM output as unsafe, low-confidence, or malformed.
* A DAG step explicitly specifies `requires_human_approval: true`.

### 7.2 Multi-Channel Alert Routing

```
[ HITL Event Triggered ]
          |
          +---> Write Ticket to `hitl_tickets` Table (PostgreSQL)
          |
          +---> Publish Event to `agent.control.hitl` (Kafka)
          |
          v
+---------------------------------------------------------------------------------------------------+
|                                 HITL Alerting Dispatcher Service                                  |
+-------------------+-----------------------------------+-------------------------------------------+
                    |                                   |
                    v                                   v
         [ Slack Webhook Engine ]              [ PagerDuty Alert API ]
         - Interactive Buttons                 - High-severity trigger
         - Direct DAG resume link              - Escalation policy routing

```

#### Slack Webhook Payload Example:

```json
{
  "text": "🚨 *AI Agent Workflow Interrupted - Action Required*",
  "blocks": [
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*Workflow ID:* `wf-9876-abc`\n*Step:* `code_generation_step`\n*Reason:* Execution failed after 3 retries (LLM Rate Limit / Timeout)."
      }
    },
    {
      "type": "actions",
      "block_id": "hitl_actions",
      "elements": [
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Retry Step" },
          "style": "primary",
          "value": "action=retry&ticket_id=ticket-123"
        },
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Skip & Continue" },
          "value": "action=skip&ticket_id=ticket-123"
        },
        {
          "type": "button",
          "text": { "type": "plain_text", "text": "Abort Workflow" },
          "style": "danger",
          "value": "action=abort&ticket_id=ticket-123"
        }
      ]
    }
  ]
}

```

---

## 8. Observability & Monitoring Strategy

1. **Distributed Tracing (OpenTelemetry)**:
* W3C Trace Context (`traceparent`) is injected into Kafka record headers.
* Spans are propagated seamlessly across REST calls, Kafka messages, DB interactions, and external LLM requests.


2. **Key Prometheus Metrics**:

| Metric Name | Type | Description |
| --- | --- | --- |
| `kafka_consumer_lag_records` | Gauge | Consumer group lag per topic/partition |
| `agent_task_execution_duration_seconds` | Histogram | Execution time per agent type and status |
| `llm_token_usage_total` | Counter | Accumulated tokens (prompt & completion) per model |
| `workflow_execution_total` | Counter | Total workflow executions by status (success/failed/hitl) |
| `hitl_ticket_open_count` | Gauge | Active unresolved human escalation tickets |

---

## 9. Maven Project Structure & Core Java Implementation

### 9.1 Multi-Module Project Layout

```
ai-agent-platform/
├── pom.xml
├── agent-core-bom/
│   └── pom.xml
├── agent-domain/
│   ├── pom.xml
│   └── src/main/java/com/platform/domain/ (Entities, DTOs, Event Models)
├── agent-orchestrator/
│   ├── pom.xml
│   └── src/main/java/com/platform/orchestrator/ (DAG Engine, Checkpoint Service)
├── agent-worker/
│   ├── pom.xml
│   └── src/main/java/com/platform/worker/ (Kafka Listeners, LLM Runners, Dynamic Config Loader)
└── agent-alerting/
    ├── pom.xml
    └── src/main/java/com/platform/alerting/ (Slack/PagerDuty Dispatchers, HITL Handlers)

```

---

### 9.2 Core Root `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="[http://maven.apache.org/POM/4.0.0](http://maven.apache.org/POM/4.0.0)"
         xmlns:xsi="[http://www.w3.org/2001/XMLSchema-instance](http://www.w3.org/2001/XMLSchema-instance)"
         xsi:schemaLocation="[http://maven.apache.org/POM/4.0.0](http://maven.apache.org/POM/4.0.0) 
                             [https://maven.apache.org/xsd/maven-4.0.0.xsd](https://maven.apache.org/xsd/maven-4.0.0.xsd)">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.platform</groupId>
    <artifactId>ai-agent-platform</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>AI Agent Platform Parent</name>

    <modules>
        <module>agent-domain</module>
        <module>agent-orchestrator</module>
        <module>agent-worker</module>
        <module>agent-alerting</module>
    </modules>

    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <spring-boot.version>3.3.3</spring-boot.version>
        <spring-kafka.version>3.2.3</spring-kafka.version>
        <opentelemetry.version>1.38.0</opentelemetry.version>
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
                    <compilerArgs>
                        <arg>--enable-preview</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>

```

---

### 9.3 Dynamic Agent Config Loader (Java Implementation)

The following component dynamically fetches agent configurations from PostgreSQL and maintains an in-memory cache updated via Kafka config update broadcasts.

```java
package com.platform.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DynamicAgentConfigRegistry {

    private static final Logger log = LoggerFactory.getLogger(DynamicAgentConfigRegistry.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final Map<String, AgentConfig> configCache = new ConcurrentHashMap<>();

    public DynamicAgentConfigRegistry(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initialize() {
        log.info("Loading initial agent definitions from PostgreSQL...");
        reloadAllConfigs();
    }

    public void reloadAllConfigs() {
        String sql = "SELECT agent_type, system_prompt, model_name, temperature, " +
                     "max_tokens, tools_config, timeout_seconds, max_retry_attempts, version " +
                     "FROM agent_definitions";

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

        log.info("Successfully loaded {} agent configurations into memory.", configCache.size());
    }

    public AgentConfig getConfig(String agentType) {
        AgentConfig config = configCache.get(agentType);
        if (config == null) {
            throw new IllegalArgumentException("No configuration registered for agent type: " + agentType);
        }
        return config;
    }

    @KafkaListener(topics = "agent.config.updates", groupId = "agent-worker-config-group")
    public void onConfigUpdateEvent(String message) {
        try {
            log.info("Received dynamic agent config update event: {}", message);
            ConfigUpdatePayload payload = objectMapper.readValue(message, ConfigUpdatePayload.class);
            reloadSingleConfig(payload.agentType());
        } catch (Exception e) {
            log.error("Failed to process dynamic config update", e);
        }
    }

    private void reloadSingleConfig(String agentType) {
        String sql = "SELECT agent_type, system_prompt, model_name, temperature, " +
                     "max_tokens, tools_config, timeout_seconds, max_retry_attempts, version " +
                     "FROM agent_definitions WHERE agent_type = ?";

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
            log.info("Dynamically updated memory cache for agent type: {}", agentType);
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

---

### 9.4 Kafka Agent Task Consumer (Java Implementation)

```java
package com.platform.worker.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.worker.config.DynamicAgentConfigRegistry;
import com.platform.worker.config.DynamicAgentConfigRegistry.AgentConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class GenericAgentTaskConsumer {

    private static final Logger log = LoggerFactory.getLogger(GenericAgentTaskConsumer.class);

    private final DynamicAgentConfigRegistry configRegistry;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public GenericAgentTaskConsumer(DynamicAgentConfigRegistry configRegistry,
                                    StringRedisTemplate redisTemplate,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ObjectMapper objectMapper) {
        this.configRegistry = configRegistry;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "#{'${app.kafka.agent-topics}'.split(',')}",
        groupId = "agent-worker-execution-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAgentTask(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String taskId = null;
        try {
            TaskEvent taskEvent = objectMapper.readValue(record.value(), TaskEvent.class);
            taskId = taskEvent.header().taskId();
            String agentType = taskEvent.agentConfig().agentType();

            log.info("Processing task ID: {} for agent type: {}", taskId, agentType);

            // 1. Redis Deduplication Check
            String dedupKey = "dedup:task:" + taskId;
            Boolean isFirstExecution = redisTemplate.opsForValue().setIfAbsent(dedupKey, "IN_PROGRESS", Duration.ofHours(24));
            if (Boolean.FALSE.equals(isFirstExecution)) {
                log.warn("Duplicate task execution detected for task ID: {}. Skipping execution.", taskId);
                ack.acknowledge();
                return;
            }

            // 2. Fetch Dynamic Config from DB-backed Registry
            AgentConfig config = configRegistry.getConfig(agentType);

            // 3. Execute Agent Task Logic (LLM API Call)
            ExecutionResult result = executeAgentLogic(taskEvent, config);

            // 4. Publish Result to Kafka
            publishResult(taskEvent.header().workflowId(), taskId, agentType, "COMPLETED", result.outputData());

            // 5. Commit Kafka Offset
            ack.acknowledge();
            log.info("Successfully completed task ID: {}", taskId);

        } catch (Exception e) {
            log.error("Error executing task ID: {}", taskId, e);
            handleExecutionFailure(record, e);
            ack.acknowledge(); // Acknowledge to move past failed record after routing to DLQ/Retry
        }
    }

    private ExecutionResult executeAgentLogic(TaskEvent taskEvent, AgentConfig config) {
        log.info("Invoking model {} with temperature {}", config.modelName(), config.temperature());
        return new ExecutionResult("{\"status\": \"SUCCESS\", \"generatedText\": \"Sample AI output\"}");
    }

    private void publishResult(String workflowId, String taskId, String agentType, String status, String data) {
        try {
            String payload = String.format(
                "{\"header\":{\"workflowId\":\"%s\",\"taskId\":\"%s\",\"agentType\":\"%s\",\"status\":\"%s\"},\"output\":%s}",
                workflowId, taskId, agentType, status, data
            );
            kafkaTemplate.send("agent.execution.results", workflowId, payload);
        } catch (Exception e) {
            log.error("Failed to publish execution result to Kafka", e);
        }
    }

    private void handleExecutionFailure(ConsumerRecord<String, String> record, Exception e) {
        kafkaTemplate.send("agent.execution.dlq", record.key(), record.value());
    }

    public record TaskHeader(String workflowId, String taskId, String traceParent) {}
    public record AgentConfigHeader(String agentType) {}
    public record TaskEvent(TaskHeader header, AgentConfigHeader agentConfig, Object payload) {}
    public record ExecutionResult(String outputData) {}
}

```

---

## 10. Summary & Deployment Readiness Checklist

1. **Kafka Cluster Setup**: Configure topics with appropriate replication factors ($\ge 3$), min-in-sync replicas ($2$), and explicit retention rules.
2. **PostgreSQL Migrations**: Apply Flyway / Liquibase scripts for the initial schema setup (`agent_definitions`, `workflow_executions`, `workflow_checkpoints`, `hitl_tickets`).
3. **Redis Cluster**: Ensure Redis is configured with AOF/RDB persistence and memory eviction set to `volatile-lru`.
4. **OpenTelemetry Agent**: Pass `-javaagent:opentelemetry-javaagent.jar` to Java container runtimes for automatic distributed tracing instrumentation across Kafka and HTTP clients.
5. **Human Notification Channels**: Register Slack Webhook endpoints and PagerDuty routing keys in Spring application secrets.
"""

with open("ai_agent_system_spec.md", "w") as f:
f.write(markdown_content)

print("Specification written successfully.")

```

```text?code_stdout&code_event_index=2
Specification written successfully.


```

Die detaillierte Systemspezifikation für die **Event-Driven AI Agent Platform** wurde im Markdown-Format erstellt und als Datei gespeichert.

---

### Übersicht & Kernkomponenten der Spezifikation

#### 1. Systemarchitektur & Technologie-Stack

* **Runtime & Framework**: Java 21 (LTS mit Virtual Threads / Project Loom), Spring Boot 3.3, Spring Kafka.
* **Build System**: Multi-Module Maven Build (`agent-domain`, `agent-orchestrator`, `agent-worker`, `agent-alerting`).
* **Event Broker (Kafka)**: Entkoppelte Asynchronität, partitioning nach `workflow_id`, compacted Config-Updates.
* **Persistence & State**:
* **PostgreSQL 16**: Exakte DAG-Checkpoints, Workflow-Status, Versionierung & dynamic Agent Configurations.
* **Redis 7.2**: Ephemerer Context Cache, Redlock für Task-Sperren, Idempotenz-Prüfungen (`dedup:task:{id}`).


* **Observability**: Distributed Tracing via OpenTelemetry & W3C Trace Context Propagation über Kafka-Header.

---

#### 2. Kafka Topic Topologie

| Topic Name | Partitions | Key Strategy | Description |
| --- | --- | --- | --- |
| `agent.execution.intake` | 12 | `workflow_id` | Eingangskanal für neue Workflows |
| `agent.tasks.<agent_type>` | 12 | `workflow_id` | Agenten-spezifische Task-Queues |
| `agent.execution.results` | 12 | `workflow_id` | Ergebnisse & Status-Rückmeldungen |
| `agent.control.timeouts` | 6 | `task_id` | Monitoring verzögerter/hängender Tasks |
| `agent.control.hitl` | 6 | `workflow_id` | Escalation Queue für Human-in-the-Loop |
| `agent.config.updates` | 1 | `agent_type` | Compacted Topic für dynamic DB Config Reloads |

---

#### 3. PostgreSQL Schema (Auszug)

Das Schema beinhaltet Tabellen für:

* `agent_definitions`: Dynamic Configuration (Prompts, Model, Temperature, Max Retries).
* `workflow_definitions` & `workflow_executions`: DAG-Strukturen und globale Instanz-Zustände.
* `workflow_checkpoints`: Atomare Zustände einzelner Knoten im DAG.
* `hitl_tickets`: Audit-Trail & Statusverwaltung für manuelle Eingriffe.

---

#### 4. Dynamic Agent Configuration System

Agenten-Parameter (Prompts, Tools, Modelle) werden **dynamisch aus PostgreSQL** geladen und im Speicher gecached (`ConcurrentHashMap`). Bei Änderungen in der Datenbank wird ein Event auf `agent.config.updates` publiziert, wodurch Workers ohne Neustart ihre Konfiguration im laufenden Betrieb aktualisieren.

---

#### 5. Human-In-The-Loop (HITL) Alerting & Fallbacks

Wenn ein Agent maximale Retries überschreitet, Guardrails fehlschlagen oder Timeouts auftreten:

1. Der Workflow geht in den Zustand `PAUSED_HITL`.
2. Ein Ticket wird in `hitl_tickets` generiert.
3. Der **Alerting Dispatcher Service** feuert Notifications via **Slack Webhooks** (inkl. interaktiver Buttons für *Retry*, *Skip*, *Abort*) und **PagerDuty REST APIs**.

---

### Vorschau des generierten Java Code-Snippets (Dynamic Config Registry)

```java
package com.platform.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DynamicAgentConfigRegistry {

    private static final Logger log = LoggerFactory.getLogger(DynamicAgentConfigRegistry.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final Map<String, AgentConfig> configCache = new ConcurrentHashMap<>();

    public DynamicAgentConfigRegistry(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initialize() {
        reloadAllConfigs();
    }

    public void reloadAllConfigs() {
        String sql = "SELECT agent_type, system_prompt, model_name, temperature, " +
                     "max_tokens, tools_config, timeout_seconds, max_retry_attempts, version " +
                     "FROM agent_definitions";

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
    }

    @KafkaListener(topics = "agent.config.updates", groupId = "agent-worker-config-group")
    public void onConfigUpdateEvent(String message) {
        try {
            ConfigUpdatePayload payload = objectMapper.readValue(message, ConfigUpdatePayload.class);
            reloadSingleConfig(payload.agentType());
        } catch (Exception e) {
            log.error("Failed to process dynamic config update", e);
        }
    }

    private void reloadSingleConfig(String agentType) {
        String sql = "SELECT agent_type, system_prompt, model_name, temperature, " +
                     "max_tokens, tools_config, timeout_seconds, max_retry_attempts, version " +
                     "FROM agent_definitions WHERE agent_type = ?";

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
        String agentType, String systemPrompt, String modelName,
        double temperature, int maxTokens, String toolsConfigJson,
        int timeoutSeconds, int maxRetryAttempts, int version
    ) {}

    public record ConfigUpdatePayload(String agentType, int newVersion) {}
}

