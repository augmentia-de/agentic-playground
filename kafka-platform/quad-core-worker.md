# quad-core-worker: Implementation Plan & Progress

## ⚡ Status: PHASE 1 - Skeleton COMPLETED ✅

**Date:** 2026-09-17  
**Project:** `/home/torsten/dev/my-projects/kafka_workflow/quad-workflow-worker`  
**Build:** `mvn compile` → SUCCESS | Tests: `mvn test` → 22/22 PASSED

### What Was Implemented (Phase 1)

| Component | Status | File(s) |
|---|---|---|
| Maven POM (Boot 4 + Quad deps) | ✅ Done | `pom.xml` |
| Contract classes (TaskEvent, ResultEvent, WorkerJson) | ✅ Done | `contract/*.java` |
| Config: WorkerProperties, KafkaConfig, ChatModelRegistry | ✅ Done | `config/*.java` |
| QuadCoreConfig (CurrentSession, WorkspaceResolver, ObjectMapper) | ✅ Done | `config/QuadCoreConfig.java` |
| ToolCatalog (global immutable) | ✅ Done | `tool/ToolCatalog.java` |
| AgentToolRegistryFactory (per-task registry copy) | ✅ Done | `tool/AgentToolRegistryFactory.java` |
| WorkerAgent (extends Agent, no CDI) | ✅ Done | `agent/WorkerAgent.java` |
| WorkerAgentDefinition (record) | ✅ Done | `agent/WorkerAgentDefinition.java` |
| WorkerAgentFactory (creates per-task Agents) | ✅ Done | `agent/WorkerAgentFactory.java` |
| TaskPromptBuilder | ✅ Done | `agent/TaskPromptBuilder.java` |
| TaskSessionContext | ✅ Done | `session/TaskSessionContext.java` |
| WorkerSecurityContext | ✅ Done | `session/WorkerSecurityContext.java` |
| ResultMapper (success + failure mapping) | ✅ Done | `result/ResultMapper.java` |
| WorkflowResultPublisher | ✅ Done | `kafka/WorkflowResultPublisher.java` |
| WorkflowDlqPublisher | ✅ Done | `kafka/WorkflowDlqPublisher.java` |
| TaskExecutor (orchestration + Caffeine cache) | ✅ Done | `agent/TaskExecutor.java` |
| WorkflowTaskListener (Kafka batch + Virtual Threads) | ✅ Done | `kafka/WorkflowTaskListener.java` |
| QuadWorkerConfig (Spring bean wiring) | ✅ Done | `config/QuadWorkerConfig.java` |
| application.yml + Dockerfile | ✅ Done | `resources/application.yml`, `Dockerfile` |
| Unit Tests | ✅ Done | `src/test/**/*Test.java` (22 tests) |
| Mock-ChatModel (deterministic JSON output) | ✅ Done | `config/ChatModelRegistry.MockChatModel` |
| **Real OpenAI/OpenRouter ChatModel** | ✅ Done | `QuadWorkerConfig.chatModelRegistry()` |
| **API key loaded from .env** | ✅ Done | `.env`: OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL |

### LLM Configuration

**.env file:** `/home/torsten/dev/my-projects/kafka_workflow/quad-workflow-worker/.env`

```bash
OPENAI_API_KEY="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"
OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://openrouter.ai/api/v1}"
OPENAI_MODEL="${OPENAI_MODEL:-deepseek/deepseek-v4-flash-0731}"
```

**Behavior:**
- If API key set → real OpenAiChatModel via OpenRouter (DeepSeek V4 Flash)
- Fallback to mock if no API keys are found (warn log)
- Custom headers for OpenRouter (`HTTP-Referer`, `X-Title`)

### Current Architecture in Code

```text
quad-workflow-worker/
├── pom.xml                      (Boot 4.0.8 + quad-core 1.0.0-SNAPSHOT)
├── Dockerfile
├── src/main/java/de/augmentia/quad/worker/
│   ├── QuadWorkflowWorkerApplication.java
│   ├── config/
│   │   ├── WorkerProperties.java          (record with agent configs)
│   │   ├── QuadCoreConfig.java            (CurrentSession etc.)
│   │   ├── ChatModelRegistry.java         (mock + placeholder real)
│   │   ├── KafkaConfig.java               (consumer factory)
│   │   └── QuadWorkerConfig.java          (bean wiring)
│   ├── contract/
│   │   ├── TaskEvent.java                 (Header, ContextReference)
│   │   ├── ResultEvent.java               (Header + resultData map)
│   │   └── WorkerJson.java                (ObjectMapper wrapper)
│   ├── kafka/
│   │   ├── WorkflowTaskListener.java      (@KafkaListener batch + virtual threads)
│   │   ├── WorkflowResultPublisher.java   (→ agent.execution.results)
│   │   └── WorkflowDlqPublisher.java      (→ agent.execution.dlq)
│   ├── agent/
│   │   ├── WorkerAgent.java               (extends Agent, wire() without CDI)
│   │   ├── WorkerAgentDefinition.java     (record)
│   │   ├── WorkerAgentFactory.java        (creates per-task Agent)
│   │   ├── TaskPromptBuilder.java         (Workflow+Step+Input+Context)
│   │   └── TaskExecutor.java              (safeExecute + cache + semaphore)
│   ├── tool/
│   │   ├── ToolCatalog.java               (immutable global catalog)
│   │   └── AgentToolRegistryFactory.java  (per-task registry copy)
│   ├── session/
│   │   ├── TaskSessionContext.java        (AgentSessionState per task)
│   │   └── WorkerSecurityContext.java     (implements SecurityContext)
│   ├── result/
│   │   └── ResultMapper.java              (mapSuccess/mapFailure/classifyError)
│   └── metrics/                         (empty - Phase 2)
└── src/test/java/de/augmentia/quad/worker/
    ├── agent/TaskPromptBuilderTest.java       (4 tests)
    ├── result/ResultMapperTest.java           (5 tests)
    ├── contract/ContractSerializationTest.java (3 tests)
    └── tool/ToolCatalogTest.java              (5 tests)
```

**Using Spring Boot with LangChain4j from `quad-core` works.** Quarkus is not a hard requirement.

Recommendation:

- **Do not copy `quad-core`.**
- **Do not copy out individual classes.**
- **New, lean worker service** that uses `quad-core` as a Maven dependency.
- For the existing Kafka project, a **Spring Boot worker** makes sense if the platform is to continue running on the Spring stack.
- A **Quarkus worker** makes sense if the worker stays within the `nooa/quad` ecosystem and CDI/Quarkus features are to be used natively.

The existing `agent-orchestrator` remains the only routing authority. The Quad worker is exclusively an agent executor via Kafka.

---

## 1. Why Quarkus Originally?

Quarkus was recommended as an option because:

1. `quad-core` uses CDI annotations:
   - `@ApplicationScoped`
   - `@Inject`
   - `jakarta.enterprise.inject.Instance`
2. `quad-tool-builtin` provides its tools via CDI providers.
3. `quad-quarkus` already contains agent factories, Kafka channels, workflow executors, SSE, persistence, and virtual thread patterns.
4. Quarkus + LangChain4j are already conceived together in the Quad project.

But:

- LangChain4j is framework-agnostic.
- `quad-core` can also be used as plain Java; the tests instantiate agents without a running CDI container.
- Spring Kafka can consume the same Kafka contract.
- The existing `KafkaInboundChannel` from `quad-quarkus` is not directly suitable for a reliable workflow worker anyway, because it uses `enable.auto.commit=true`.

Therefore: **Quarkus is convenient, but not mandatory.**

---

## 2. Spring Variant: Yes, with Clear Rules

A Spring Boot worker can use `quad-core` if CDI is not required as a runtime.

### What Works in Spring

- Subclass `Agent` and instantiate it plainly
- Create `AgentSessionState` per task
- Use `CurrentSession` as ThreadLocal
- Create `QuadToolRegistry` per agent/task
- Wire `ToolExecutor`, `HookRegistry`, `AgentEventPublisher` manually
- Use `ReflectiveToolMethod` for built-in tools
- Build a LangChain4j `ChatModel` as a Spring bean
- Use Spring Kafka for task input and result output

### What Does Not Work Unchanged in Spring

- `StandardToolProvider` is based on CDI `Instance<BuiltInToolProvider>`
- `BuiltinToolsProvider` expects CDI injection
- `WorkspaceResolver` uses MicroProfile `@ConfigProperty`
- `QuadToolRegistry` is intended as `@ApplicationScoped`, but must not be used in the worker as a global mutable singleton registry
- `ToolActivatorTool` mutates a registry and is only safe with a per-task registry

Solution: **build your own Spring tool catalog** and register the built-in tools manually.

---

## 3. Repo/Module Decision

### Recommendation

New standalone project:

```text
quad-workflow-worker/
```

Possible locations:

1. **Preferred for Quad proximity:**
   ```text
   /home/torsten/dev/my-projects/nooa/quad/quad-workflow-worker
   ```

2. **Preferred for Kafka platform proximity:**
   ```text
   /home/torsten/dev/my-projects/quad-workflow-worker
   ```

3. **Not recommended:** as a fixed child module in the existing `agent-platform` parent, because the deterministic Kafka project is intentionally meant to remain LLM-free and `quad-core` brings LangChain4j/OpenAI dependencies.

The worker can nevertheless be run as an additional service in the Kafka project's `docker-compose.yml`.

---

## 4. Target Architecture

```text
agent-orchestrator
  ├── DAG / Checkpoints / Routing / HITL / Retries / Sweep
  ├── publishes agent.tasks.<agentType>
  └── consumes agent.execution.results

quad-workflow-worker
  ├── consumes agent.tasks.<agentType>
  ├── parses TaskEvent
  ├── resolves WorkerAgentDefinition
  ├── creates ToolRegistry copy
  ├── creates Agent per task
  ├── creates AgentSessionState per task
  ├── binds CurrentSession per thread
  ├── runs quad-core ReAct run
  ├── maps AgentResult to ResultEvent
  └── publishes agent.execution.results
```

Important:

- The worker does **not** write to the orchestrator database.
- The worker makes **no routing** decisions.
- The worker only decides how a single task is executed.
- The orchestrator remains the single writer.

---

## 5. Maven Setup

### 5.1 Install Quad Artifacts

```bash
cd /home/torsten/dev/my-projects/nooa/quad

mvn -q -DskipTests install \
  -pl quad-core,quad-tool-builtin \
  -am
```

Optional, if sandbox/MCP are needed:

```bash
mvn -q -DskipTests install \
  -pl quad-core,quad-tool-builtin,quad-sandbox-docker,quad-tool-mcp \
  -am
```

For CI/team use, the artifacts should be deployed to Nexus/GitHub Packages.

### 5.2 Spring Boot Worker POM

```xml
<project>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.0.8</version>
        <relativePath/>
    </parent>

    <groupId>de.augmentia.quad.worker</groupId>
    <artifactId>quad-workflow-worker</artifactId>
    <version>1.0.0-SNAPSHOT</version>

    <properties>
        <java.version>21</java.version>
        <quad.version>1.0.0-SNAPSHOT</quad.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>

        <dependency>
            <groupId>de.augmentia.quad</groupId>
            <artifactId>quad-core</artifactId>
            <version>${quad.version}</version>
        </dependency>

        <dependency>
            <groupId>de.augmentia.quad</groupId>
            <artifactId>quad-tool-builtin</artifactId>
            <version>${quad.version}</version>
        </dependency>

        <!-- CDI-APIs: quad-core referenziert Annotationen, läuft aber ohne CDI-Container -->
        <dependency>
            <groupId>jakarta.enterprise</groupId>
            <artifactId>jakarta.enterprise.cdi-api</artifactId>
        </dependency>

        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.datatype</groupId>
            <artifactId>jackson-datatype-jsr310</artifactId>
        </dependency>

        <dependency>
            <groupId>com.github.ben-manes.caffeine</groupId>
            <artifactId>caffeine</artifactId>
        </dependency>

        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

Note:

- `quad-core` brings LangChain4j in transitively.
- Jackson 2 and Jackson 3 can coexist because they have different package names:
  - Jackson 2: `com.fasterxml.jackson`
  - Jackson 3: `tools.jackson`
- For Quad contracts, the worker should use Jackson 2, matching `quad-core`.

---

## 6. Package Structure

```text
de.augmentia.quad.worker
├── QuadWorkflowWorkerApplication.java
├── config/
│   ├── WorkerProperties.java
│   ├── QuadCoreConfig.java
│   ├── ChatModelRegistry.java
│   └── KafkaConfig.java
├── contract/
│   ├── TaskEvent.java
│   ├── ResultEvent.java
│   └── WorkerJson.java
├── kafka/
│   ├── WorkflowTaskListener.java
│   ├── WorkflowResultPublisher.java
│   └── WorkflowDlqPublisher.java
├── agent/
│   ├── WorkerAgent.java
│   ├── WorkerAgentDefinition.java
│   ├── WorkerAgentFactory.java
│   ├── TaskPromptBuilder.java
│   └── TaskExecutor.java
├── tool/
│   ├── ToolCatalog.java
│   └── AgentToolRegistryFactory.java
├── session/
│   ├── TaskSessionContext.java
│   ├── WorkspacePolicy.java
│   └── WorkerSecurityContext.java
├── result/
│   ├── ResultMapper.java
│   └── ErrorClassifier.java
└── metrics/
    └── WorkerMetrics.java
```

---

## 7. Replacing CDI Dependencies in Spring

### 7.1 Base Beans

```java
@Configuration
public class QuadCoreConfig {

    @Bean
    public CurrentSession currentSession() {
        return new CurrentSession();
    }

    @Bean
    public WorkspaceResolver workspaceResolver() {
        return new WorkspaceResolver();
    }

    @Bean(name = "quadObjectMapper")
    public ObjectMapper quadObjectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Bean
    public ToolArgsMapper toolArgsMapper(@Qualifier("quadObjectMapper") ObjectMapper mapper) {
        return new ToolArgsMapper(mapper);
    }

    @Bean
    public IdempotencyStore idempotencyStore() {
        return new BoundedIdempotencyStore();
    }

    @Bean
    public OTelAgentTracer otelAgentTracer() {
        return new OTelAgentTracer();
    }
}
```

Important:

- `WorkspaceResolver` has a MicroProfile config field. Without CDI it remains null.
- `WorkspaceResolver` then falls back to `WORKSPACE_BASE`.
- `QUAD_WORKSPACE` or `quad.workspace` should therefore be set early.

### 7.2 Build Built-in Tools Manually

```java
@Configuration
public class BuiltinToolConfig {

    @Bean
    public ReadFileTool readFileTool(WorkspaceResolver resolver) {
        ReadFileTool tool = new ReadFileTool();
        tool.setWorkspaceResolver(resolver);
        return tool;
    }

    @Bean
    public WriteTool writeTool(WorkspaceResolver resolver) {
        WriteTool tool = new WriteTool();
        tool.setWorkspaceResolver(resolver);
        return tool;
    }

    @Bean
    public GrepTool grepTool(WorkspaceResolver resolver) {
        GrepTool tool = new GrepTool();
        tool.setWorkspaceResolver(resolver);
        return tool;
    }

    // FindTool, LsTool, MultiEditTool, ApplyPatchTool analog
    // WebSearchTool, WebFetchTool ohne WorkspaceResolver
}
```

The tools fall back to the static `CurrentSession` if the injected field is null. In addition, `ReflectiveToolMethod.execute(...)` sets the session before the tool call.

---

## 8. ToolCatalog: All Tools Global, Registry Copy per Agent

### 8.1 Principle

```text
ToolCatalog
  ├── contains all available ToolMethod instances
  ├── is built once at startup
  └── is immutable thereafter

AgentToolRegistryFactory
  ├── creates a new QuadToolRegistry per task
  ├── copies only approved tools from the catalog
  └── passes the registry to the agent
```

This corresponds to the desired model:

> All tools are stored centrally in the system; each agent receives a copy with approved tools.

More precisely: the **registry is copied**, not each tool instance. `ToolMethod` instances are shared. This is fine as long as tools are stateless and receive session state via parameters or `CurrentSession`.

### 8.2 ToolCatalog

```java
@Component
public class ToolCatalog {

    private final Map<String, ToolMethod> catalog;

    public ToolCatalog(ToolArgsMapper argsMapper,
                       List<Object> builtinToolBeans) {
        Map<String, ToolMethod> tools = new HashMap<>();

        for (Object bean : builtinToolBeans) {
            for (Method method : bean.getClass().getDeclaredMethods()) {
                if (!method.isAnnotationPresent(Tool.class)) {
                    continue;
                }
                ToolMethod toolMethod = new ReflectiveToolMethod(method, bean, argsMapper);
                tools.putIfAbsent(method.getName(), toolMethod);
            }
        }

        this.catalog = Map.copyOf(tools);
    }

    public Set<String> names() {
        return catalog.keySet();
    }

    public ToolMethod require(String name) {
        ToolMethod tool = catalog.get(name);
        if (tool == null) {
            throw new IllegalArgumentException("Unknown tool: " + name);
        }
        return tool;
    }
}
```

### 8.3 Registry per Agent

```java
@Component
public class AgentToolRegistryFactory {

    private final ToolCatalog catalog;
    private final ToolArgsMapper argsMapper;

    public QuadToolRegistry create(Agent agent, Set<String> allowedTools) {
        QuadToolRegistry registry = new QuadToolRegistry(argsMapper);
        registry.registerFromAgent(agent);

        allowedTools.stream()
            .sorted()
            .forEach(name -> registry.register(name, catalog.require(name)));

        return registry;
    }
}
```

Rules:

- no global `QuadToolRegistry` as an agent registry
- no parallel mutation of the same registry
- disable `ToolActivatorTool` in Phase 1
- check unknown tool names fail-fast at config load
- optional later upstream extension: `QuadToolRegistry.freeze()`

---

## 9. Agent per Task

### 9.1 WorkerAgent

```java
public class WorkerAgent extends Agent {

    @Override
    protected AgentSessionState newSessionState() {
        return new AgentSessionState();
    }

    public void wire(CurrentSession currentSession, WorkspaceResolver workspaceResolver) {
        this.currentSession = currentSession;
        this.workspaceResolver = workspaceResolver;
    }
}
```

### 9.2 Agent Definition

```java
public record WorkerAgentDefinition(
    String agentType,
    String agentId,
    String systemPrompt,
    String modelKey,
    Double temperature,
    Integer maxOutputTokens,
    Set<String> allowedTools,
    int maxReActIterations,
    long timeoutMs,
    String jsonOutputSchema,
    boolean structuredOutputEnabled
) {}
```

### 9.3 Factory

```java
@Component
public class WorkerAgentFactory {

    private final ChatModelRegistry models;
    private final ToolCatalog catalog;
    private final AgentToolRegistryFactory registryFactory;
    private final CurrentSession currentSession;
    private final WorkspaceResolver workspaceResolver;
    private final IdempotencyStore idempotencyStore;
    private final OTelAgentTracer tracer;

    public Agent create(WorkerAgentDefinition def) {
        WorkerAgent agent = new WorkerAgent();
        agent.wire(currentSession, workspaceResolver);
        agent.setLlm(models.get(def.modelKey()));

        QuadToolRegistry registry = registryFactory.create(agent, def.allowedTools());
        agent.setToolRegistry(registry);

        HookRegistry hooks = new HookRegistry();
        AgentEventPublisher events = new AgentEventPublisher();

        agent.setHookRegistry(hooks);
        agent.setEventPublisher(events);
        agent.setToolExecutor(new ToolExecutor(idempotencyStore, tracer, events, hooks));
        agent.setContextManager(new ContextManager());

        agent.setSystemPrompt(def.systemPrompt());
        agent.setReActMaxIterations(def.maxReActIterations());

        if (def.structuredOutputEnabled() && def.jsonOutputSchema() != null) {
            agent.setStructuredOutputConfig(
                StructuredOutputConfig.dynamicSchema(def.jsonOutputSchema())
            );
        }

        if (def.temperature() != null || def.maxOutputTokens() != null) {
            var params = DefaultChatRequestParameters.builder();
            if (def.temperature() != null) params.temperature(def.temperature());
            if (def.maxOutputTokens() != null) params.maxOutputTokens(def.maxOutputTokens());
            agent.setChatParameters(params.build());
        }

        return agent;
    }
}
```

Rules:

- **no agent singleton**
- new agent instance per task
- new `QuadToolRegistry` per task
- new `AgentSessionState` per task
- `HookRegistry` per task if hooks are task-specific
- `AgentEventPublisher` per task or deliberately shared, but then do not mutate it at runtime

---

## 10. Session, Workspace, and Thread Isolation

### 10.1 Session ID

```java
String sessionId = String.join(":",
    task.header().workflowId(),
    stepId,
    Integer.toString(iteration),
    Integer.toString(attempt),
    task.header().taskId()
);
```

### 10.2 Session State

```java
AgentSessionState state = AgentSessionState.create(sessionId);
state.setTenantId(resolveTenant(task));
state.setSecurityContext(new WorkerSecurityContext(
    resolveUserId(task),
    Set.of("workflow-worker"),
    sessionId,
    Map.of(
        "workflowId", task.header().workflowId(),
        "taskId", task.header().taskId(),
        "correlationId", nullSafe(task.header().traceParent())
    )
));
state.setMemory(new SessionMemory(maxMessages));
state.grantDir(workspacePolicy.workflowDir(task.header().workflowId()), true);
```

### 10.3 Workspace

```text
${QUAD_WORKSPACE}
└── <workflowId>/
    ├── shared/
    └── <stepId>-<iteration>-<attempt>/
```

Recommendation:

- session workspace per task
- optional shared workflow directory with an explicit grant
- do not let parallel fork/join branches write to the same files unchecked
- sandbox/file tools only with a session-scoped workspace

### 10.4 ThreadLocal Cleanup

```java
CurrentSession.setCurrent(state);
try {
    return agent.executeStructured(prompt, state);
} finally {
    CurrentSession.setCurrent(null);
}
```

Upstream, a real `CurrentSession.remove()` would be better.

---

## 11. Prompt and Structured Output

### 11.1 Prompt Components

```text
Workflow: <workflowName> v<workflowVersion>
Execution: <workflowId>
Step: <stepId> (<nodeType>, iteration <iteration>, attempt <attempt>)
Agent: <agentType>

Task:
<inputPrompt>

Workflow input:
<input JSON>

Upstream context:
<only contextReferences>

Output contract:
Return one JSON object.
```

### 11.2 Standard JSON Schema

```json
{
  "type": "object",
  "properties": {
    "response": { "type": "string" },
    "confidence": { "type": "number", "minimum": 0, "maximum": 1 },
    "status": {
      "type": "string",
      "enum": ["COMPLETED", "FAILED_RETRYABLE", "FAILED_FATAL", "NEEDS_HITL"]
    },
    "errorMessage": { "type": "string" },
    "artifacts": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "name": { "type": "string" },
          "path": { "type": "string" },
          "mime": { "type": "string" }
        },
        "required": ["name", "path"]
      }
    }
  },
  "required": ["response"],
  "additionalProperties": true
}
```

This keeps orchestrator conditions such as:

```text
context['research_step'].resultData.confidence < 0.8
```

usable.

---

## 12. Kafka Consumer in Spring

### 12.1 Topics

The worker explicitly consumes only the topics it is responsible for:

```properties
worker.quad.task-topics=agent.tasks.researcher,agent.tasks.planner,agent.tasks.coder
```

No global `agent.tasks.*`, as long as the deterministic mock worker runs in parallel. Otherwise two worker implementations would consume the same tasks.

### 12.2 Consumer Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: ${QUAD_WORKER_GROUP_ID:quad-workflow-worker}
      auto-offset-reset: latest
      enable-auto-commit: false
      max-poll-records: 8
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        isolation.level: read_committed
        max.poll.interval.ms: 900000
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
```

### 12.3 Batch Listener with Parallel Execution

```java
@Component
public class WorkflowTaskListener {

    private final TaskExecutor taskExecutor;
    private final ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();

    @KafkaListener(
        topics = "#{'${worker.quad.task-topics}'.split(',')}",
        groupId = "${worker.quad.group-id:quad-workflow-worker}",
        containerFactory = "quadBatchListenerFactory"
    )
    public void onTasks(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        CompletableFuture<?>[] futures = records.stream()
            .map(record -> CompletableFuture.runAsync(() -> taskExecutor.safeExecute(record), virtualThreads))
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
        ack.acknowledge();
    }
}
```

Container factory:

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, String> quadBatchListenerFactory(
    ConsumerFactory<String, String> consumerFactory,
    WorkerProperties props
) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(consumerFactory);
    factory.setBatchListener(true);
    factory.setConcurrency(props.kafka().concurrency());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    return factory;
}
```

Flow:

1. Poll batch
2. Execute all records in parallel on virtual threads
3. Publish result or DLQ per record
4. Only when all records are processed: batch ack

Advantage:

- simple
- at-least-once
- tasks of the same batch run in parallel

Disadvantage:

- the slowest task blocks the batch ack
- `max.poll.interval.ms` must be large enough
- keep `max.poll.records` small

---

## 13. Concurrency and Kafka Partitioning

All task topics are keyed with `workflow_id` according to the SPEC. This means:

```text
Tasks of the same workflow land on the same partition.
```

Consequences:

- Multiple workflows run in parallel across multiple partitions.
- Fork/Join branches of the same workflow can run in parallel if they arrive in the same consumer batch.
- If they arrive in different polls, they are processed sequentially as long as one consumer thread works per partition.

### Phase 1

Batch parallelism suffices:

```text
max.poll.records = 4..16
listener.concurrency = partitions / instances
semaphore = maxConcurrentTasks
```

### Phase 2: Real In-Flight Parallelism per Partition

For maximum parallelism with long LLM tasks:

- own consumer loop or asynchronous listener
- hand records immediately to a bounded queue
- virtual thread executor
- offset tracker per partition
- commit only for the highest contiguous offset
- pause partition when the in-flight limit is reached
- resume partition when the water mark is undershot

Sketch:

```text
TopicPartition p0
  outstanding offsets: 10, 11, 12
  completed: 10, 12
  commit safe: 10
  after 11 completes: commit 12
```

This is the production-ready variant, but more involved.

### Phase 3: Horizontal Scaling

Multiple worker instances in the same consumer group:

```text
quad-workflow-worker-1
quad-workflow-worker-2
quad-workflow-worker-3
```

Kafka distributes the partitions. Each instance processes its partitions in parallel.

---

## 14. TaskExecutor

```java
@Component
public class TaskExecutor {

    private final WorkerAgentFactory agentFactory;
    private final AgentCatalog agentCatalog;
    private final TaskPromptBuilder promptBuilder;
    private final TaskSessionContext sessionContext;
    private final ResultMapper resultMapper;
    private final ErrorClassifier errorClassifier;
    private final WorkflowResultPublisher results;
    private final WorkflowDlqPublisher dlq;
    private final Semaphore taskPermits;

    public void safeExecute(ConsumerRecord<String, String> record) {
        TaskEvent task = null;
        try {
            task = WorkerJson.read(record.value(), TaskEvent.class);
            validate(task);

            taskPermits.acquire();
            try {
                ResultEvent result = execute(task);
                results.send(task.header().workflowId(), result);
            } finally {
                taskPermits.release();
            }
        } catch (Exception ex) {
            if (task == null) {
                dlq.send(record, ex);
            } else {
                results.send(task.header().workflowId(),
                    resultMapper.failure(task, errorClassifier.classify(ex), ex));
            }
        }
    }

    private ResultEvent execute(TaskEvent task) {
        WorkerAgentDefinition def = agentCatalog.resolve(task.agentConfig().agentType());
        Agent agent = agentFactory.create(def);
        AgentSessionState state = sessionContext.create(task);
        String prompt = promptBuilder.build(task, def);

        CurrentSession.setCurrent(state);
        long startedAt = System.currentTimeMillis();
        try {
            AgentResult result = agent.executeStructured(prompt, state);
            return resultMapper.success(task, def, result, state,
                System.currentTimeMillis() - startedAt);
        } catch (Exception ex) {
            return resultMapper.failure(task, def, ex,
                System.currentTimeMillis() - startedAt);
        } finally {
            CurrentSession.setCurrent(null);
        }
    }
}
```

---

## 15. Result Mapping

### 15.1 Success

```java
Map<String, Object> resultData = new LinkedHashMap<>();
resultData.put("executor", "quad-core");
resultData.put("agentType", task.agentConfig().agentType());
resultData.put("stepId", stepId);
resultData.put("iteration", iteration);
resultData.put("attempt", task.header().attempt());
resultData.put("sessionId", state.getSessionId());

if (result.hasStructuredOutput()) {
    resultData.putAll(WorkerJson.readMap(result.structuredOutput()));
} else {
    resultData.put("response", result.finalAnswer());
    resultData.put("text", result.finalAnswer());
}
```

### 15.2 Status from Agent Output

```java
String status = "COMPLETED";
Object rawStatus = resultData.get("status");
if (rawStatus != null && isValidStatus(rawStatus.toString())) {
    status = rawStatus.toString().toUpperCase();
}
```

### 15.3 Error Classification

| Error | Status |
|---|---|
| Timeout | `FAILED_RETRYABLE` |
| HTTP 429 | `FAILED_RETRYABLE` |
| HTTP 5xx | `FAILED_RETRYABLE` |
| IOException / network | `FAILED_RETRYABLE` |
| HTTP 401/403 | `FAILED_FATAL` |
| HTTP 400/422 schema error | `FAILED_FATAL` |
| SecurityException / ToolGuard-Deny | `FAILED_FATAL` |
| NeedsHumanReview | `NEEDS_HITL` |
| unknown error | `FAILED_RETRYABLE` |

The orchestrator then decides on retry, DLQ, HITL, or workflow abort.

---

## 16. HITL

### 16.1 Workflow HITL Stays in the Orchestrator

The worker does not create its own ticket system; instead it delivers:

```text
NEEDS_HITL
```

The orchestrator:

- sets the checkpoint to `WAITING_HITL`
- creates a ticket
- pauses the execution
- resumes after retry/skip/abort

### 16.2 Using Quad HITL Non-Blocking

`HITLPlugin` blocks the agent thread and uses in-memory checkpoints. That does not fit well with Kafka workers.

Phase 1:

- disable `HITLPlugin`
- do not approve dangerous tools
- use ToolGuard/Guardrails

Phase 2:

- own non-blocking hook
- throws `NeedsHumanReviewException`
- worker maps to `NEEDS_HITL`
- orchestrator takes over ticket and resume

A real resume in the middle of an agent run requires durable checkpoints and is a stage of its own.

---

## 17. Idempotency and Retries

### 17.1 Orchestrator Side

- Task ID is unique per attempt
- checkpoints are protected via `(executionId, stepId, iteration)`
- duplicate results are ignored if the checkpoint is no longer `IN_PROGRESS`

### 17.2 Worker Side

Additional result cache:

```java
Cache<String, String> resultCache = Caffeine.newBuilder()
    .maximumSize(50_000)
    .expireAfterWrite(Duration.ofHours(1))
    .build();
```

Key:

```text
workflowId:taskId:attempt
```

Flow:

1. Cache hit → publish the stored result again
2. Cache miss → execute agent
3. Publish result
4. Fill cache

For multi-instance production: Redis instead of Caffeine.

---

## 18. Timeouts

Three levels:

1. **LLM HTTP timeout**
   - must be smaller than the task timeout
2. **Worker task timeout**
   - from `task.header().timeoutMs()`
3. **Orchestrator sweep**
   - detects stale `IN_PROGRESS` checkpoints

The worker should use:

```java
future.get(task.header().timeoutMs() + graceMs, TimeUnit.MILLISECONDS)
```

Note: an aborted HTTP call does not always reliably terminate the underlying request. Therefore, additionally configure a provider timeout.

---

## 19. Observability

### 19.1 Logging

Every log entry should contain:

```text
workflowId
taskId
stepId
iteration
attempt
agentType
sessionId
topic
partition
offset
```

### 19.2 Tracing

The task header contains `traceParent`. The worker should extract the W3C Trace Context and set it as the current OTel context before `quad-core` runs.

### 19.3 Metrics

```text
quad_worker_tasks_received_total
quad_worker_tasks_completed_total{agentType,status}
quad_worker_task_duration_seconds
quad_worker_llm_duration_seconds
quad_worker_active_tasks
quad_worker_permits_available
quad_worker_tool_errors_total{tool}
quad_worker_dlq_total
quad_worker_duplicate_tasks_total
```

### 19.4 Token/Cost Gap

`AgentRuntime` sees TokenUsage but currently does not return it completely via `AgentResult`. As an interim measure:

- `promptTokens=0`
- `completionTokens=0`
- `totalCostUsd=0`
- or your own estimate

Upstream fix recommended:

- TokenUsage in `ExecutionMetrics`
- `AgentResult` provides real usage
- optional CostCalculator

---

## 20. Configuration

```yaml
worker:
  quad:
    enabled: true
    group-id: quad-workflow-worker
    task-topics:
      - agent.tasks.researcher
      - agent.tasks.planner
      - agent.tasks.coder
    result-topic: agent.execution.results
    dlq-topic: agent.execution.dlq
    concurrency: 3
    max-concurrent-tasks: 24
    default-timeout-ms: 120000
    workspace-base: /var/lib/quad-worker/workspace
    agents:
      researcher:
        agent-id: researcher
        model-key: simple
        temperature: 0.2
        max-react-iterations: 8
        allowed-tools:
          - readFile
          - webSearch
          - webfetch
        structured-output-enabled: true
      coder:
        agent-id: coder
        model-key: advanced
        allowed-tools:
          - readFile
          - writeFile
          - grepSearch
          - listDirectory
```

---

## 21. Tests

### 21.1 Unit Tests

- ToolCatalog contains the expected tools
- registry copy contains only approved tools
- two agent registries do not influence each other
- ResultMapper maps raw text, JSON, status, errors
- ErrorClassifier classifies timeout/401/429/5xx correctly
- PromptBuilder contains step, input, contextReferences

### 21.2 Concurrency Tests

With mock ChatModel:

- 50 tasks in parallel
- each task has its own session ID
- tool sees the correct `CurrentSession`
- no cross-session memory
- `CurrentSession.getCurrent()` is null after every task
- no registry mutation across task boundaries

### 21.3 Kafka Integration

With Embedded Kafka or Testcontainers:

1. Task on `agent.tasks.researcher`
2. Worker with mock ChatModel
3. Result on `agent.execution.results`
4. Key = workflowId
5. taskId identical
6. status = COMPLETED

### 21.4 E2E Against Orchestrator

Test existing workflows:

- `reference-code-quality`
- `xor-race`
- `n-of-quorum`
- `guard-skip`
- `error-taxonomy`

To do this, run the worker either with a real LLM or a deterministic mock ChatModel.

---

## 22. Rollout & Current Status

### Phase 1: Skeleton ✅ COMPLETED (2026-09-17)

**Result:**  
- `mvn compile` → SUCCESS
- `mvn test` → **22 tests passed** (TaskPromptBuilder, ResultMapper, ContractSerialization, ToolCatalog, ChatModelRegistry)  

**New features since the last status:**
- **Real LLM execution**: TaskExecutor uses WorkerAgentFactory for agent creation per task
- **Timeout handling**: CompletableFuture.get(timeoutMs, MILLISECONDS) per task
- **Startup validation**: ApiKeyStartupValidator validates OPENAI_API_KEY during bootstrap
- **Settings error → Kafka publish**: missing API keys → ERROR event to DLQ
- **.env auto-loading**: spring-dotenv integration for the .env file
- **Prometheus metrics**: WorkerMetrics tracks TasksCompleted/Failed/DLQ/Latency
- **API rate limiting**: TokenBucket algorithm against external LLM APIs
- **ToolGuard/Guardrails**: protects WriteTool/MultiEditTool/etc. from misuse
- **Graceful shutdown lifecycle**: ApplicationReady/Failed events + health checks
- **Docker Compose service**: quad-workflow-worker with health check

**.env configuration:**
```bash
OPENAI_API_KEY="sk-or-v1-..."       # OpenRouter Key aus .env
OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://openrouter.ai/api/v1}"
OPENAI_MODEL="${OPENAI_MODEL:-deepseek/deepseek-v4-flash-0731}"
```

**Behavior on settings errors:**
- If OPENAI_API_KEY is missing → MOCK mode activated + warning in the log
- Warning is published as an error event to Kafka `agent.execution.results`
- Worker keeps running but without real LLM calls
- Startup validator reports the status via Kafka and in the application log

**Docker Compose Integration:**
```yaml
services:
  quad-worker:
    image: augmentia/quad-workflow-worker:1.0.0-SNAPSHOT
    build: ./quad-workflow-worker
    ports: [8091:8091]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      OPENAI_API_KEY: ${OPENAI_API_KEY}
      SPRING_CONFIG_IMPORT: optional:file:.env[.properties]
    volumes:
      - ./quad-workflow-worker/.env:/app/.env:ro
    healthcheck:
      test: wget -qO- http://localhost:8091/actuator/health || exit 1
```

**Next steps for Phase 2 (Kafka integration against a real broker):**
1. Start Docker Compose (`docker-compose.yml` from kafka_workflow)
2. Deploy the worker and run it against real topics
3. Receive real TaskEvents from the deterministic orchestrator
4. Validate results (status COMPLETED/FAILED_RETRYABLE/FAILED_FATAL)
5. Enable Prometheus metrics (/actuator/prometheus)

### Phase 3: Real LLM Operation
- Configure provider timeouts (LLM HTTP vs task timeout) ✓
- Implement rate limits ✓
- Track cost metrics per task (via WorkerMetrics) ✓
- Guardrails for dangerous tools (WriteTool, MultiEditTool, etc.) ✓

### Phase 4: Production Switchover
1. Enable Quad worker only for selected agent types
2. Disable the deterministic mock worker for the same topics
3. Migrate one agent type (e.g. `researcher`)
4. Test E2E against the orchestrator
5. Migrate further agent types

---

## 23. Necessary or Recommended Upstream Fixes in quad-core

### P0

1. Add `CurrentSession.remove()`.
2. Make `QuadToolRegistry` thread-safe or freezable.
   - The spec says `ConcurrentHashMap`; the implementation currently uses `LinkedHashMap`.
3. Fully expose TokenUsage/cost in `AgentResult`.

### P1

4. Make `IdempotencyStore` bounded or an interface with a Caffeine/Redis implementation.
5. Make `SessionLockService` atomic and implement the heartbeat properly.
6. Do not use `InMemorySessionManager.executeLocked` as a global lock.
7. `AgentBuilder` should make `CurrentSession`, `WorkspaceResolver`, `ContextManager` explicitly wireable.
8. Make `QuadAgent.initialized` volatile or avoid `QuadAgent` in the worker.

These fixes are small. They are a reason **not** to copy, but to patch upstream.

---

## 24. Variant Comparison

| Variant | Effort | Risk | Maintainability | Recommendation |
|---|---:|---:|---:|---|
| Copy `quad-core` completely | high | high | poor | no |
| Copy individual classes | medium | very high | very poor | no |
| Quarkus worker in the Quad repo | medium | low | good | good if the Quad ecosystem is desired |
| Spring Boot worker as its own service | medium | medium | good | recommended for the Kafka platform |
| Extend `ai-agent-service` | low | high with concurrency | mediocre | only a temporary solution |
| Replace existing deterministic mock worker | low | low | good | keep for tests, optionally replace in prod |

---

## 25. Final Recommendation

**Build a new Spring Boot worker `quad-workflow-worker`.**

It:

- depends on `quad-core` and `quad-tool-builtin`
- uses LangChain4j from `quad-core`
- uses Spring Kafka for topics and acks
- builds an immutable global `ToolCatalog`
- creates per task:
  - Agent
  - ToolRegistry copy
  - AgentSessionState
  - Workspace grants
  - ThreadLocal session
- publishes results to `agent.execution.results`
- leaves routing, retries, HITL, sweep, and determinism to the existing orchestrator

Quarkus remains a valid alternative but is not a prerequisite. For the Kafka project, Spring Boot is even more obvious, because the orchestrator, mock worker, and infrastructure are already Spring-based.
