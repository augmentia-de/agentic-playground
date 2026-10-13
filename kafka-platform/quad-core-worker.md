# quad-core-worker: Implementationsplan & Fortschritt

## ⚡ Status: PHASE 1 - Skeleton COMPLETED ✅

**Datum:** 2026-09-17  
**Projekt:** `/home/torsten/dev/my-projects/kafka_workflow/quad-workflow-worker`  
**Build:** `mvn compile` → SUCCESS | Tests: `mvn test` → 22/22 PASSED

### Was implementiert wurde (Phase 1)

| Komponente | Status | Datei(en) |
|---|---|---|
| Maven POM (Boot 4 + Quad deps) | ✅ Done | `pom.xml` |
| Contract-Klassen (TaskEvent, ResultEvent, WorkerJson) | ✅ Done | `contract/*.java` |
| Config: WorkerProperties, KafkaConfig, ChatModelRegistry | ✅ Done | `config/*.java` |
| QuadCoreConfig (CurrentSession, WorkspaceResolver, ObjectMapper) | ✅ Done | `config/QuadCoreConfig.java` |
| ToolCatalog (global immutable) | ✅ Done | `tool/ToolCatalog.java` |
| AgentToolRegistryFactory (per-task Registry-Kopie) | ✅ Done | `tool/AgentToolRegistryFactory.java` |
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
| **Echter OpenAI/OpenRouter ChatModel** | ✅ Done | `QuadWorkerConfig.chatModelRegistry()` |
| **API Key aus .env geladen** | ✅ Done | `.env`: OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL |

### LLM Configuration

**.env Datei:** `/home/torsten/dev/my-projects/kafka_workflow/quad-workflow-worker/.env`

```bash
OPENAI_API_KEY="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"
OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://openrouter.ai/api/v1}"
OPENAI_MODEL="${OPENAI_MODEL:-deepseek/deepseek-v4-flash-0731}"
```

**Verhalten:**
- Wenn API key gesetzt → echter OpenAiChatModel über OpenRouter (DeepSeek V4 Flash)
- Fallback auf Mock wenn keine API keys gefunden werden (warn log)
- Custom headers für OpenRouter (`HTTP-Referer`, `X-Title`)

### Bisherige Architektur im Code

```text
quad-workflow-worker/
├── pom.xml                      (Boot 4.0.8 + quad-core 1.0.0-SNAPSHOT)
├── Dockerfile
├── src/main/java/de/augmentia/quad/worker/
│   ├── QuadWorkflowWorkerApplication.java
│   ├── config/
│   │   ├── WorkerProperties.java          (record mit agent configs)
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
│   │   ├── WorkerAgent.java               (extends Agent, wire() ohne CDI)
│   │   ├── WorkerAgentDefinition.java     (record)
│   │   ├── WorkerAgentFactory.java        (creates per-task Agent)
│   │   ├── TaskPromptBuilder.java         (Workflow+Step+Input+Context)
│   │   └── TaskExecutor.java              (safeExecute + cache + semaphore)
│   ├── tool/
│   │   ├── ToolCatalog.java               (immutable global catalog)
│   │   └── AgentToolRegistryFactory.java  (per-task registry copy)
│   ├── session/
│   │   ├── TaskSessionContext.java        (AgentSessionState pro task)
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

**Spring Boot mit LangChain4j aus `quad-core` geht.** Quarkus ist keine harte Voraussetzung.

Empfehlung:

- **Kein Kopieren von `quad-core`.**
- **Kein einzelnes Herauskopieren von Klassen.**
- **Neuer, schlanker Worker-Service**, der `quad-core` als Maven-Dependency nutzt.
- Für das bestehende Kafka-Projekt ist ein **Spring-Boot-Worker** sinnvoll, wenn die Plattform weiterhin im Spring-Stack betrieben werden soll.
- Ein **Quarkus-Worker** ist sinnvoll, wenn der Worker im `nooa/quad`-Ökosystem bleibt und CDI/Quarkus-Features nativ genutzt werden sollen.

Der bestehende `agent-orchestrator` bleibt die einzige Routing-Autorität. Der Quad-Worker ist ausschließlich ein Agent-Executor über Kafka.

---

## 1. Warum ursprünglich Quarkus?

Quarkus wurde als Option empfohlen, weil:

1. `quad-core` CDI-Annotationen nutzt:
   - `@ApplicationScoped`
   - `@Inject`
   - `jakarta.enterprise.inject.Instance`
2. `quad-tool-builtin` seine Tools über CDI-Provider bereitstellt.
3. `quad-quarkus` bereits Agent-Factories, Kafka-Channels, Workflow-Executor, SSE, Persistence und Virtual-Thread-Muster enthält.
4. Quarkus + LangChain4j im Quad-Projekt schon zusammengedacht sind.

Aber:

- LangChain4j ist framework-agnostic.
- `quad-core` ist auch plain Java nutzbar; die Tests instanziieren Agents ohne laufenden CDI-Container.
- Spring Kafka kann denselben Kafka-Contract konsumieren.
- Der bestehende `KafkaInboundChannel` aus `quad-quarkus` ist für einen zuverlässigen Workflow-Worker ohnehin nicht direkt geeignet, weil er `enable.auto.commit=true` nutzt.

Deshalb: **Quarkus ist bequem, aber nicht zwingend.**

---

## 2. Spring-Variante: Ja, mit klaren Regeln

Ein Spring-Boot-Worker kann `quad-core` verwenden, wenn man CDI nicht als Runtime voraussetzt.

### Was in Spring funktioniert

- `Agent` subclassen und plain instanziieren
- `AgentSessionState` pro Task erzeugen
- `CurrentSession` als ThreadLocal nutzen
- `QuadToolRegistry` pro Agent/Task erzeugen
- `ToolExecutor`, `HookRegistry`, `AgentEventPublisher` manuell verdrahten
- `ReflectiveToolMethod` für Built-in-Tools verwenden
- LangChain4j `ChatModel` als Spring-Bean bauen
- Spring Kafka für Task-Input und Result-Output nutzen

### Was in Spring nicht unverändert funktioniert

- `StandardToolProvider` basiert auf CDI-`Instance<BuiltInToolProvider>`
- `BuiltinToolsProvider` erwartet CDI-Injection
- `WorkspaceResolver` nutzt MicroProfile `@ConfigProperty`
- `QuadToolRegistry` ist als `@ApplicationScoped` gedacht, darf im Worker aber nicht als globale mutable Singleton-Registry verwendet werden
- `ToolActivatorTool` mutiert eine Registry und ist nur mit per-task Registry sicher

Lösung: **eigenen Spring-ToolCatalog bauen** und die Built-in-Tools manuell registrieren.

---

## 3. Repo-/Modul-Entscheidung

### Empfehlung

Neues eigenständiges Projekt:

```text
quad-workflow-worker/
```

Mögliche Orte:

1. **Bevorzugt für Quad-Nähe:**
   ```text
   /home/torsten/dev/my-projects/nooa/quad/quad-workflow-worker
   ```

2. **Bevorzugt für Kafka-Plattform-Nähe:**
   ```text
   /home/torsten/dev/my-projects/quad-workflow-worker
   ```

3. **Nicht empfohlen:** als festes Kindmodul im bestehenden `agent-platform`-Parent, weil das deterministische Kafka-Projekt bewusst LLM-frei bleiben soll und `quad-core` LangChain4j/OpenAI-Abhängigkeiten mitbringt.

Der Worker kann trotzdem im `docker-compose.yml` des Kafka-Projekts als zusätzlicher Service betrieben werden.

---

## 4. Zielarchitektur

```text
agent-orchestrator
  ├── DAG / Checkpoints / Routing / HITL / Retries / Sweep
  ├── publiziert agent.tasks.<agentType>
  └── konsumiert agent.execution.results

quad-workflow-worker
  ├── konsumiert agent.tasks.<agentType>
  ├── parsed TaskEvent
  ├── resolved WorkerAgentDefinition
  ├── erzeugt ToolRegistry-Kopie
  ├── erzeugt Agent pro Task
  ├── erzeugt AgentSessionState pro Task
  ├── bindet CurrentSession pro Thread
  ├── führt quad-core ReAct-Run aus
  ├── mapped AgentResult auf ResultEvent
  └── publiziert agent.execution.results
```

Wichtig:

- Worker schreibt **nicht** in die Orchestrator-Datenbank.
- Worker trifft **kein Routing**.
- Worker entscheidet nur, wie ein einzelner Task ausgeführt wird.
- Orchestrator bleibt Single Writer.

---

## 5. Maven-Setup

### 5.1 Quad-Artefakte installieren

```bash
cd /home/torsten/dev/my-projects/nooa/quad

mvn -q -DskipTests install \
  -pl quad-core,quad-tool-builtin \
  -am
```

Optional, wenn Sandbox/MCP gebraucht werden:

```bash
mvn -q -DskipTests install \
  -pl quad-core,quad-tool-builtin,quad-sandbox-docker,quad-tool-mcp \
  -am
```

Für CI/Team sollten die Artefakte nach Nexus/GitHub Packages deployt werden.

### 5.2 Spring-Boot-Worker-POM

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

Hinweis:

- `quad-core` bringt LangChain4j transitiv mit.
- Jackson 2 und Jackson 3 können koexistieren, weil sie unterschiedliche Paketnamen haben:
  - Jackson 2: `com.fasterxml.jackson`
  - Jackson 3: `tools.jackson`
- Der Worker sollte für Quad-Verträge Jackson 2 verwenden, passend zu `quad-core`.

---

## 6. Paketstruktur

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

## 7. CDI-Abhängigkeiten in Spring ersetzen

### 7.1 Basis-Beans

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

Wichtig:

- `WorkspaceResolver` hat ein MicroProfile-Config-Feld. Ohne CDI bleibt es null.
- `WorkspaceResolver` fällt dann auf `WORKSPACE_BASE` zurück.
- `QUAD_WORKSPACE` oder `quad.workspace` sollte deshalb früh gesetzt werden.

### 7.2 Built-in-Tools manuell bauen

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

Die Tools haben einen Fallback auf die statische `CurrentSession`, wenn das injizierte Feld null ist. Zusätzlich setzt `ReflectiveToolMethod.execute(...)` die Session vor dem Tool-Aufruf.

---

## 8. ToolCatalog: alle Tools global, Registry-Kopie pro Agent

### 8.1 Prinzip

```text
ToolCatalog
  ├── enthält alle verfügbaren ToolMethod-Instanzen
  ├── wird einmal beim Startup gebaut
  └── ist danach immutable

AgentToolRegistryFactory
  ├── erzeugt pro Task eine neue QuadToolRegistry
  ├── kopiert nur freigegebene Tools aus dem Catalog
  └── übergibt die Registry an den Agent
```

Das entspricht dem gewünschten Modell:

> Alle Tools liegen zentral im System; jeder Agent bekommt eine Kopie mit freigegebenen Tools.

Genauer: Es wird die **Registry kopiert**, nicht jede Tool-Instanz. `ToolMethod`-Instanzen werden geteilt. Das ist okay, solange Tools zustandslos sind und Session-State per Parameter oder `CurrentSession` erhalten.

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

### 8.3 Registry pro Agent

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

Regeln:

- keine globale `QuadToolRegistry` als Agent-Registry
- keine parallele Mutation derselben Registry
- `ToolActivatorTool` in Phase 1 deaktivieren
- unbekannte Tool-Namen beim Config-Load fail-fast prüfen
- optional spätere Upstream-Erweiterung: `QuadToolRegistry.freeze()`

---

## 9. Agent pro Task

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

### 9.2 Agent-Definition

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

Regeln:

- **kein Agent-Singleton**
- pro Task neue Agent-Instanz
- pro Task neue `QuadToolRegistry`
- pro Task neue `AgentSessionState`
- `HookRegistry` pro Task, wenn Hooks task-spezifisch sind
- `AgentEventPublisher` pro Task oder bewusst shared, dann aber nicht zur Laufzeit mutieren

---

## 10. Session, Workspace und Thread-Isolation

### 10.1 Session-ID

```java
String sessionId = String.join(":",
    task.header().workflowId(),
    stepId,
    Integer.toString(iteration),
    Integer.toString(attempt),
    task.header().taskId()
);
```

### 10.2 Session-State

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

Empfehlung:

- Session-Workspace pro Task
- optional shared Workflow-Verzeichnis mit explizitem Grant
- parallele Fork/Join-Branches nicht ungehindert in dieselben Dateien schreiben lassen
- Sandbox/Datei-Tools nur mit session-scoped Workspace

### 10.4 ThreadLocal-Cleanup

```java
CurrentSession.setCurrent(state);
try {
    return agent.executeStructured(prompt, state);
} finally {
    CurrentSession.setCurrent(null);
}
```

Upstream wäre ein echtes `CurrentSession.remove()` besser.

---

## 11. Prompt und Structured Output

### 11.1 Prompt-Bestandteile

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
<nur contextReferences>

Output contract:
Return one JSON object.
```

### 11.2 Standard-JSON-Schema

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

Damit bleiben Orchestrator-Bedingungen wie:

```text
context['research_step'].resultData.confidence < 0.8
```

weiterhin nutzbar.

---

## 12. Kafka-Consumer in Spring

### 12.1 Topics

Der Worker konsumiert explizit nur die Topics, für die er zuständig ist:

```properties
worker.quad.task-topics=agent.tasks.researcher,agent.tasks.planner,agent.tasks.coder
```

Kein globales `agent.tasks.*`, solange der deterministische Mock-Worker parallel läuft. Sonst konsumieren zwei Worker-Implementierungen dieselben Tasks.

### 12.2 Consumer-Konfiguration

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

### 12.3 Batch-Listener mit paralleler Ausführung

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

Container-Factory:

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

Ablauf:

1. Batch pollen
2. alle Records parallel auf Virtual Threads ausführen
3. pro Record Result oder DLQ publizieren
4. erst wenn alle Records verarbeitet sind: Batch ack

Vorteil:

- einfach
- at-least-once
- Tasks desselben Batches laufen parallel

Nachteil:

- der langsamste Task blockiert den Batch-Ack
- `max.poll.interval.ms` muss groß genug sein
- `max.poll.records` klein halten

---

## 13. Parallelität und Kafka-Partitionierung

Alle Task-Topics sind laut SPEC mit `workflow_id` keyed. Das bedeutet:

```text
Tasks desselben Workflows landen auf derselben Partition.
```

Folgen:

- Mehrere Workflows laufen parallel über mehrere Partitionen.
- Fork/Join-Branches desselben Workflows können parallel laufen, wenn sie im selben Consumer-Batch ankommen.
- Wenn sie in unterschiedlichen Polls ankommen, werden sie sequenziell verarbeitet, solange ein Consumer-Thread pro Partition arbeitet.

### Phase 1

Batch-Parallelität reicht:

```text
max.poll.records = 4..16
listener.concurrency = partitions / instances
semaphore = maxConcurrentTasks
```

### Phase 2: echte in-flight Parallelität pro Partition

Für maximale Parallelität bei langen LLM-Tasks:

- eigener Consumer-Loop oder asynchroner Listener
- Records sofort in bounded Queue übergeben
- Virtual-Thread-Executor
- Offset-Tracker pro Partition
- Commit nur für höchsten zusammenhängenden Offset
- Partition pausieren, wenn in-flight Limit erreicht
- Partition resume, wenn Wasserstand unterschritten

Skizze:

```text
TopicPartition p0
  outstanding offsets: 10, 11, 12
  completed: 10, 12
  commit safe: 10
  after 11 completes: commit 12
```

Das ist die produktionsreife Variante, aber aufwendiger.

### Phase 3: horizontale Skalierung

Mehrere Worker-Instanzen in derselben Consumer-Group:

```text
quad-workflow-worker-1
quad-workflow-worker-2
quad-workflow-worker-3
```

Kafka verteilt die Partitionen. Jede Instanz verarbeitet ihre Partitionen parallel.

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

## 15. Result-Mapping

### 15.1 Erfolg

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

### 15.2 Status aus Agent-Output

```java
String status = "COMPLETED";
Object rawStatus = resultData.get("status");
if (rawStatus != null && isValidStatus(rawStatus.toString())) {
    status = rawStatus.toString().toUpperCase();
}
```

### 15.3 Fehlerklassifizierung

| Fehler | Status |
|---|---|
| Timeout | `FAILED_RETRYABLE` |
| HTTP 429 | `FAILED_RETRYABLE` |
| HTTP 5xx | `FAILED_RETRYABLE` |
| IOException / Netzwerk | `FAILED_RETRYABLE` |
| HTTP 401/403 | `FAILED_FATAL` |
| HTTP 400/422 Schemafehler | `FAILED_FATAL` |
| SecurityException / ToolGuard-Deny | `FAILED_FATAL` |
| NeedsHumanReview | `NEEDS_HITL` |
| unbekannter Fehler | `FAILED_RETRYABLE` |

Der Orchestrator entscheidet dann über Retry, DLQ, HITL oder Workflow-Abbruch.

---

## 16. HITL

### 16.1 Workflow-HITL bleibt im Orchestrator

Der Worker erzeugt kein eigenes Ticket-System, sondern liefert:

```text
NEEDS_HITL
```

Der Orchestrator:

- setzt den Checkpoint auf `WAITING_HITL`
- erzeugt ein Ticket
- pausiert die Execution
- resumed nach Retry/Skip/Abort

### 16.2 Quad-HITL nicht blockierend nutzen

`HITLPlugin` blockiert den Agent-Thread und nutzt in-memory Checkpoints. Das passt nicht gut zu Kafka-Workern.

Phase 1:

- `HITLPlugin` deaktivieren
- gefährliche Tools nicht freigeben
- ToolGuard/Guardrails nutzen

Phase 2:

- eigener non-blocking Hook
- wirft `NeedsHumanReviewException`
- Worker mapped auf `NEEDS_HITL`
- Orchestrator übernimmt Ticket und Resume

Ein echter Resume mitten im Agent-Run erfordert durable Checkpoints und ist eine eigene Ausbaustufe.

---

## 17. Idempotenz und Retries

### 17.1 Orchestrator-Seite

- Task-ID ist pro Attempt eindeutig
- Checkpoints sind über `(executionId, stepId, iteration)` geschützt
- doppelte Results werden ignoriert, wenn der Checkpoint nicht mehr `IN_PROGRESS` ist

### 17.2 Worker-Seite

Zusätzlicher Result-Cache:

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

Ablauf:

1. Cache-Hit → gespeichertes Result erneut publizieren
2. Cache-Miss → Agent ausführen
3. Result publizieren
4. Cache füllen

Für Multi-Instanz-Produktion: Redis statt Caffeine.

---

## 18. Timeouts

Drei Ebenen:

1. **LLM-HTTP-Timeout**
   - muss kleiner als Task-Timeout sein
2. **Worker-Task-Timeout**
   - aus `task.header().timeoutMs()`
3. **Orchestrator-Sweep**
   - erkennt stale `IN_PROGRESS`-Checkpoints

Worker sollte:

```java
future.get(task.header().timeoutMs() + graceMs, TimeUnit.MILLISECONDS)
```

nutzen. Achtung: Ein abgebrochener HTTP-Call beendet nicht immer garantiert den underlying Request. Deshalb zusätzlich Provider-Timeout konfigurieren.

---

## 19. Observability

### 19.1 Logging

Jeder Log-Eintrag sollte enthalten:

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

Task-Header enthält `traceParent`. Worker soll W3C Trace Context extrahieren und als aktuellen OTel-Context setzen, bevor `quad-core` läuft.

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

### 19.4 Token/Cost-Lücke

`AgentRuntime` sieht TokenUsage, gibt sie aber aktuell nicht vollständig über `AgentResult` zurück. Übergangsweise:

- `promptTokens=0`
- `completionTokens=0`
- `totalCostUsd=0`
- oder eigene Schätzung

Upstream-Fix empfohlen:

- TokenUsage in `ExecutionMetrics`
- `AgentResult` liefert echte Usage
- optional CostCalculator

---

## 20. Konfiguration

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

### 21.1 Unit-Tests

- ToolCatalog enthält erwartete Tools
- Registry-Kopie enthält nur freigegebene Tools
- zwei Agent-Registries beeinflussen sich nicht
- ResultMapper mappt Raw-Text, JSON, Status, Fehler
- ErrorClassifier klassifiziert Timeout/401/429/5xx korrekt
- PromptBuilder enthält Step, Input, ContextReferences

### 21.2 Concurrency-Tests

Mit Mock-ChatModel:

- 50 Tasks parallel
- jeder Task eigene Session-ID
- Tool sieht korrekte `CurrentSession`
- kein Cross-Session-Memory
- `CurrentSession.getCurrent()` ist nach jedem Task null
- keine Registry-Mutation über Task-Grenzen

### 21.3 Kafka-Integration

Mit Embedded Kafka oder Testcontainers:

1. Task auf `agent.tasks.researcher`
2. Worker mit Mock-ChatModel
3. Result auf `agent.execution.results`
4. Key = workflowId
5. taskId identisch
6. status = COMPLETED

### 21.4 E2E gegen Orchestrator

Bestehende Workflows testen:

- `reference-code-quality`
- `xor-race`
- `n-of-quorum`
- `guard-skip`
- `error-taxonomy`

Dafür Worker entweder mit echtem LLM oder deterministischem Mock-ChatModel betreiben.

---

## 22. Rollout & Aktueller Stand

### Phase 1: Skeleton ✅ COMPLETED (2026-09-17)

**Ergebnis:**  
- `mvn compile` → SUCCESS
- `mvn test` → **22 Tests bestanden** (TaskPromptBuilder, ResultMapper, ContractSerialization, ToolCatalog, ChatModelRegistry)  

**Neue Features seit letztem Stand:**
- **Echte LLM-Execution**: TaskExecutor nutzt WorkerAgentFactory für Agent-Erzeugung pro Task
- **Timeout-Handling**: CompletableFuture.get(timeoutMs, MILLISECONDS) pro Task
- **Startup Validation**: ApiKeyStartupValidator validiert OPENAI_API_KEY beim Bootstrapping
- **Settings-Fehler → Kafka Publish**: Fehlende API Keys → ERROR Event an DLQ
- **.env auto-loading**: spring-dotenv Integration für .env Datei
- **Prometheus Metrics**: WorkerMetrics trackt TasksCompleted/Failed/DLQ/Latency
- **API Rate Limiting**: TokenBucket Algorithmus gegen externe LLM APIs
- **ToolGuard/Guardrails**: Schützt WriteTool/MultiEditTool/etc vor Missbrauch
- **Graceful Shutdown Lifecycle**: ApplicationReady/Failed Events + Health Checks
- **Docker Compose Service**: quad-workflow-worker mit HealthCheck

**.env Konfiguration:**
```bash
OPENAI_API_KEY="sk-or-v1-..."       # OpenRouter Key aus .env
OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://openrouter.ai/api/v1}"
OPENAI_MODEL="${OPENAI_MODEL:-deepseek/deepseek-v4-flash-0731}"
```

**Verhalten bei Settings-Fehlern:**
- Wenn OPENAI_API_KEY fehlt → MOCK-Modus aktiviert + Warnung ins Log
- Warnung wird als Error-Event an Kafka `agent.execution.results` publiziert
- Worker läuft weiter aber ohne echte LLM-Calls
- Startup Validator meldet den Status via Kafka und im Application Log

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

**Nächste Schritte für Phase 2 (Kafka-Integration gegen echten Broker):**
1. Docker Compose starten (`docker-compose.yml` aus kafka_workflow)
2. Worker deployen und gegen echte Topics laufen lassen
3. Echte TaskEvents vom deterministischen Orchestrator empfangen
4. Ergebnisse validieren (status COMPLETED/FAILED_RETRYABLE/FAILED_FATAL)
5. Prometheus Metrics aktivieren (/actuator/prometheus)

### Phase 3: Echter LLM-Betrieb
- Provider-Timeouts konfigurieren (LLM HTTP vs Task Timeout) ✓
- Rate-Limits implementieren ✓
- Cost-Metrics pro Task tracken (über WorkerMetrics) ✓
- Guardrails für gefährliche Tools (WriteTool, MultiEditTool etc.) ✓

### Phase 4: Produktiver Wechsel
1. Quad-Worker nur für ausgewählte Agent-Typen aktivieren
2. Deterministischen Mock-Worker für dieselben Topics deaktivieren
3. Ein Agent-Typ migrieren (z.B. `researcher`)
4. E2E gegen Orchestrator testen
5. Weitere Agent-Typen migrieren

---

## 23. Notwendige oder empfohlene Upstream-Fixes in quad-core

### P0

1. `CurrentSession.remove()` ergänzen.
2. `QuadToolRegistry` thread-safe oder freezbar machen.
   - Spec sagt `ConcurrentHashMap`, Implementierung nutzt aktuell `LinkedHashMap`.
3. TokenUsage/Cost in `AgentResult` vollständig exponieren.

### P1

4. `IdempotencyStore` bounded oder als Interface mit Caffeine/Redis-Implementierung.
5. `SessionLockService` atomar machen und Heartbeat echt implementieren.
6. `InMemorySessionManager.executeLocked` nicht als globalen Lock verwenden.
7. `AgentBuilder` sollte `CurrentSession`, `WorkspaceResolver`, `ContextManager` explizit wiring-fähig machen.
8. `QuadAgent.initialized` volatile machen oder `QuadAgent` im Worker vermeiden.

Diese Fixes sind klein. Sie sind ein Grund, **nicht** zu kopieren, sondern upstream zu patchen.

---

## 24. Variantenvergleich

| Variante | Aufwand | Risiko | Wartbarkeit | Empfehlung |
|---|---:|---:|---:|---|
| `quad-core` komplett kopieren | hoch | hoch | schlecht | nein |
| Einzelne Klassen kopieren | mittel | sehr hoch | sehr schlecht | nein |
| Quarkus-Worker im Quad-Repo | mittel | niedrig | gut | gut, wenn Quad-Ökosystem gewünscht |
| Spring-Boot-Worker als eigener Service | mittel | mittel | gut | empfohlen für Kafka-Plattform |
| `ai-agent-service` erweitern | niedrig | hoch bei Parallelität | mittelmäßig | nur Übergangslösung |
| Bestehenden deterministischen Mock-Worker ersetzen | niedrig | niedrig | gut | für Tests behalten, prod optional ersetzen |

---

## 25. Finale Empfehlung

**Baue einen neuen Spring-Boot-Worker `quad-workflow-worker`.**

Er:

- hängt an `quad-core` und `quad-tool-builtin`
- nutzt LangChain4j aus `quad-core`
- verwendet Spring Kafka für Topics und Acks
- baut einen immutable globalen `ToolCatalog`
- erzeugt pro Task:
  - Agent
  - ToolRegistry-Kopie
  - AgentSessionState
  - Workspace-Grants
  - ThreadLocal-Session
- publiziert Ergebnisse nach `agent.execution.results`
- überlässt Routing, Retries, HITL, Sweep und Determinismus dem bestehenden Orchestrator

Quarkus bleibt eine valide Alternative, ist aber keine Voraussetzung. Für das Kafka-Projekt ist Spring Boot sogar naheliegender, weil Orchestrator, Mock-Worker und Infrastruktur bereits Spring-basiert sind.
