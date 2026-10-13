package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.common.event.WorkflowStartEvent;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcCheckpointStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcDlqStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcHitlTicketStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowDefinitionStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import de.augmentia.agentplatform.orchestrator.metrics.OrchestratorMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test infrastructure for the characterisation suite of {@link WorkflowEngine}.
 *
 * <p>Deliberate decision: <b>real Postgres, real transactions</b>. The engine relies on
 * {@code SELECT ... FOR UPDATE} ({@code executions.lock}) and on
 * {@code publishAfterCommit}. Both behave fundamentally differently with mock repos — a
 * refactor against mocks would not check exactly the semantics that are meant to be
 * pinned down here.
 *
 * <p>Only the Kafka publisher is a recording fake. That additionally makes it possible to
 * observe <em>what</em> was dispatched — topic and taskId — which is what makes the routing
 * character of the engine testable at all.
 *
 * <p>The context is deliberately minimal: no Spring Boot autoconfiguration, no Kafka
 * broker, no seeding. Only the bean graph the engine needs.
 */
@SpringJUnitConfig
@ActiveProfiles("test")
@ContextConfiguration(classes = WorkflowEngineTestBase.TestConfig.class)
abstract class WorkflowEngineTestBase {

    /** Fixed clock: time measurements and expires_at become reproducible. */
    protected static final Instant NOW = Instant.parse("2026-09-25T12:00:00Z");

    protected static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @SuppressWarnings("resource")
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("agent_platform")
                    .withUsername("test")
                    .withPassword("test");

    static {
        POSTGRES.start();
        migrateRealSchema();
    }

    /**
     * Applies the orchestrator's <em>real</em> migrations against the container.
     *
     * <p>Deliberately Flyway instead of a copied schema string (as the worker test has to
     * do for lack of Flyway): the stores in the test should run against exactly the schema
     * that also arises in production. A divergence between migration and store SQL shows
     * up here and not only in operation.
     */
    private static void migrateRealSchema() {
        var result = org.flywaydb.core.Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
        if (result.migrationsExecuted == 0) {
            throw new IllegalStateException(
                    "Flyway executed no migration — V1__init.sql from "
                            + "classpath:db/migration was expected. Is "
                            + "agent-orchestrator/src/main/resources on the test classpath?");
        }
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    // ── Production beans under test ──

    @Autowired protected WorkflowEngine engine;
    @Autowired protected JdbcCheckpointStore checkpoints;
    @Autowired protected JdbcWorkflowExecutionStore executions;
    @Autowired protected JdbcHitlTicketStore ticketStore;
    @Autowired protected WorkflowDefinitionService definitions;
    @Autowired protected TaskDispatcher dispatcher;
    @Autowired protected RecordingPublisher published;
    @Autowired protected JdbcTemplate jdbc;

    // ── Testhilfen ──

    @BeforeEach
    void resetState() {
        // Reihenfolge beachten: FK-Beziehungen.
        jdbc.update("DELETE FROM dlq_messages");
        jdbc.update("DELETE FROM hitl_tickets");
        jdbc.update("DELETE FROM workflow_checkpoints");
        jdbc.update("DELETE FROM workflow_executions");
        jdbc.update("DELETE FROM workflow_definitions");
        jdbc.update("DELETE FROM agent_definitions");
        jdbc.update("DELETE FROM shedlock");
        published.clear();
    }

    /** Builds a linear workflow DAG: a → b → end. */
    protected UUID linearWorkflow() {
        return insertWorkflow("test-linear", """
                {
                  "nodes": [
                    {"stepId":"a","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"b","nodeType":"STEP","agentType":"coder","dependsOn":["a"]},
                    {"stepId":"end","nodeType":"END","dependsOn":["b"]}
                  ]
                }
                """);
    }

    /**
     * Builds a DAG with a DECISION node: trigger → gate → x / end.
     *
     * <p>The condition deliberately uses the <em>flat</em> path
     * {@code context['trigger'].mode}. The context from
     * {@code JdbcCheckpointStore.outputContext} ist {@code context[stepId] = output_payload},
     * and {@code output_payload} is the flat {@code result_data} of the worker.
     * An intermediate level {@code .resultData} does not exist — see
     * {@code ExpressionEvaluatorContextContractTest}, which pins exactly this contract.
     */
    protected UUID decisionWorkflow() {
        return insertWorkflow("test-decision", """
                {
                  "nodes": [
                    {"stepId":"trigger","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"gate","nodeType":"DECISION","dependsOn":["trigger"],
                     "branches":[
                       {"branchId":"go","condition":"context['trigger'].mode == 'go'",
                        "target":"after"}
                     ],
                     "defaultTarget":"end"},
                    {"stepId":"after","nodeType":"STEP","agentType":"coder","dependsOn":["gate"]},
                    {"stepId":"end","nodeType":"END","dependsOn":["trigger","gate","after"]}
                  ]
                }
                """);
    }

    /** Workflow with a HITL gate before the second step. */
    protected UUID approvalWorkflow() {
        return insertWorkflow("test-approval", """
                {
                  "nodes": [
                    {"stepId":"a","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"gate","nodeType":"STEP","agentType":"reviewer","dependsOn":["a"],
                     "requiresHumanApproval":true},
                    {"stepId":"end","nodeType":"END","dependsOn":["gate"]}
                  ]
                }
                """);
    }

    protected UUID insertWorkflow(String name, String dagJson) {
        return definitions.insertIfAbsent(name, 1, EventJson.write(Map.of("nodes",
                parseNodes(dagJson))), true);
    }

    private List<Map<String, Object>> parseNodes(String dagJson) {
        try {
            var root = new ObjectMapper().readTree(dagJson);
            List<Map<String, Object>> nodes = new ArrayList<>();
            root.get("nodes").forEach(n -> {
                Map<String, Object> m = new ObjectMapper().convertValue(n, Map.class);
                nodes.add(m);
            });
            return nodes;
        } catch (Exception e) {
            throw new IllegalStateException("DAG-JSON unparsbar", e);
        }
    }

    /** Starts an execution and fires {@code onIntake} — the normal case. */
    protected WorkflowExecutionRow start(UUID definitionId, String inputJson) {
        UUID executionId = UUID.randomUUID();
        String idempotencyKey = "idem-" + executionId;
        executions.create(executionId, definitionId, idempotencyKey, inputJson);
        engine.onIntake(new WorkflowStartEvent(
                executionId.toString(),
                definitionId.toString(),
                "test-workflow",
                1,
                parseParams(inputJson),
                idempotencyKey,
                NOW));
        return executions.findById(executionId).orElseThrow();
    }

    private Map<String, Object> parseParams(String json) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = new ObjectMapper().readValue(json, Map.class);
            return parsed == null ? Map.of() : parsed;
        } catch (Exception e) {
            throw new IllegalStateException("inputParams-JSON unparsbar: " + json, e);
        }
    }

    protected WorkflowExecutionRow startLinear(String inputJson) {
        return start(linearWorkflow(), inputJson);
    }

    // ── Assertions ──

    /** The most recently published TaskEvent on a task topic, or null. */
    protected TaskEvent lastDispatchedTask() {
        List<TaskEvent> tasks = published.taskEvents();
        return tasks.isEmpty() ? null : tasks.get(tasks.size() - 1);
    }

    protected List<TaskEvent> dispatchedTasks() {
        return published.taskEvents();
    }

    /**
     * All tasks dispatched so far for a given step.
     *
     * <p>Replaces the worker in the test: instead of a real Kafka roundtrip, the
     * TaskEvent published by the orchestrator is looked up and answered with a
     * self-built {@code ResultEvent}. The path
     * engine → routing → TaskEvent is thereby real, only the execution is simulated.
     */
    protected List<TaskEvent> tasksFor(String stepId) {
        return dispatchedTasks().stream()
                .filter(t -> stepId.equals(t.stepId()))
                .toList();
    }

    /**
     * Answers the most recently dispatched task of a step.
     *
     * @return the TaskEvent that was answered
     * @throws AssertionError if no task was dispatched for the step
     */
    protected TaskEvent completeStep(String stepId, Map<String, Object> resultData) {
        List<TaskEvent> tasks = tasksFor(stepId);
        assertThat(tasks)
                .as("A task must have been dispatched for step '%s'", stepId)
                .isNotEmpty();
        TaskEvent task = tasks.get(tasks.size() - 1);
        engine.onResult(resultFor(task, resultData));
        return task;
    }

    /** Convenience: COMPLETED with given additional fields. */
    protected TaskEvent completeStep(String stepId, String status, Map<String, Object> extra) {
        Map<String, Object> data = new java.util.LinkedHashMap<>(extra);
        data.put("status", status);
        return completeStep(stepId, data);
    }

    protected List<String> dispatchedTopics() {
        return published.topics();
    }

    protected Optional<CheckpointRow> checkpoint(UUID executionId, String stepId) {
        return checkpoints.latest(executionId, stepId);
    }

    protected ExecutionStatus status(UUID executionId) {
        return executions.findById(executionId).orElseThrow().status();
    }

    /**
     * Builds a ResultEvent for a just-dispatched task. Deliberately uses the real
     * {@code taskId} from the published TaskEvent — that way the mapping is not hard-coded
     * and a bug in {@code deterministicTaskId} shows up.
     */
    protected ResultEvent resultFor(TaskEvent task, Map<String, Object> resultData) {
        return new ResultEvent(
                new ResultEvent.Header("trace-" + task.header().workflowId(), NOW.toEpochMilli()),
                task.header().workflowId(),
                task.header().taskId(),
                task.stepId(),
                task.iteration(),
                task.header().attempt(),
                NOW.toEpochMilli(),
                NOW.toEpochMilli(),
                resultData);
    }

    protected ResultEvent completed(TaskEvent task) {
        return resultFor(task, Map.of(
                "status", "COMPLETED",
                "response", "ok",
                "confidence", 0.9));
    }

    protected ResultEvent failed(TaskEvent task, String status, String message) {
        return resultFor(task, Map.of(
                "status", status,
                "errorMessage", message));
    }

    // ── Configuration ──

    @Configuration
    @EnableTransactionManagement
    static class TestConfig {

        @Bean
        DataSource dataSource() {
            return org.springframework.boot.jdbc.DataSourceBuilder.create()
                    .url(POSTGRES.getJdbcUrl())
                    .username(POSTGRES.getUsername())
                    .password(POSTGRES.getPassword())
                    .driverClassName("org.postgresql.Driver")
                    .build();
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        /**
         * Without this bean {@code @Transactional} on the engine goes into the void.
         * It is also the reason the engine takes its locks seriously:
         * {@code executions.lock()} runs in a real transaction.
         */
        @Bean
        org.springframework.transaction.PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource);
        }

        @Bean
        OrchestratorProperties properties() {
            // Explicit defaults, matching the defaults in the record itself.
            return new OrchestratorProperties(
                    new OrchestratorProperties.Kafka(List.of()),
                    new OrchestratorProperties.Orchestration(60_000L, 3, 5_000L, 1_000L, 600_000L),
                    new OrchestratorProperties.Hitl(24L, 3),
                    new OrchestratorProperties.Seeding(false, ""),
                    new OrchestratorProperties.AutoScaling(false, 30, 500, 2, 20, null, null),
                    new OrchestratorProperties.Vault(false, "http://vault:8200", "", "secret/data", 60L),
                    new OrchestratorProperties.Lease("test-node", 30L),
                null);
        }

        @Bean
        Clock clock() {
            return FIXED_CLOCK;
        }

        @Bean
        OrchestratorMetrics metrics() {
            return new OrchestratorMetrics(new SimpleMeterRegistry());
        }

        @Bean
        JdbcWorkflowDefinitionStore workflowDefinitionStore(JdbcTemplate jdbcTemplate) {
            return new JdbcWorkflowDefinitionStore(jdbcTemplate);
        }

        @Bean
        JdbcWorkflowExecutionStore workflowExecutionStore(JdbcTemplate jdbcTemplate, Clock clock) {
            return new JdbcWorkflowExecutionStore(jdbcTemplate, clock);
        }

        @Bean
        JdbcCheckpointStore checkpointStore(JdbcTemplate jdbcTemplate, Clock clock) {
            return new JdbcCheckpointStore(jdbcTemplate, clock);
        }

        @Bean
        JdbcHitlTicketStore hitlTicketStore(JdbcTemplate jdbcTemplate, Clock clock) {
            return new JdbcHitlTicketStore(jdbcTemplate, clock);
        }

        @Bean
        JdbcAgentDefinitionStore agentDefinitionStore(JdbcTemplate jdbcTemplate) {
            return new JdbcAgentDefinitionStore(jdbcTemplate);
        }

        @Bean
        JdbcDlqStore dlqStore(JdbcTemplate jdbcTemplate, Clock clock) {
            return new JdbcDlqStore(jdbcTemplate, clock);
        }

        @Bean
        WorkflowDefinitionService workflowDefinitionService(JdbcWorkflowDefinitionStore store) {
            return new WorkflowDefinitionService(store);
        }

        @Bean
        HitlTicketService hitlTicketService(JdbcHitlTicketStore store, OrchestratorProperties props, Clock clock) {
            return new HitlTicketService(store, props, clock);
        }

        @Bean
        RecordingPublisher publisher(
                org.springframework.beans.factory.ObjectProvider<io.micrometer.tracing.Tracer> tracer) {
            return new RecordingPublisher(tracer.getIfAvailable());
        }

        @Bean
        DlqService dlqService(JdbcDlqStore store, RecordingPublisher publisher, Clock clock) {
            return new DlqService(store, publisher, clock);
        }

        @Bean
        TaskDispatcher taskDispatcher(JdbcAgentDefinitionStore store, RecordingPublisher publisher,
                                      OrchestratorProperties props, Clock clock) {
            return new TaskDispatcher(store, publisher, props, clock);
        }

        @Bean
        WorkflowEngine workflowEngine(JdbcWorkflowExecutionStore executions,
                                      JdbcCheckpointStore checkpoints,
                                      JdbcHitlTicketStore ticketStore,
                                      WorkflowDefinitionService definitions,
                                      HitlTicketService hitlTickets,
                                      RoutingAdvancer routingAdvancer,
                                      TaskDispatchCoordinator dispatchCoordinator,
                                      ExecutionCanceller canceller,
                                      DlqService dlqService,
                                      RecordingPublisher publisher,
                                      OrchestratorProperties props,
                                      OrchestratorMetrics metrics,
                                      Clock clock) {
            return new WorkflowEngine(executions, checkpoints, ticketStore, definitions, hitlTickets,
                    routingAdvancer, dispatchCoordinator, canceller, dlqService, publisher, props, metrics, clock);
        }

        @Bean
        TaskDispatchCoordinator taskDispatchCoordinator(JdbcCheckpointStore checkpoints,
                                                        JdbcWorkflowExecutionStore executions,
                                                        WorkflowDefinitionService definitions,
                                                        TaskDispatcher dispatcher,
                                                        HitlTicketService hitlTickets,
                                                        OrchestratorProperties props,
                                                        Clock clock) {
            return new TaskDispatchCoordinator(checkpoints, executions, definitions, dispatcher,
                    hitlTickets, props, clock);
        }

        @Bean
        RoutingAdvancer routingAdvancer(JdbcWorkflowExecutionStore executions,
                                        JdbcCheckpointStore checkpoints,
                                        WorkflowDefinitionService definitions,
                                        TaskDispatchCoordinator dispatchCoordinator,
                                        OrchestratorMetrics metrics,
                                        OrchestratorProperties props,
                                        Clock clock) {
            return new RoutingAdvancer(executions, checkpoints, definitions, dispatchCoordinator,
                    metrics, props, clock);
        }

        @Bean
        ExecutionCanceller executionCanceller(JdbcCheckpointStore checkpoints,
                                              JdbcWorkflowExecutionStore executions,
                                              WorkflowDefinitionService definitions,
                                              OrchestratorProperties props,
                                              Clock clock) {
            return new ExecutionCanceller(checkpoints, executions, definitions, props, clock);
        }

        @Bean
        OrchestratorLeaseService orchestratorLeaseService(JdbcTemplate jdbcTemplate,
                                                          OrchestratorProperties props) {
            return new OrchestratorLeaseService(jdbcTemplate, "test-node", props);
        }

        @Bean
        SweepReaperService sweepReaperService(JdbcCheckpointStore checkpointStore,
                                              JdbcWorkflowExecutionStore executionStore,
                                              HitlTicketService hitlTicketService,
                                              WorkflowEngine engine,
                                              OrchestratorProperties props,
                                              OrchestratorLeaseService lease,
                                              Clock clock) {
            return new SweepReaperService(checkpointStore, executionStore, hitlTicketService,
                    engine, props, lease, clock);
        }
    }

    /**
     * Kafka fake that records publish calls instead of sending them.
     *
     * <p>Deliberately overrides {@code publishAfterCommit}: the engine uses it for tasks
     * and loop events. Because a real transaction runs in the test, the registration would
     * otherwise only fire after the commit — the recording therefore observes both paths.
     */
    static class RecordingPublisher extends OrchestratorEventPublisher {

        private final List<String> topics = java.util.Collections.synchronizedList(new ArrayList<>());
        private final List<TaskEvent> tasks = java.util.Collections.synchronizedList(new ArrayList<>());

        RecordingPublisher(io.micrometer.tracing.Tracer tracer) {
            // kafkaTemplate null because publish()/publishAfterCommit() are overridden.
            // The tracer comes from the container, here normally null.
            super(null, new org.springframework.beans.factory.ObjectProvider<>() {
                @Override
                public io.micrometer.tracing.Tracer getObject() {
                    return tracer;
                }

                @Override
                public io.micrometer.tracing.Tracer getObject(Object... args) {
                    return tracer;
                }

                @Override
                public io.micrometer.tracing.Tracer getIfAvailable() {
                    return tracer;
                }

                @Override
                public io.micrometer.tracing.Tracer getIfUnique() {
                    return tracer;
                }
            });
        }

        @Override
        public void publish(String topic, String key, String payload) {
            topics.add(topic);
            if (topic.startsWith(TopicNames.TASKS_PREFIX)) {
                try {
                    tasks.add(EventJson.read(payload, TaskEvent.class));
                } catch (RuntimeException ignored) {
                    // Not a parseable task — irrelevant for the assertions.
                }
            }
        }

        @Override
        public void publishAfterCommit(String topic, String key, String payload) {
            publish(topic, key, payload);
        }

        List<String> topics() {
            return List.copyOf(topics);
        }

        List<TaskEvent> taskEvents() {
            return List.copyOf(tasks);
        }

        void clear() {
            topics.clear();
            tasks.clear();
        }
    }
}
