package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.bridge.AgentDefinitionBridge;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import de.augmentia.agentplatform.orchestrator.domain.AgentDefinitionRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final JdbcAgentDefinitionStore agentDefinitions;
    private final OrchestratorEventPublisher publisher;
    private final OrchestratorProperties properties;
    private final Clock clock;

    public TaskDispatcher(JdbcAgentDefinitionStore agentDefinitions,
                          OrchestratorEventPublisher publisher,
                          OrchestratorProperties properties,
                          Clock clock) {
        this.agentDefinitions = agentDefinitions;
        this.publisher = publisher;
        this.properties = properties;
        this.clock = clock;
    }

    public static UUID deterministicTaskId(UUID executionId, String stepId, int iteration, int attempt) {
        String material = executionId + "|" + stepId + "|" + iteration + "|" + attempt;
        return UUID.nameUUIDFromBytes(material.getBytes(StandardCharsets.UTF_8));
    }

    public AgentDefinitionRow resolveAgent(String agentType) {
        return agentDefinitions.findByType(agentType).orElseGet(() -> new AgentDefinitionRow(
                agentType,
                "Default deterministic mock configuration",
                "Execute the assigned workflow step deterministically.",
                "mock-deterministic",
                0.0d,
                1024,
                (int) Math.max(1, properties.orchestration().defaultTimeoutMs() / 1000),
                properties.orchestration().defaultMaxAttempts(),
                1,
                AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS
        ));
    }

    /**
     * Determines the task timeout and caps it at {@code orchestration.max-task-timeout-ms}.
     *
     * <p><b>Why a cap is necessary:</b> The quad worker processes a Kafka batch by blocking
     * the poll thread until all records are done. If it blocks longer than
     * {@code max.poll.interval.ms}, the consumer is kicked out of the group, there is a
     * rebalance, and the still-running tasks are executed again by another instance — with
     * LLM-Kosten als Folge.
     *
     * <p>A value that is too large is therefore <em>capped</em> and warned about loudly
     * instead of refusing a workflow start outright: a timeout that is too short is a
     * correctable error, a hanging rebalance loop is not.
     */
    public long resolveTimeoutMs(DagNode node, AgentDefinitionRow agent) {
        long resolved = properties.orchestration().defaultTimeoutMs();
        if (node.timeoutMs() != null) {
            resolved = node.timeoutMs();
        } else if (agent.timeoutSeconds() > 0) {
            resolved = agent.timeoutSeconds() * 1000L;
        }

        long ceiling = properties.orchestration().maxTaskTimeoutMs();
        if (resolved > ceiling) {
            log.warn("Task timeout {}ms for step '{}' (agentType={}) exceeds max-task-timeout-ms={} — " +
                    "capping to avoid consumer rebalance. Raise app.orchestration.max-task-timeout-ms only " +
                    "together with worker.quad.kafka.max-poll-interval-ms.",
                    resolved, node.stepId(), node.agentType(), ceiling);
            return ceiling;
        }
        return resolved;
    }

    public int resolveMaxAttempts(AgentDefinitionRow agent) {
        return agent.maxRetryAttempts() > 0 ? agent.maxRetryAttempts() : properties.orchestration().defaultMaxAttempts();
    }

    public TaskEvent buildTaskEvent(WorkflowExecutionRow execution,
                                    WorkflowDefinition definition,
                                    DagNode node,
                                    int iteration,
                                    int attempt,
                                    int maxAttempts,
                                    long timeoutMs,
                                    UUID taskId,
                                    Map<String, Object> input,
                                    Map<String, Object> context) {
        AgentDefinitionRow agent = resolveAgent(node.agentType());
        Map<String, Object> inputJson = new LinkedHashMap<>();
        inputJson.put("workflowName", definition.name());
        inputJson.put("workflowVersion", definition.version());
        inputJson.put("stepId", node.stepId());
        inputJson.put("nodeType", node.nodeType().name());
        inputJson.put("iteration", iteration);
        inputJson.put("attempt", attempt);
        inputJson.put("requiresHumanApproval", node.requiresHumanApproval());
        inputJson.put("input", input);
        inputJson.put("context", context);

        String prompt = "Execute step '" + node.stepId() + "' (iteration " + iteration + ", attempt " + attempt
                + ") of workflow '" + definition.name() + "' v" + definition.version() + ".";

        @SuppressWarnings("unchecked")
        List<TaskEvent.ContextReference> contextReferences = node.dependsOn().stream()
                .map(dep -> new TaskEvent.ContextReference(
                        dep,
                        dep,
                        context.get(dep) instanceof Map<?, ?> m ? (Map<String, Object>) m : null
                ))
                .toList();

        return new TaskEvent(
                new TaskEvent.Header(
                        execution.id().toString(),
                        taskId.toString(),
                        execution.id() + ":" + taskId,
                        attempt,
                        clock.instant().toEpochMilli()
                ),
                agent.agentType(),
                node.stepId(),
                iteration,
                prompt,
                inputJson,
                contextReferences,
                timeoutMs
        );
    }

    public String inputPayloadJson(TaskEvent event) {
        return EventJson.write(event.inputJson());
    }

    public void dispatch(TaskEvent event) {
        publisher.publishAfterCommit(
                TopicNames.tasks(event.agentType()),
                event.header().workflowId(),
                EventJson.write(event)
        );
    }
}
