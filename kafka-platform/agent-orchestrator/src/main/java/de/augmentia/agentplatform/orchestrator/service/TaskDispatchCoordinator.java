package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.RoutingEvaluator;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcCheckpointStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.AgentDefinitionRow;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointInsert;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.TaskEvent;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

/**
 * Responsible for the <em>firing</em> of steps: from the routing result to the
 * published task event, including the approval gate.
 *
 * <p>Extracted from {@link WorkflowEngine} so that the engine does not carry
 * routing evaluation, checkpoint write logic and task dispatch at the same time. The
 * caller determines the transaction — all methods here run in the
 * caller's transaction and do not open one themselves. That is
 * important because {@code publishAfterCommit} exploits exactly this.
 *
 * <p>Idempotency: {@code insertIfAbsent} returns whether the row was newly
 * created. Only for {@code true} is a dispatch performed — a repeated
 * firing thus does not create a second task.
 */
@Component
public class TaskDispatchCoordinator {

    private final JdbcCheckpointStore checkpoints;
    private final JdbcWorkflowExecutionStore executions;
    private final WorkflowDefinitionService definitions;
    private final TaskDispatcher dispatcher;
    private final HitlTicketService hitlTickets;
    private final OrchestratorProperties properties;
    private final Clock clock;

    public TaskDispatchCoordinator(JdbcCheckpointStore checkpoints,
                                   JdbcWorkflowExecutionStore executions,
                                   WorkflowDefinitionService definitions,
                                   TaskDispatcher dispatcher,
                                   HitlTicketService hitlTickets,
                                   OrchestratorProperties properties,
                                   Clock clock) {
        this.checkpoints = checkpoints;
        this.executions = executions;
        this.definitions = definitions;
        this.dispatcher = dispatcher;
        this.hitlTickets = hitlTickets;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Fires a step determined by routing. With
     * {@code requiresHumanApproval} a HITL ticket is created instead of the task
     * and the execution is paused.
     */
    public void fire(WorkflowExecutionRow execution, WorkflowDefinition dag,
                     RoutingEvaluator.Fire routingFire,
                     Map<String, Object> input, Map<String, Object> context) {
        DagNode node = dag.node(routingFire.stepId());
        CheckpointRow latest = checkpoints.latest(execution.id(), node.stepId()).orElse(null);
        int requested = routingFire.iteration();
        CheckpointRow existing = null;
        if (latest != null) {
            if (latest.iteration() > requested) {
                return; // stale fire from an older iteration
            }
            if (latest.iteration() == requested) {
                if (latest.status() != CheckpointStatus.PENDING) {
                    return; // already dispatched or terminal for this iteration
                }
                existing = latest;
            }
            // requested > latest.iteration(): a fresh region-loop iteration -> new checkpoint row
        }
        int iteration = requested;

        if (node.requiresHumanApproval()) {
            openApprovalGate(execution, node, iteration);
            return;
        }

        dispatchIteration(execution, dag, node, iteration, 1, existing);
    }

    /**
     * Approval gate: create a checkpoint in WAITING_HITL and create a ticket.
     * No task is deliberately published — the step waits for a human.
     */
    private void openApprovalGate(WorkflowExecutionRow execution, DagNode node, int iteration) {
        UUID taskId = TaskDispatcher.deterministicTaskId(execution.id(), node.stepId(), iteration, 1);
        AgentDefinitionRow agent = dispatcher.resolveAgent(node.agentType());
        checkpoints.insertIfAbsent(new CheckpointInsert(
                UUID.randomUUID(),
                execution.id(),
                node.stepId(),
                iteration,
                node.nodeType(),
                node.agentType(),
                taskId,
                CheckpointStatus.WAITING_HITL,
                "approval-required",
                null,
                null,
                null,
                1,
                dispatcher.resolveMaxAttempts(agent),
                dispatcher.resolveTimeoutMs(node, agent),
                null,
                null
        ));
        CheckpointRow approvalCheckpoint = checkpoints.latest(execution.id(), node.stepId()).orElse(null);
        UUID checkpointId = approvalCheckpoint == null ? null : approvalCheckpoint.id();
        hitlTickets.createIfAbsent(execution.id(), checkpointId, node.stepId(), iteration,
                "APPROVAL_REQUIRED", Map.of(
                        "stepId", node.stepId(),
                        "iteration", iteration,
                        "agentType", nullSafe(node.agentType())
                ));
        executions.markPausedHitl(execution.id());
    }

    /**
     * Creates the checkpoint (if new) and publishes the task.
     */
    public void dispatchIteration(WorkflowExecutionRow execution, WorkflowDefinition dag, DagNode node,
                                  int iteration, int attempt, CheckpointRow existing) {
        AgentDefinitionRow agent = dispatcher.resolveAgent(node.agentType());
        int maxAttempts = existing == null ? dispatcher.resolveMaxAttempts(agent) : existing.maxAttempts();
        long timeoutMs = existing == null ? dispatcher.resolveTimeoutMs(node, agent) : existing.timeoutMs();
        UUID taskId = TaskDispatcher.deterministicTaskId(execution.id(), node.stepId(), iteration, attempt);
        Map<String, Object> input = readMap(execution.inputParamsJson());
        Map<String, Object> context = checkpoints.outputContext(execution.id());
        TaskEvent event = dispatcher.buildTaskEvent(execution, dag, node, iteration, attempt,
                maxAttempts, timeoutMs, taskId, input, context);
        String inputJson = dispatcher.inputPayloadJson(event);

        if (existing == null) {
            boolean inserted = checkpoints.insertIfAbsent(new CheckpointInsert(
                    UUID.randomUUID(),
                    execution.id(),
                    node.stepId(),
                    iteration,
                    node.nodeType(),
                    node.agentType(),
                    taskId,
                    CheckpointStatus.IN_PROGRESS,
                    null,
                    null,
                    inputJson,
                    null,
                    attempt,
                    maxAttempts,
                    timeoutMs,
                    clock.instant(),
                    null
            ));
            if (inserted) {
                dispatcher.dispatch(event);
            }
        } else {
            checkpoints.dispatchNewTask(existing.id(), taskId, attempt, inputJson, clock.instant());
            dispatcher.dispatch(event);
        }
    }

    /**
     * Advances an existing checkpoint — after HITL approval or a sweep retry.
     *
     * @param retry true increments the attempt and counts the retry
     */
    public void resumeExistingCheckpoint(WorkflowExecutionRow execution, DagNode node,
                                         CheckpointRow checkpoint, int attempt, boolean retry,
                                         String reason) {
        WorkflowDefinition dag = definitions.loadDefinition(execution.workflowDefinitionId());
        Map<String, Object> input = readMap(execution.inputParamsJson());
        Map<String, Object> context = checkpoints.outputContext(execution.id());
        UUID taskId = TaskDispatcher.deterministicTaskId(execution.id(), node.stepId(),
                checkpoint.iteration(), attempt);
        TaskEvent event = dispatcher.buildTaskEvent(execution, dag, node, checkpoint.iteration(), attempt,
                checkpoint.maxAttempts(), checkpoint.timeoutMs(), taskId, input, context);
        String inputJson = dispatcher.inputPayloadJson(event);
        if (retry) {
            checkpoints.retryTask(checkpoint.id(), taskId, attempt, inputJson, reason, clock.instant());
        } else {
            checkpoints.dispatchNewTask(checkpoint.id(), taskId, attempt, inputJson, clock.instant());
        }
        dispatcher.dispatch(event);
    }

    private Map<String, Object> readMap(String json) {
        if (json == null || json.isBlank()) {
            return new java.util.LinkedHashMap<>();
        }
        try {
            Map<String, Object> map = EventJson.read(json, Map.class);
            return map == null ? new java.util.LinkedHashMap<>() : map;
        } catch (RuntimeException ex) {
            return new java.util.LinkedHashMap<>();
        }
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }
}
