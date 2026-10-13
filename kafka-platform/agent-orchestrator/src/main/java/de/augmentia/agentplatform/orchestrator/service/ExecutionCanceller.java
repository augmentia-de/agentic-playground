package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.RoutingEvaluator;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcCheckpointStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointInsert;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

/**
 * Aborts an execution and sets all open checkpoints to CANCELLED.
 *
 * <p>Extracted from {@link WorkflowEngine}. Deliberately uses
 * {@link JdbcCheckpointStore#latestByStep(UUID)} instead of a
 * {@code latest()} call per node — on a large DAG that would be an N+1
 * within the transaction.
 *
 * <p>Completed checkpoints stay completed. A workflow that has
 * already produced partial results must not lose them through an abort
 * — that is why only PENDING, IN_PROGRESS, WAITING_HITL and
 * FAILED are switched.
 */
@Component
public class ExecutionCanceller {

    private static final String REASON = "cancelled";

    private final JdbcCheckpointStore checkpoints;
    private final JdbcWorkflowExecutionStore executions;
    private final OrchestratorProperties properties;
    private final WorkflowDefinitionService definitions;
    private final Clock clock;

    public ExecutionCanceller(JdbcCheckpointStore checkpoints,
                              JdbcWorkflowExecutionStore executions,
                              WorkflowDefinitionService definitions,
                              OrchestratorProperties properties,
                              Clock clock) {
        this.checkpoints = checkpoints;
        this.executions = executions;
        this.definitions = definitions;
        this.properties = properties;
        this.clock = clock;
    }

    public void cancel(WorkflowExecutionRow execution, String reason, String actor) {
        WorkflowDefinition dag = definitions.loadDefinition(execution.workflowDefinitionId());

        // one query instead of N: on a large DAG a latest() per node would be
        // an N+1 within the transaction.
        Map<String, CheckpointRow> latestByStep = checkpoints.latestByStep(execution.id());

        for (DagNode node : RoutingEvaluator.orderedNodes(dag)) {
            CheckpointRow latest = latestByStep.get(node.stepId());
            if (latest == null) {
                checkpoints.insertIfAbsent(new CheckpointInsert(
                        UUID.randomUUID(),
                        execution.id(),
                        node.stepId(),
                        1,
                        node.nodeType(),
                        node.agentType(),
                        null,
                        CheckpointStatus.CANCELLED,
                        REASON,
                        null,
                        null,
                        null,
                        1,
                        properties.orchestration().defaultMaxAttempts(),
                        properties.orchestration().defaultTimeoutMs(),
                        null,
                        clock.instant()
                ));
            } else if (isCancellable(latest.status())) {
                checkpoints.markCancelled(latest.id());
            }
        }

        executions.markCancelled(execution.id(), EventJson.write(Map.of(
                "reason", nullSafe(reason),
                "actor", nullSafe(actor)
        )));
    }

    private static boolean isCancellable(CheckpointStatus status) {
        return status == CheckpointStatus.PENDING
                || status == CheckpointStatus.IN_PROGRESS
                || status == CheckpointStatus.WAITING_HITL
                || status == CheckpointStatus.FAILED;
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }
}
