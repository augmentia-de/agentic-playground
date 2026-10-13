package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.*;
import de.augmentia.agentplatform.common.routing.RoutingEvaluator;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcCheckpointStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointInsert;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import de.augmentia.agentplatform.orchestrator.metrics.OrchestratorMetrics;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The routing core: evaluates the persisted checkpoint state and writes
 * the result back.
 *
 * <p>Ausgelagert aus {@link WorkflowEngine}. Die Trennlinie ist bewusst an der
 * question is drawn: <em>what</em> to do next (here) versus <em>how</em>
 * a step is started ({@link TaskDispatchCoordinator}).
 *
 * <p>The caller holds the transaction; {@link RoutingEvaluator} itself is pure
 * and deterministic. The duration of the evaluation is measured with
 * {@link System#nanoTime()}, not with the domain clock — with a fixed test clock
 * it would always be 0.
 */
@Component
public class RoutingAdvancer {

    private final JdbcWorkflowExecutionStore executions;
    private final JdbcCheckpointStore checkpoints;
    private final WorkflowDefinitionService definitions;
    private final TaskDispatchCoordinator dispatchCoordinator;
    private final OrchestratorMetrics metrics;
    private final OrchestratorProperties properties;
    private final Clock clock;

    public RoutingAdvancer(JdbcWorkflowExecutionStore executions,
                           JdbcCheckpointStore checkpoints,
                           WorkflowDefinitionService definitions,
                           TaskDispatchCoordinator dispatchCoordinator,
                           OrchestratorMetrics metrics,
                           OrchestratorProperties properties,
                           Clock clock) {
        this.executions = executions;
        this.checkpoints = checkpoints;
        this.definitions = definitions;
        this.dispatchCoordinator = dispatchCoordinator;
        this.metrics = metrics;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Evaluates the routing and writes decisions, auto-completes and
     *      * prunes back. Then fires all determined steps.
     *
     * <p>Sets the execution to COMPLETED when the workflow end is reached.
     */
    public void advance(WorkflowExecutionRow execution) {
        WorkflowDefinition dag = definitions.loadDefinition(execution.workflowDefinitionId());
        Map<String, CheckpointSnapshot> state = checkpoints.snapshotByStep(execution.id());
        Map<String, Object> context = checkpoints.outputContext(execution.id());
        Map<String, Object> input = readMap(execution.inputParamsJson());

        long evalStart = System.nanoTime();
        RoutingEvaluator.Result result = RoutingEvaluator.evaluate(state, dag, context, input);
        long evalDurationMs = (System.nanoTime() - evalStart) / 1_000_000L;

        Set<String> prunedSteps = result.pruned().stream()
                .map(RoutingEvaluator.Prune::stepId)
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));

        for (RoutingEvaluator.Decision decision : result.decisions()) {
            // branch ID and target are persisted separately: from the database
            // it can thus be answered which branch ran — previously only
            // the target step was stored there.
            writeRoutingCheckpoint(execution, dag, decision.stepId(), CheckpointStatus.COMPLETED,
                    decision.chosenTarget(), decision.branchId(), decision.iteration());
            DagNode node = dag.byIdOrNull(decision.stepId());
            if (node != null) {
                metrics.routingDecisionsTotal("routing-decision").increment();
                if (decision.branchId() != null) {
                    metrics.routingDecisionsTotal("branch:" + decision.branchId()).increment();
                }
            }
        }
        // Running region-LOOPs are routing-only (no task), but their iteration is persisted
        // as an IN_PROGRESS checkpoint so loop progress is observable and restart-stable.
        Set<String> completedSteps = result.completedSteps().stream()
                .map(RoutingEvaluator.CompletedStep::stepId)
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        for (RoutingEvaluator.LoopProgress loop : result.runningLoops()) {
            if (completedSteps.contains(loop.stepId())) {
                continue;
            }
            writeLoopProgress(execution, dag, loop);
        }
        for (RoutingEvaluator.CompletedStep step : result.completedSteps()) {
            writeRoutingCheckpoint(execution, dag, step.stepId(), CheckpointStatus.COMPLETED, null, null, step.iteration());
            metrics.routingDecisionsTotal("auto-complete").increment();
        }
        for (RoutingEvaluator.Prune prune : result.pruned()) {
            CheckpointStatus status = prune.prune() ? CheckpointStatus.PRUNED : CheckpointStatus.SKIPPED;
            writeRoutingCheckpoint(execution, dag, prune.stepId(), status, prune.reason(), null, prune.iteration());
            metrics.pruningStepsTotal(prune.prune() ? "prune" : "skip").increment();
            DagNode node = dag.byIdOrNull(prune.stepId());
            if (node != null) {
                metrics.routingDecisionsTotal("pruning").increment();
            }
        }
        for (RoutingEvaluator.Fire fire : result.fired()) {
            if (prunedSteps.contains(fire.stepId())) {
                continue;
            }
            dispatchCoordinator.fire(execution, dag, fire, input, context);
            DagNode node = dag.node(fire.stepId());
            if (node != null && node.nodeType() == NodeType.JOIN) {
                metrics.joinWaitDurationSeconds(evalDurationMs / 1000.0);
            }
        }

        if (result.workflowEndReached()) {
            executions.markCompleted(execution.id(), finalOutputJson(context));
        } else if (execution.status() == ExecutionStatus.PENDING) {
            executions.markRunning(execution.id());
        }
    }

    /**
     * Writes a routing checkpoint without overwriting an already terminal state.
     * Repeated calls with the same result are no-ops — that is
     * the basis for {@code advance} being allowed to run multiple times.
     *
     * <p>Iteration-aware: if a row already exists for exactly this iteration, it is
     * updated in place. If the last row belongs to an earlier iteration
     * (body node of a region LOOP that has continued), a new row is created for the
     * current iteration instead of overwriting the old one. Results of an
     * outdated iteration are discarded.
     */
    private void writeRoutingCheckpoint(WorkflowExecutionRow execution, WorkflowDefinition dag, String stepId,
                                        CheckpointStatus status, String branch, String branchId, int iteration) {
        DagNode node = dag.byIdOrNull(stepId);
        if (node == null) {
            return;
        }
        CheckpointRow latest = checkpoints.latest(execution.id(), stepId).orElse(null);
        Instant completedAt = isCompletedLike(status) ? clock.instant() : null;

        if (latest != null && latest.iteration() > iteration) {
            // A newer iteration already owns this step; a late result must not overwrite it.
            return;
        }
        if (latest != null && latest.iteration() == iteration) {
            if (latest.status() == status && java.util.Objects.equals(latest.decisionBranch(), branch)) {
                return;
            }
            if (latest.isTerminal()) {
                return;
            }
            checkpoints.updateRoutingStatus(latest.id(), status, branch, branchId, completedAt);
            return;
        }

        checkpoints.insertIfAbsent(new CheckpointInsert(
                UUID.randomUUID(),
                execution.id(),
                stepId,
                iteration,
                node.nodeType(),
                node.agentType(),
                null,
                status,
                branch,
                branchId,
                null,
                null,
                1,
                properties.orchestration().defaultMaxAttempts(),
                properties.orchestration().defaultTimeoutMs(),
                status == CheckpointStatus.IN_PROGRESS ? clock.instant() : null,
                completedAt
        ));
    }

    /**
     * Persists the progress of a running region LOOP as an IN_PROGRESS row for
     * the current iteration. The loop stays routing-only but gets one
     * its own, observable row per iteration (unique key execution/step/iteration); a re-arm creates
     * the next one without overwriting the previous.
     */
    private void writeLoopProgress(WorkflowExecutionRow execution, WorkflowDefinition dag,
                                   RoutingEvaluator.LoopProgress loop) {
        DagNode node = dag.byIdOrNull(loop.stepId());
        if (node == null) {
            return;
        }
        checkpoints.insertIfAbsent(new CheckpointInsert(
                UUID.randomUUID(),
                execution.id(),
                loop.stepId(),
                loop.iteration(),
                node.nodeType(),
                node.agentType(),
                null,
                CheckpointStatus.IN_PROGRESS,
                null,
                null,
                null,
                null,
                1,
                properties.orchestration().defaultMaxAttempts(),
                properties.orchestration().defaultTimeoutMs(),
                clock.instant(),
                null
        ));
    }

    private boolean isCompletedLike(CheckpointStatus status) {
        return status == CheckpointStatus.COMPLETED
                || status == CheckpointStatus.SKIPPED
                || status == CheckpointStatus.PRUNED
                || status == CheckpointStatus.CANCELLED;
    }

    private String finalOutputJson(Map<String, Object> context) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("context", new TreeMap<>(context));
        return EventJson.write(output);
    }

    private Map<String, Object> readMap(String json) {
        if (json == null || json.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, Object> map = EventJson.read(json, Map.class);
            return map == null ? new LinkedHashMap<>() : map;
        } catch (RuntimeException ex) {
            return new LinkedHashMap<>();
        }
    }
}
