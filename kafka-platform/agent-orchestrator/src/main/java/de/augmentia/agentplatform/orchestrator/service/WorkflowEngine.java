package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.WorkflowStartEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.ResultStatus;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.condition.ExpressionEvaluator;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcCheckpointStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcHitlTicketStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import de.augmentia.agentplatform.orchestrator.metrics.OrchestratorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Orchestrates the lifecycle of a workflow execution.
 *
 * <p>This class is deliberately kept <em>narrow</em>. It takes Kafka events,
 * locks the execution row and decides based on the reported status
 * what is to be done, and delegates the work:
 *
 * <ul>
 *   <li>{@link RoutingAdvancer} — evaluate routing, write checkpoints, fire</li>
 *   <li>{@link TaskDispatchCoordinator} — build and publish the task event, approval gate</li>
 *   <li>{@link ExecutionCanceller} — cancellation across all checkpoints</li>
 * </ul>
 *
 * <p>All public methods are {@code @Transactional} and first lock the execution
 * row. That protects against concurrent result events from several orchestrator
 * instances, regardless of how many replicas
 * are running.
 *
 * @see SweepReaperService — the periodic reaper that cleans up stuck
 *      executions and expired HITL tickets
 */
@Service
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    /** Placeholder step for tickets that are not assigned to a concrete node. */
    private static final String WORKFLOW_PLACEHOLDER_STEP = "__workflow__";

    private final JdbcWorkflowExecutionStore executions;
    private final JdbcCheckpointStore checkpoints;
    private final JdbcHitlTicketStore ticketStore;
    private final WorkflowDefinitionService definitions;
    private final HitlTicketService hitlTickets;
    private final RoutingAdvancer routingAdvancer;
    private final TaskDispatchCoordinator dispatchCoordinator;
    private final ExecutionCanceller canceller;
    private final DlqService dlqService;
    private final OrchestratorEventPublisher publisher;
    private final OrchestratorProperties properties;
    private final OrchestratorMetrics metrics;
    private final Clock clock;

    public WorkflowEngine(JdbcWorkflowExecutionStore executions,
                          JdbcCheckpointStore checkpoints,
                          JdbcHitlTicketStore ticketStore,
                          WorkflowDefinitionService definitions,
                          HitlTicketService hitlTickets,
                          RoutingAdvancer routingAdvancer,
                          TaskDispatchCoordinator dispatchCoordinator,
                          ExecutionCanceller canceller,
                          DlqService dlqService,
                          OrchestratorEventPublisher publisher,
                          OrchestratorProperties properties,
                          OrchestratorMetrics metrics,
                          Clock clock) {
        this.executions = executions;
        this.checkpoints = checkpoints;
        this.ticketStore = ticketStore;
        this.definitions = definitions;
        this.hitlTickets = hitlTickets;
        this.routingAdvancer = routingAdvancer;
        this.dispatchCoordinator = dispatchCoordinator;
        this.canceller = canceller;
        this.dlqService = dlqService;
        this.publisher = publisher;
        this.properties = properties;
        this.metrics = metrics;
        this.clock = clock;
    }

    // ── Eingehende Steuerereignisse ──────────────────────────────────

    @Transactional
    public void onIntake(WorkflowStartEvent event) {
        UUID executionId = parseUuidOrNull(event.workflowExecutionId(), "workflowExecutionId");
        if (executionId == null) {
            return;
        }
        WorkflowExecutionRow execution = executions.lock(executionId).orElse(null);
        if (execution == null || execution.status().isTerminal()) {
            return;
        }
        if (execution.status() == ExecutionStatus.PENDING) {
            executions.markRunning(executionId);
        }
        routingAdvancer.advance(execution);
    }

    /**
     * Progress for an execution without an incoming event — for example after a
     * sweep recovery. Repeated calls are harmless: already terminal
     * checkpoints are not overwritten, already IN_PROGRESS ones are not fired again.
     */
    @Transactional
    public void advance(UUID executionId) {
        WorkflowExecutionRow execution = executions.lock(executionId).orElse(null);
        if (execution == null || execution.status().isTerminal()) {
            return;
        }
        routingAdvancer.advance(execution);
    }

    @Transactional
    public void onResult(ResultEvent event) {
        UUID taskId = parseUuidOrNull(event.taskId(), "taskId");
        if (taskId == null) {
            // A result we cannot identify is not actionable and must not become DLQ noise:
            // it never belonged to a task this orchestrator dispatched.
            metrics.staleResultTotal("invalid_task_id").increment();
            return;
        }
        CheckpointRow checkpoint = checkpoints.findByTaskId(taskId).orElse(null);
        if (checkpoint == null) {
            metrics.staleResultTotal("no_checkpoint").increment();
            dlqService.send(TopicNames.RESULTS, event.workflowId(), event,
                    new IllegalArgumentException("Unknown task id: " + taskId));
            return;
        }

        WorkflowExecutionRow execution = executions.lock(checkpoint.workflowExecutionId()).orElse(null);
        if (execution == null || execution.status().isTerminal()) {
            return;
        }
        CheckpointRow latest = checkpoints.latest(execution.id(), checkpoint.stepId()).orElse(null);
        if (latest == null) {
            metrics.staleResultTotal("no_checkpoint").increment();
            log.debug("Dropping result for taskId={}: no checkpoint for step '{}'", taskId, checkpoint.stepId());
            return;
        }
        if (!latest.id().equals(checkpoint.id()) || latest.status() != CheckpointStatus.IN_PROGRESS) {
            metrics.staleResultTotal(latest.id().equals(checkpoint.id()) ? "not_in_progress" : "superseded").increment();
            log.debug("Dropping result for taskId={}: step '{}' is at checkpoint {} status {}, not this one in status {}",
                taskId, checkpoint.stepId(), latest.id(), latest.status(), CheckpointStatus.IN_PROGRESS);
            return;
        }

        WorkflowDefinition dag = definitions.loadDefinition(execution.workflowDefinitionId());
        DagNode node = dag.node(latest.stepId());
        Map<String, Object> resultData = event.resultData() == null ? Map.of() : event.resultData();
        String outputJson = EventJson.write(resultData);
        String errorMessage = resultData.get("errorMessage") instanceof String e ? e : null;
        ResultStatus status = resolveStatus(resultData.get("status"));

        switch (status) {
            case COMPLETED -> handleCompleted(execution, dag, node, latest, resultData, outputJson);
            case FAILED_RETRYABLE -> {
                metrics.retryAttemptsTotal(node.agentType()).increment();
                retryOrEscalate(execution, dag, node, latest,
                        errorMessage == null ? "Agent reported retryable failure" : errorMessage, event);
            }
            case FAILED_FATAL -> {
                checkpoints.markFailed(latest.id(), errorMessage);
                dlqService.send(TopicNames.RESULTS, execution.id().toString(), event,
                        new IllegalStateException("Agent reported fatal failure"));
                createTicket(execution, latest, "FAILED_FATAL", Map.of("errorMessage", nullSafe(errorMessage)));
                executions.markPausedHitl(execution.id());
            }
            case NEEDS_HITL -> {
                checkpoints.markWaitingHitl(latest.id(), "needs-hitl");
                createTicket(execution, latest, "NEEDS_HITL", Map.of("errorMessage", nullSafe(errorMessage)));
                executions.markPausedHitl(execution.id());
            }
        }
    }

    private void handleCompleted(WorkflowExecutionRow execution, WorkflowDefinition dag, DagNode node,
                                 CheckpointRow latest, Map<String, Object> resultData, String outputJson) {
        if (node.nodeType() == NodeType.LOOP) {
            // the loop node itself is not done yet: first the exit condition
            // decides whether another iteration follows.
            checkpoints.saveOutput(latest.id(), outputJson);
            metrics.loopIterationTotal(node.stepId()).increment();
            Map<String, Object> context = checkpoints.outputContext(execution.id());
            Map<String, Object> input = readMap(execution.inputParamsJson());
            boolean exit = node.exitCondition() != null
                    && ExpressionEvaluator.eval(node.exitCondition(), context, input);
            int maxIterations = node.maxIterations() == null ? 1 : node.maxIterations();
            ControlEvents.LoopEvent loopEvent = new ControlEvents.LoopEvent(
                    execution.id().toString(),
                    node.stepId(),
                    latest.iteration(),
                    exit,
                    maxIterations,
                    resultData
            );
            publisher.publishAfterCommit(TopicNames.LOOP, execution.id().toString(), EventJson.write(loopEvent));
        } else {
            checkpoints.complete(latest.id(), outputJson);
            routingAdvancer.advance(execution);
        }
    }

    /**
     * Resolves the reported result status.
     *
     * <p><b>Fail-closed:</b> an unknown or missing status is
     * <em>not</em> interpreted as {@code COMPLETED}. The status comes from
     * an LLM JSON payload and is therefore untrusted: a truncated block,
     * a typo or a hallucinated value would otherwise mark a workflow
     * as successful without a single step having run. Instead
     * {@code NEEDS_HITL} is chosen — a human decides.
     */
    private ResultStatus resolveStatus(Object raw) {
        if (raw instanceof String s) {
            try {
                return ResultStatus.valueOf(s);
            } catch (IllegalArgumentException ignored) {
                log.warn("Unknown result status '{}' — failing closed to NEEDS_HITL", s);
                return ResultStatus.NEEDS_HITL;
            }
        }
        log.warn("Missing or non-string result status (raw={}) — failing closed to NEEDS_HITL", raw);
        return ResultStatus.NEEDS_HITL;
    }

    @Transactional
    public void onLoop(ControlEvents.LoopEvent event) {
        UUID executionId = parseUuidOrNull(event.workflowId(), "workflowId");
        if (executionId == null) {
            return;
        }
        WorkflowExecutionRow execution = executions.lock(executionId).orElse(null);
        if (execution == null || execution.status().isTerminal()) {
            return;
        }
        CheckpointRow latest = checkpoints.latest(executionId, event.stepId()).orElse(null);
        if (latest == null || latest.iteration() != event.iteration() || latest.status() != CheckpointStatus.IN_PROGRESS) {
            return;
        }

        WorkflowDefinition dag = definitions.loadDefinition(execution.workflowDefinitionId());
        DagNode node = dag.node(event.stepId());

        if (event.exitConditionTrue()) {
            checkpoints.complete(latest.id(), latest.outputPayloadJson());
            metrics.loopIterationTotal(event.stepId()).increment();
            metrics.loopExitReasonTotal("exit-condition").increment();
            routingAdvancer.advance(execution);
            return;
        }

        if (latest.iteration() < event.maxIterations()) {
            int nextIteration = latest.iteration() + 1;
            checkpoints.complete(latest.id(), latest.outputPayloadJson());
            metrics.loopIterationTotal(event.stepId()).increment();
            dispatchCoordinator.dispatchIteration(execution, dag, node, nextIteration, 1, null);
            return;
        }

        checkpoints.markWaitingHitl(latest.id(), "loop-max-iterations");
        createTicket(execution, latest, "LOOP_MAX_ITERATIONS", Map.of(
                "iteration", latest.iteration(),
                "maxIterations", event.maxIterations()
        ));
        executions.markPausedHitl(executionId);
        metrics.loopExitReasonTotal("max-iterations").increment();
    }

    @Transactional
    public void onCancellation(ControlEvents.CancellationEvent event) {
        UUID executionId = parseUuidOrNull(event.workflowId(), "workflowId");
        if (executionId == null) {
            return;
        }
        WorkflowExecutionRow execution = executions.lock(executionId).orElse(null);
        if (execution == null || execution.status().isTerminal()) {
            return;
        }
        canceller.cancel(execution, event.reason(), event.actor());
    }

    @Transactional
    public void onHitl(ControlEvents.HitlEvent event) {
        UUID ticketId = parseUuidOrNull(event.ticketId(), "ticketId");
        if (ticketId == null) {
            return;
        }
        HitlTicketRow ticket = ticketStore.findById(ticketId).orElse(null);
        if (ticket == null || ticket.status() != HitlTicketStatus.OPEN) {
            return;
        }
        WorkflowExecutionRow execution = executions.lock(ticket.workflowExecutionId()).orElse(null);
        if (execution == null) {
            return;
        }
        CheckpointRow latest = checkpoints.latest(execution.id(), ticket.stepId()).orElse(null);

        switch (event.action()) {
            case APPROVED, RETRY -> {
                ticketStore.resolve(ticketId, HitlTicketStatus.APPROVED, event.decidedBy(), "Human resume");
                if (latest != null) {
                    DagNode node = definitions.loadDefinition(execution.workflowDefinitionId()).node(latest.stepId());
                    if (latest.status() == CheckpointStatus.WAITING_HITL) {
                        dispatchCoordinator.resumeExistingCheckpoint(execution, node, latest,
                                latest.attempt(), false, "Human resume");
                    } else if (latest.status() == CheckpointStatus.FAILED) {
                        metrics.retryAttemptsTotal(node.agentType()).increment();
                        dispatchCoordinator.resumeExistingCheckpoint(execution, node, latest,
                                latest.attempt() + 1, true, "Human retry");
                    }
                }
                executions.markRunning(execution.id());
                routingAdvancer.advance(execution);
            }
            case SKIP -> {
                ticketStore.resolve(ticketId, HitlTicketStatus.SKIPPED, event.decidedBy(), "Human skip");
                if (latest != null && latest.status() != CheckpointStatus.COMPLETED && !latest.status().isDead()) {
                    checkpoints.updateRoutingStatus(latest.id(), CheckpointStatus.SKIPPED, "hitl:skip", clock.instant());
                }
                executions.markRunning(execution.id());
                routingAdvancer.advance(execution);
            }
            case ABORT -> {
                ticketStore.resolve(ticketId, HitlTicketStatus.ABORTED, event.decidedBy(), "Human abort");
                canceller.cancel(execution, "HITL abort", event.decidedBy());
            }
        }
    }

    // ── Sweep-Pfade ──────────────────────────────────────────────────

    /**
     * An IN_PROGRESS checkpoint without a result counts as orphaned — typically
     * because the worker crashed. The step is retried; if it is
     * markiert, eskaliert {@link #retryOrEscalate}.
     */
    @Transactional
    public void handleStaleCheckpoint(UUID checkpointId) {
        CheckpointRow stale = checkpoints.findById(checkpointId).orElse(null);
        if (stale == null || stale.status() != CheckpointStatus.IN_PROGRESS) {
            return;
        }
        WorkflowExecutionRow execution = executions.lock(stale.workflowExecutionId()).orElse(null);
        if (execution == null || execution.status().isTerminal()) {
            return;
        }
        CheckpointRow latest = checkpoints.latest(execution.id(), stale.stepId()).orElse(null);
        if (latest == null || !latest.id().equals(stale.id()) || latest.status() != CheckpointStatus.IN_PROGRESS) {
            return;
        }
        WorkflowDefinition dag = definitions.loadDefinition(execution.workflowDefinitionId());
        DagNode node = dag.node(latest.stepId());
        if (node != null && node.nodeType() == NodeType.LOOP) {
            metrics.loopIterationTotal(latest.stepId()).increment();
        }
        retryOrEscalate(execution, dag, node, latest, "Task timeout detected by sweep", null);
    }

    @Transactional
    public void pauseStalledExecution(UUID executionId, String reason) {
        WorkflowExecutionRow execution = executions.lock(executionId).orElse(null);
        if (execution == null || execution.status().isTerminal() || execution.status() == ExecutionStatus.PAUSED_HITL) {
            return;
        }
        hitlTickets.createIfAbsent(executionId, null, WORKFLOW_PLACEHOLDER_STEP, 1, reason, Map.of(
                "executionId", executionId.toString(),
                "reason", reason
        ));
        executions.markPausedHitl(executionId);
    }

    /**
     * Escalates an expired ticket. If it reaches {@code maxEscalations} it is
     * resolved and the execution stays paused — a repeatedly timed-out
     * ticket without reaction must not escalate forever.
     */
    @Transactional
    public void expireTicket(UUID ticketId) {
        HitlTicketRow ticket = ticketStore.findById(ticketId).orElse(null);
        if (ticket == null || ticket.status() != HitlTicketStatus.OPEN) {
            return;
        }
        WorkflowExecutionRow execution = executions.lock(ticket.workflowExecutionId()).orElse(null);
        if (execution == null) {
            return;
        }
        int nextEscalation = ticket.escalationCount() + 1;
        if (nextEscalation >= properties.hitl().maxEscalations()) {
            ticketStore.resolve(ticketId, HitlTicketStatus.RESOLVED, "system", "Expired after max escalations");
            executions.markPausedHitl(execution.id());
        } else {
            ticketStore.escalate(ticketId, nextEscalation,
                    clock.instant().plus(java.time.Duration.ofHours(properties.hitl().ttlHours())));
        }
    }

    /**
     * Repeats a failed step or escalates when the attempts
     * aufgebraucht sind.
     */
    private void retryOrEscalate(WorkflowExecutionRow execution, WorkflowDefinition dag, DagNode node,
                                 CheckpointRow latest, String errorMessage, Object originalEvent) {
        int nextAttempt = latest.attempt() + 1;
        if (nextAttempt <= latest.maxAttempts()) {
            dispatchCoordinator.resumeExistingCheckpoint(execution, node, latest, nextAttempt, true, errorMessage);
            return;
        }

        checkpoints.markFailed(latest.id(), errorMessage);
        dlqService.send(TopicNames.RESULTS, execution.id().toString(),
                originalEvent == null ? Map.of(
                        "stepId", latest.stepId(),
                        "iteration", latest.iteration(),
                        "errorMessage", nullSafe(errorMessage)
                ) : originalEvent,
                new IllegalStateException("Retries exhausted: " + errorMessage));
        createTicket(execution, latest, "RETRIES_EXHAUSTED", Map.of("errorMessage", nullSafe(errorMessage)));
        executions.markPausedHitl(execution.id());
    }

    // ── Queries and helpers ───────────────────────────────────────────

    private UUID createTicket(WorkflowExecutionRow execution, CheckpointRow checkpoint, String reason,
                              Map<String, Object> extra) {
        Map<String, Object> context = new LinkedHashMap<>(extra);
        context.put("executionId", execution.id().toString());
        context.put("status", checkpoint.status().name());
        context.put("output", parseJsonOrRaw(checkpoint.outputPayloadJson()));
        return hitlTickets.createIfAbsent(
                execution.id(),
                checkpoint.id(),
                checkpoint.stepId(),
                checkpoint.iteration(),
                reason,
                context
        );
    }

    private Object parseJsonOrRaw(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return EventJson.read(json, Map.class);
        } catch (RuntimeException ex) {
            return json;
        }
    }

    private Map<String, Object> readMap(String json) {
        if (json == null || json.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = EventJson.read(json, Map.class);
            return map == null ? new LinkedHashMap<>() : map;
        } catch (RuntimeException ex) {
            log.warn("Unable to parse JSON map, using empty map", ex);
            return new LinkedHashMap<>();
        }
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    /**
     * Parses a UUID from an untrusted event field.
     *
     * <p>Returns {@code null} for a missing, blank or malformed value. The callers then ignore
     * the event with a clear warning instead of throwing — a thrown {@link IllegalArgumentException}
     * would be caught by the Kafka adapter and turned into a DLQ entry, i.e. self-inflicted noise
     * for a message the orchestrator could never map to a checkpoint anyway.
     */
    private static UUID parseUuidOrNull(String raw, String fieldName) {
        if (raw == null || raw.isBlank()) {
            log.warn("Ignoring event with missing {}", fieldName);
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ex) {
            log.warn("Ignoring event with invalid {}: '{}'", fieldName, raw);
            return null;
        }
    }

    public Optional<WorkflowExecutionRow> findExecution(UUID executionId) {
        return executions.findById(executionId);
    }

    public List<CheckpointRow> findCheckpoints(UUID executionId) {
        return checkpoints.findByExecution(executionId);
    }

}
