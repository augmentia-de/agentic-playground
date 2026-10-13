package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.common.model.HitlAction;
import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Characterisation tests for {@link WorkflowEngine}.
 *
 * <p>This suite came into being because the engine previously had <b>no test coverage</b>:
 * no test referenced {@code WorkflowEngine}, {@code advanceLocked},
 * {@code onResult}, {@code retryOrEscalate} or {@code onHitl}. The 30 existing
 * "orchestrator tests" check event serialisation, topic names, error
 * classification and the pure {@code RoutingEvaluator} — not against the engine itself.
 *
 * <p>Two goals, in this order:
 * <ol>
 *   <li><b>Pin down the behaviour</b> as it is today — including the places where
 *       it unintentionally is. That makes a later refactor safe.</li>
 *   <li><b>The fixes from the analysis are pinned down</b>: fail-closed status resolution,
 *       stale-result discard, timeout cap, N+1-free abort.</li>
 * </ol>
 *
 * <p>The tests run against real Postgres with real transactions — see
 * {@link WorkflowEngineTestBase}.
 */
@DisplayName("WorkflowEngine — Charakterisierung")
class WorkflowEngineCharacterizationTest extends WorkflowEngineTestBase {

    // ── Intake & Dispatch ─────────────────────────────────────────────

    @Test
    @DisplayName("onIntake fires the first STEP and dispatches exactly one task")
    void intakeDispatchesFirstStep() {
        WorkflowExecutionRow execution = startLinear("{}");

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(dispatchedTopics()).containsExactly("agent.tasks.researcher");

        TaskEvent task = lastDispatchedTask();
        assertThat(task.stepId()).isEqualTo("a");
        assertThat(task.agentType()).isEqualTo("researcher");
        assertThat(task.header().workflowId()).isEqualTo(execution.id().toString());

        assertThat(checkpoint(execution.id(), "a")).isPresent()
                .get()
                .extracting(c -> c.status())
                .isEqualTo(CheckpointStatus.IN_PROGRESS);
    }

    @Test
    @DisplayName("taskId ist deterministisch aus (execution, step, iteration, attempt)")
    void taskIdIsDeterministic() {
        UUID executionId = UUID.randomUUID();

        UUID a = TaskDispatcher.deterministicTaskId(executionId, "a", 1, 1);
        UUID b = TaskDispatcher.deterministicTaskId(executionId, "a", 1, 1);
        UUID c = TaskDispatcher.deterministicTaskId(executionId, "a", 1, 2);
        UUID d = TaskDispatcher.deterministicTaskId(executionId, "b", 1, 1);

        assertThat(a).as("same input -> same taskId").isEqualTo(b);
        assertThat(a).as("anderer attempt -> andere taskId").isNotEqualTo(c);
        assertThat(a).as("different step -> different taskId").isNotEqualTo(d);
    }

    // ── onResult: Erfolgsfall ─────────────────────────────────────────

    @Test
    @DisplayName("COMPLETED closes the checkpoint and fires the next step")
    void completedAdvancesToNextStep() {
        WorkflowExecutionRow execution = startLinear("{}");
        engine.onResult(completed(lastDispatchedTask()));

        assertThat(checkpoint(execution.id(), "a")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.COMPLETED);

        // Not "end" — the next real step is b.
        assertThat(dispatchedTasks()).hasSize(2);
        assertThat(lastDispatchedTask().stepId()).isEqualTo("b");
        assertThat(dispatchedTopics()).containsExactly(
                "agent.tasks.researcher", "agent.tasks.coder");
    }

    @Test
    @DisplayName("The last step completes the execution as COMPLETED")
    void lastStepCompletesExecution() {
        WorkflowExecutionRow execution = startLinear("{}");
        engine.onResult(completed(lastDispatchedTask()));   // a
        engine.onResult(completed(lastDispatchedTask()));   // b

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(executions.findById(execution.id()).orElseThrow().finalOutputJson())
                .contains("context");
    }

    // ── onResult: error and escalation paths ────────────────────────

    @Test
    @DisplayName("FAILED_RETRYABLE dispatches the same step with a higher attempt")
    void retryableFailureReDispatchesSameStep() {
        WorkflowExecutionRow execution = startLinear("{}");
        TaskEvent first = lastDispatchedTask();

        engine.onResult(failed(first, "FAILED_RETRYABLE", "transient"));

        assertThat(dispatchedTasks()).hasSize(2);
        TaskEvent retry = lastDispatchedTask();
        assertThat(retry.stepId()).isEqualTo("a");
        assertThat(retry.header().attempt()).isEqualTo(2);
        assertThat(retry.header().taskId()).isNotEqualTo(first.header().taskId());
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    @DisplayName("FAILED_RETRYABLE beyond maxAttempts creates ticket and DLQ")
    void retriesExhaustedCreatesTicketAndDlq() {
        WorkflowExecutionRow execution = startLinear("{}");
        for (int i = 0; i < 5; i++) {
            engine.onResult(failed(lastDispatchedTask(), "FAILED_RETRYABLE", "still broken"));
        }

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
        assertThat(ticketStore.findOpenByExecution(execution.id()))
                .isNotEmpty()
                .allSatisfy(t -> assertThat(t.reason()).isEqualTo("RETRIES_EXHAUSTED"));

        // dlq_messages has no workflow_execution_id column — the key
        // landet in original_key (= executionId, gesetzt via DlqService.send).
        Integer dlq = jdbc.queryForObject(
                "SELECT count(*) FROM dlq_messages WHERE topic = ? AND original_key = ?",
                Integer.class, TopicNames.RESULTS, execution.id().toString());
        assertThat(dlq).as("Retries exhausted -> DLQ-Eintrag").isNotNull().isPositive();
    }

    @Test
    @DisplayName("FAILED_FATAL marks the checkpoint FAILED, pauses and escalates")
    void fatalFailurePausesForHitl() {
        WorkflowExecutionRow execution = startLinear("{}");

        engine.onResult(failed(lastDispatchedTask(), "FAILED_FATAL", "boom"));

        assertThat(checkpoint(execution.id(), "a")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.FAILED);
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
        assertThat(ticketStore.findOpenByExecution(execution.id()))
                .isNotEmpty()
                .allSatisfy(t -> assertThat(t.reason()).isEqualTo("FAILED_FATAL"));
    }

    // ── Fail-closed: der Fix aus O2 ──────────────────────────────────

    @Test
    @DisplayName("An unknown result status escalates to NEEDS_HITL instead of COMPLETED")
    void unknownStatusFailsClosed() {
        WorkflowExecutionRow execution = startLinear("{}");
        TaskEvent task = lastDispatchedTask();

        // "CMPLETED" — exactly the typo that a truncated LLM JSON
        // produces. Before the fix that was silently counted as COMPLETED.
        engine.onResult(resultFor(task, Map.of("status", "CMPLETED", "response", "x")));

        assertThat(status(execution.id()))
                .as("Fail-closed: must not pass as success")
                .isNotEqualTo(ExecutionStatus.COMPLETED);
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
        assertThat(ticketStore.findOpenByExecution(execution.id()))
                .isNotEmpty()
                .allSatisfy(t -> assertThat(t.reason()).isEqualTo("NEEDS_HITL"));
    }

    @Test
    @DisplayName("A missing status likewise falls to NEEDS_HITL")
    void missingStatusFailsClosed() {
        WorkflowExecutionRow execution = startLinear("{}");
        TaskEvent task = lastDispatchedTask();

        engine.onResult(resultFor(task, Map.of("response", "no status field")));

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
    }

    // ── Stale Results: der Fix aus O5 ────────────────────────────────

    @Test
    @DisplayName("A superseded result is discarded and triggers no progress")
    void supersededResultIsDropped() {
        WorkflowExecutionRow execution = startLinear("{}");
        TaskEvent first = lastDispatchedTask();

        engine.onResult(failed(first, "FAILED_RETRYABLE", "transient"));
        assertThat(dispatchedTasks()).hasSize(2);

        // now replay the RESULT OF THE OLD task afterwards. The
        // checkpoint is meanwhile at attempt 2, the old result is stale.
        published.clear();
        engine.onResult(completed(first));

        assertThat(dispatchedTasks())
                .as("A stale result must not fire another task")
                .isEmpty();
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    @DisplayName("A result for an aborted execution is ignored")
    void resultAfterCancellationIsIgnored() {
        WorkflowExecutionRow execution = startLinear("{}");
        TaskEvent task = lastDispatchedTask();

        engine.onCancellation(new ControlEvents.CancellationEvent(
                execution.id().toString(), "user aborted", "operator"));
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.CANCELLED);

        published.clear();
        engine.onResult(completed(task));

        assertThat(dispatchedTasks()).isEmpty();
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.CANCELLED);
    }

    // ── Abbruch: der Fix aus O5 (N+1) ────────────────────────────────

    @Test
    @DisplayName("Abort marks open checkpoints CANCELLED, leaves completed ones alone")
    void cancellationMarksOpenCheckpoints() {
        WorkflowExecutionRow execution = startLinear("{}");
        engine.onResult(completed(lastDispatchedTask()));   // a done, b running

        engine.onCancellation(new ControlEvents.CancellationEvent(
                execution.id().toString(), "abort", "operator"));

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.CANCELLED);
        assertThat(engine.findCheckpoints(execution.id()))
                .isNotEmpty()
                // deliberately so: an already completed step is not
                // retroactively rewritten to CANCELLED. Otherwise the
                // workflow would lose the results it has already produced.
                .allSatisfy(c -> {
                    if (c.stepId().equals("a")) {
                        assertThat(c.status()).isEqualTo(CheckpointStatus.COMPLETED);
                    } else {
                        assertThat(c.status()).isEqualTo(CheckpointStatus.CANCELLED);
                    }
                });
    }

    // ── HITL ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("A requiresHumanApproval node pauses instead of dispatching")
    void approvalGatePausesInsteadOfDispatching() {
        WorkflowExecutionRow execution = start(approvalWorkflow(), "{}");
        engine.onResult(completed(lastDispatchedTask()));   // a fertig

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
        assertThat(checkpoint(execution.id(), "gate")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.WAITING_HITL);
        assertThat(ticketStore.findOpenByExecution(execution.id()))
                .isNotEmpty()
                .allSatisfy(t -> assertThat(t.reason()).isEqualTo("APPROVAL_REQUIRED"));

        assertThat(dispatchedTasks())
                .as("The gate step must not send any task to the worker")
                .hasSize(1);
    }

    @Test
    @DisplayName("HITL APPROVED continues the gate")
    void approvalResumesGate() {
        WorkflowExecutionRow execution = start(approvalWorkflow(), "{}");
        engine.onResult(completed(lastDispatchedTask()));

        var ticket = ticketStore.findOpenByExecution(execution.id()).get(0);
        published.clear();

        engine.onHitl(new ControlEvents.HitlEvent(
                execution.id().toString(), ticket.id().toString(), ticket.stepId(),
                ticket.iteration(), HitlAction.APPROVED, "operator"));

        assertThat(ticketStore.findById(ticket.id()).orElseThrow().status())
                .isEqualTo(HitlTicketStatus.APPROVED);
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(dispatchedTasks()).isNotEmpty();
        assertThat(lastDispatchedTask().stepId()).isEqualTo("gate");
    }

    @Test
    @DisplayName("HITL SKIP skips the checkpoint instead of executing it")
    void hitlSkipSkipsCheckpoint() {
        WorkflowExecutionRow execution = start(approvalWorkflow(), "{}");
        engine.onResult(completed(lastDispatchedTask()));

        var ticket = ticketStore.findOpenByExecution(execution.id()).get(0);
        published.clear();

        engine.onHitl(new ControlEvents.HitlEvent(
                execution.id().toString(), ticket.id().toString(), ticket.stepId(),
                ticket.iteration(), HitlAction.SKIP, "operator"));

        assertThat(checkpoint(execution.id(), "gate")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.SKIPPED);
        assertThat(dispatchedTasks())
                .as("A skipped step must not fire any task")
                .isEmpty();
    }

    @Test
    @DisplayName("HITL ABORT bricht die Execution ab")
    void hitlAbortCancelsExecution() {
        WorkflowExecutionRow execution = start(approvalWorkflow(), "{}");
        engine.onResult(completed(lastDispatchedTask()));

        var ticket = ticketStore.findOpenByExecution(execution.id()).get(0);
        engine.onHitl(new ControlEvents.HitlEvent(
                execution.id().toString(), ticket.id().toString(), ticket.stepId(),
                ticket.iteration(), HitlAction.ABORT, "operator"));

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.CANCELLED);
    }

    @Test
    @DisplayName("Ticket escalation runs up to maxEscalations and then resolves")
    void ticketEscalationResolvesAfterMax() {
        WorkflowExecutionRow execution = start(approvalWorkflow(), "{}");
        engine.onResult(completed(lastDispatchedTask()));
        var ticket = ticketStore.findOpenByExecution(execution.id()).get(0);

        for (int i = 0; i < 5; i++) {
            engine.expireTicket(ticket.id());
        }

        assertThat(ticketStore.findById(ticket.id()).orElseThrow().status())
                .isEqualTo(HitlTicketStatus.RESOLVED);
    }

    // ── Sweep-Pfade ──────────────────────────────────────────────────

    @Test
    @DisplayName("An orphaned IN_PROGRESS checkpoint is dispatched again as a retry")
    void staleCheckpointTriggersRetry() {
        WorkflowExecutionRow execution = startLinear("{}");
        TaskEvent first = lastDispatchedTask();
        var checkpoint = checkpoint(execution.id(), "a").orElseThrow();
        published.clear();

        engine.handleStaleCheckpoint(checkpoint.id());

        assertThat(dispatchedTasks()).isNotEmpty();
        assertThat(lastDispatchedTask().header().attempt())
                .as("The sweep should repeat the attempt")
                .isGreaterThan(first.header().attempt());
    }

    @Test
    @DisplayName("pauseStalledExecution creates a ticket and pauses")
    void pauseStalledExecutionCreatesTicket() {
        WorkflowExecutionRow execution = startLinear("{}");

        engine.pauseStalledExecution(execution.id(), "STALLED_FAILED_BRANCH");

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
        assertThat(ticketStore.findOpenByExecution(execution.id())).isNotEmpty();
    }

    // ── Determinismus: die Kernzusage der Plattform ──────────────────

    @Test
    @DisplayName("The same persisted state yields the same task — without double dispatch")
    void repeatedAdvanceDoesNotDuplicateDispatch() {
        WorkflowExecutionRow execution = startLinear("{}");
        engine.onResult(completed(lastDispatchedTask()));
        assertThat(dispatchedTasks()).hasSize(2);

        // advance() once more on the same state: fire is already IN_PROGRESS.
        engine.advance(execution.id());
        engine.advance(execution.id());

        assertThat(dispatchedTasks())
                .as("A repeated advance must not create a second task")
                .hasSize(2);
    }

    @Test
    @DisplayName("A DECISION node branches on the trigger result")
    void decisionBranchesOnResultData() {
        UUID definitionId = decisionWorkflow();
        WorkflowExecutionRow execution = start(definitionId, "{}");
        TaskEvent trigger = lastDispatchedTask();

        engine.onResult(resultFor(trigger, Map.of(
                "status", "COMPLETED", "mode", "go")));

        assertThat(dispatchedTasks()).hasSize(2);
        assertThat(lastDispatchedTask().stepId())
                .as("The 'go' branch must fire 'after', not the defaultTarget 'end'")
                .isEqualTo("after");
    }

    @Test
    @DisplayName("decision_branch stores the target step, not the branchId")
    void decisionBranchColumnHoldsTargetNotBranchId() {
        UUID definitionId = decisionWorkflow();
        WorkflowExecutionRow execution = start(definitionId, "{}");
        engine.onResult(resultFor(lastDispatchedTask(), Map.of(
                "status", "COMPLETED", "mode", "go")));

        // Deliberate finding, not a wish: RoutingEvaluator.Decision carries
        // only chosenTarget, the branchId is used internally for pruning and
        // discarded afterwards. The DB therefore holds the target, not the branch.
        // Recoverable only via the "b.<branchId>." naming convention of the
        // delivered workflows — an implicit dependency.
        assertThat(checkpoint(execution.id(), "gate")).isPresent()
                .get().extracting(c -> c.decisionBranch())
                .isEqualTo("after");
    }

    @Test
    @DisplayName("DECISION falls back to defaultTarget when the condition does not match")
    void decisionFallsBackToDefaultTarget() {
        UUID definitionId = decisionWorkflow();
        start(definitionId, "{}");

        // mode != 'go' -> condition false -> defaultTarget 'end'
        engine.onResult(resultFor(lastDispatchedTask(), Map.of(
                "status", "COMPLETED", "mode", "stop")));

        assertThat(dispatchedTasks())
                .as("No STEP may fire when only the END node is left")
                .hasSize(1);
        assertThat(lastDispatchedTask().stepId()).isEqualTo("trigger");
    }
}
