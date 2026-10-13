package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.orchestrator.db.JdbcDlqStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcHitlTicketStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointInsert;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Calls every writing method of the JDBC stores against the real schema.
 *
 * <p>The purpose is not functionality — the characterisation tests check that —
 * but <em>parameter binding</em>. In SQL with timestamps errors
 * of the kind "no value supplied for parameter 6" easily arise, which only
 * surface when exactly this method is called in the right state. That
 * cost a test run when the stores were switched to the application clock, because
 * exactly one path was affected.
 *
 * <p>A test that executes every write method once makes that permanently
 * part of the runtime of the test run instead of leaving it to time analysis.
 */
@DisplayName("Alle Store-Schreibmethoden sind aufrufbar")
class StoreWriteMethodsTest extends WorkflowEngineTestBase {

    @Autowired
    private JdbcWorkflowExecutionStore executionStore;

    @Autowired
    private JdbcHitlTicketStore ticketStore;

    @Autowired
    private JdbcDlqStore dlqStore;

    @Test
    @DisplayName("Execution: create, updateStatus, markRunning, markPausedHitl, markCompleted")
    void executionWriteMethods() {
        UUID id = UUID.randomUUID();
        executionStore.create(id, linearWorkflow(), "idem-" + id, "{}");

        executionStore.updateStatus(id, ExecutionStatus.PENDING);
        executionStore.markRunning(id);
        executionStore.markPausedHitl(id);
        executionStore.markCompleted(id, "{\"a\":1}");

        assertThat(executionStore.findById(id).orElseThrow().status())
                .isEqualTo(ExecutionStatus.COMPLETED);
    }

    @Test
    @DisplayName("Execution: markCancelled and markFailed")
    void executionTerminalMethods() {
        UUID cancelled = UUID.randomUUID();
        executionStore.create(cancelled, linearWorkflow(), "c-" + cancelled, "{}");
        executionStore.markCancelled(cancelled, "{\"reason\":\"test\"}");
        assertThat(executionStore.findById(cancelled).orElseThrow().status())
                .isEqualTo(ExecutionStatus.CANCELLED);

        UUID failed = UUID.randomUUID();
        executionStore.create(failed, linearWorkflow(), "f-" + failed, "{}");
        executionStore.markFailed(failed, "{\"reason\":\"test\"}");
        assertThat(executionStore.findById(failed).orElseThrow().status())
                .isEqualTo(ExecutionStatus.FAILED);
    }

    @Test
    @DisplayName("Checkpoint: insert, complete, saveOutput, dispatch, retry")
    void checkpointWriteMethods() {
        UUID executionId = startLinear("{}").id();
        UUID id = UUID.randomUUID();

        assertThat(checkpoints.insertIfAbsent(new CheckpointInsert(
                id, executionId, "step-a", 1,
                NodeType.STEP, "researcher", null,
                CheckpointStatus.PENDING, null, null, "{}", null,
                1, 3, 60_000L, NOW, null))).isTrue();

        checkpoints.dispatchNewTask(id, UUID.randomUUID(), 1, "{\"in\":1}", NOW);
        checkpoints.saveOutput(id, "{\"mid\":1}");
        checkpoints.complete(id, "{\"out\":1}");

        CheckpointRow row = checkpoints.findById(id).orElseThrow();
        assertThat(row.status()).isEqualTo(CheckpointStatus.COMPLETED);
        assertThat(row.outputPayloadJson()).contains("out");
    }

    @Test
    @DisplayName("Checkpoint: retryTask, markFailed, markCancelled, updateRoutingStatus")
    void checkpointStateTransitionMethods() {
        UUID executionId = startLinear("{}").id();
        UUID id = UUID.randomUUID();
        checkpoints.insertIfAbsent(new CheckpointInsert(
                id, executionId, "step-b", 1,
                NodeType.STEP, "researcher", null,
                CheckpointStatus.PENDING, null, null, "{}", null,
                1, 3, 60_000L, NOW, null));

        checkpoints.dispatchNewTask(id, UUID.randomUUID(), 1, "{}", NOW);
        checkpoints.retryTask(id, UUID.randomUUID(), 2, "{}", "transient", NOW);
        assertThat(checkpoints.findById(id).orElseThrow().attempt()).isEqualTo(2);
        assertThat(checkpoints.findById(id).orElseThrow().retryCount()).isEqualTo(1);

        // four-argument form (branchId is preserved) and five-argument form
        checkpoints.updateRoutingStatus(id, CheckpointStatus.SKIPPED, "hitl:skip", NOW);
        assertThat(checkpoints.findById(id).orElseThrow().decisionBranch()).isEqualTo("hitl:skip");

        checkpoints.updateRoutingStatus(id, CheckpointStatus.PRUNED, "b.other.task", "other", NOW);
        CheckpointRow pruned = checkpoints.findById(id).orElseThrow();
        assertThat(pruned.decisionBranch()).isEqualTo("b.other.task");
        assertThat(pruned.branchId())
                .as("branch_id is only set, not overwritten")
                .isEqualTo("other");

        checkpoints.markFailed(id, "boom");
        assertThat(checkpoints.findById(id).orElseThrow().status()).isEqualTo(CheckpointStatus.FAILED);

        UUID other = UUID.randomUUID();
        checkpoints.insertIfAbsent(new CheckpointInsert(
                other, executionId, "step-c", 1,
                NodeType.STEP, "coder", null,
                CheckpointStatus.IN_PROGRESS, null, null, "{}", null,
                1, 3, 60_000L, NOW, null));
        checkpoints.markCancelled(other);
        assertThat(checkpoints.findById(other).orElseThrow().status())
                .isEqualTo(CheckpointStatus.CANCELLED);
    }

    @Test
    @DisplayName("Checkpoint: markWaitingHitl writes target and branch separately")
    void markWaitingHitlWritesBothColumns() {
        UUID executionId = startLinear("{}").id();
        UUID id = UUID.randomUUID();
        checkpoints.insertIfAbsent(new CheckpointInsert(
                id, executionId, "gate", 1,
                NodeType.STEP, "reviewer", null,
                CheckpointStatus.PENDING, null, null, "{}", null,
                1, 3, 60_000L, NOW, null));

        checkpoints.markWaitingHitl(id, "approval-required");

        CheckpointRow row = checkpoints.findById(id).orElseThrow();
        assertThat(row.status()).isEqualTo(CheckpointStatus.WAITING_HITL);
        assertThat(row.decisionBranch()).isEqualTo("approval-required");
    }

    @Test
    @DisplayName("HITL-Ticket: insert, escalate, resolve")
    void ticketWriteMethods() {
        UUID executionId = startLinear("{}").id();
        UUID ticketId = ticketStore.insert(executionId, null, "step-x", 1,
                "NEEDS_HITL", "{}", NOW.plus(Duration.ofHours(1)));

        assertThat(ticketStore.findById(ticketId)).isPresent();
        assertThat(ticketStore.findOpen(executionId, "step-x", 1, "NEEDS_HITL")).isPresent();
        assertThat(ticketStore.hasOpen(executionId, "step-x", 1, "NEEDS_HITL")).isTrue();

        ticketStore.escalate(ticketId, 2, NOW.plus(Duration.ofHours(2)));
        assertThat(ticketStore.findById(ticketId).orElseThrow().escalationCount()).isEqualTo(2);

        ticketStore.resolve(ticketId, HitlTicketStatus.RESOLVED, "operator", "done");
        assertThat(ticketStore.findById(ticketId).orElseThrow().status())
                .isEqualTo(HitlTicketStatus.RESOLVED);
    }

    @Test
    @DisplayName("DLQ: insert schreibt einen lesbaren Eintrag")
    void dlqInsert() {
        UUID dlqId = dlqStore.insert("agent.execution.results", "wf-1", "{\"a\":1}",
                "IllegalState", "boom");
        assertThat(dlqId).isNotNull();

        Integer count = jdbc.queryForObject("SELECT count(*) FROM dlq_messages WHERE id = ?",
                Integer.class, dlqId);
        assertThat(count).isEqualTo(1);
    }
}
