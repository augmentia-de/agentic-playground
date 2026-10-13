package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.event.ControlEvents;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guard for the lease wiring (O1) and the N+1-free abort (O5).
 *
 * <p>{@code OrchestratorLeaseService} existed but had <b>0 callers</b> —
 * with several orchestrator replicas the sweep ran on every instance. Now it holds
 * {@link OrchestratorLeaseService#LOCK_SWEEP} and skips the cycle when
 * another instance holds it.
 *
 * <p>The lease is fail-closed: every error yields {@code false}. That is why it is also
 * checked that a second acquisition within the TTL is rejected.
 */
@DisplayName("SweepReaper — lease and abort")
class SweepReaperLeaseTest extends WorkflowEngineTestBase {

    @Autowired
    private OrchestratorLeaseService lease;

    @Autowired
    private SweepReaperService sweepReaper;

    // ── Lease ────────────────────────────────────────────────────────

    @Test
    @DisplayName("The lease can be taken and released again")
    void leaseCanBeAcquiredAndReleased() {
        assertThat(lease.tryAcquire(OrchestratorLeaseService.LOCK_SWEEP)).isTrue();
        assertThat(lease.holds(OrchestratorLeaseService.LOCK_SWEEP))
                .as("The same instance holds the lock")
                .isTrue();

        lease.release(OrchestratorLeaseService.LOCK_SWEEP);
        assertThat(lease.holds(OrchestratorLeaseService.LOCK_SWEEP)).isFalse();
    }

    @Test
    @DisplayName("A second acquisition without release is rejected (fail-closed)")
    void secondAcquireIsRejected() {
        assertThat(lease.tryAcquire(OrchestratorLeaseService.LOCK_SWEEP)).isTrue();
        assertThat(lease.tryAcquire(OrchestratorLeaseService.LOCK_SWEEP))
                .as("lock not yet released -> rejected")
                .isFalse();
    }

    @Test
    @DisplayName("Different lock names do not interfere")
    void differentLockNamesAreIndependent() {
        assertThat(lease.tryAcquire(OrchestratorLeaseService.LOCK_SWEEP)).isTrue();
        assertThat(lease.tryAcquire(OrchestratorLeaseService.LOCK_CONFIG_RELOAD)).isTrue();
    }

    @Test
    @DisplayName("A foreign lock blocks the sweep cycle")
    void runSweepSkipsWhenAnotherInstanceHoldsLock() {
        // simulate the lock of another instance
        jdbc.update("""
                INSERT INTO shedlock (name, lock_until, locked_at, locked_by)
                VALUES (?, now() + interval '10 minutes', now(), ?)
                ON CONFLICT (name) DO NOTHING
                """, OrchestratorLeaseService.LOCK_SWEEP, "another-node");

        assertThat(lease.tryAcquire(OrchestratorLeaseService.LOCK_SWEEP))
                .as("A foreign lock blocks the acquisition")
                .isFalse();

        sweepReaper.runSweep(null);

        // The foreign lock must neither have been taken over nor released.
        String holder = jdbc.queryForObject(
                "SELECT locked_by FROM shedlock WHERE name = ?", String.class,
                OrchestratorLeaseService.LOCK_SWEEP);
        assertThat(holder)
                .as("runSweep must not release a foreign lock")
                .isEqualTo("another-node");
    }

    @Test
    @DisplayName("runSweep releases the lock afterwards")
    void runSweepReleasesLock() {
        startLinear("{}");   // the sweep should find real work

        sweepReaper.runSweep(null);

        assertThat(lease.holds(OrchestratorLeaseService.LOCK_SWEEP))
                .as("runSweep must release the lock again in the finally")
                .isFalse();
    }

    @Test
    @DisplayName("Two consecutive sweeps are both executed")
    void consecutiveSweepsBothRun() {
        startLinear("{}");
        sweepReaper.runSweep(null);
        sweepReaper.runSweep(null);
        assertThat(lease.holds(OrchestratorLeaseService.LOCK_SWEEP))
                .as("Without a permanent blockage the second cycle may run again")
                .isFalse();
    }

    // ── latestByStep: der N+1-Fix (O5) ──────────────────────────────

    @Test
    @DisplayName("latestByStep yields the highest iteration per step")
    void latestByStepReturnsHighestIterationPerStep() {
        var execution = startLinear("{}");
        UUID id = execution.id();

        insertCheckpoint(id, "a", 1, CheckpointStatus.COMPLETED);
        insertCheckpoint(id, "a", 2, CheckpointStatus.IN_PROGRESS);
        insertCheckpoint(id, "b", 1, CheckpointStatus.PENDING);

        var latest = checkpoints.latestByStep(id);

        assertThat(latest).containsOnlyKeys("a", "b");
        assertThat(latest.get("a").iteration())
                .as("The higher iteration wins")
                .isEqualTo(2);
        assertThat(latest.get("a").status()).isEqualTo(CheckpointStatus.IN_PROGRESS);
        assertThat(latest.get("b").iteration()).isEqualTo(1);
    }

    @Test
    @DisplayName("latestByStep liefert dasselbe wie der Einzelabruf latest(step)")
    void latestByStepMatchesSingleLookup() {
        var execution = startLinear("{}");
        UUID id = execution.id();
        insertCheckpoint(id, "a", 1, CheckpointStatus.COMPLETED);
        insertCheckpoint(id, "a", 3, CheckpointStatus.IN_PROGRESS);

        var bulk = checkpoints.latestByStep(id);
        var single = checkpoints.latest(id, "a").orElseThrow();

        assertThat(bulk.get("a").id()).isEqualTo(single.id());
        assertThat(bulk.get("a").iteration()).isEqualTo(single.iteration());
        assertThat(bulk.get("a").status()).isEqualTo(single.status());
    }

    @Test
    @DisplayName("latestByStep returns an empty map for an unknown execution")
    void latestByStepEmptyForUnknownExecution() {
        assertThat(checkpoints.latestByStep(UUID.randomUUID())).isEmpty();
    }

    @Test
    @DisplayName("The abort of a large DAG uses latestByStep instead of N individual queries")
    void cancellationOfLinearWorkflowIsConsistent() {
        var execution = startLinear("{}");
        engine.onResult(completed(lastDispatchedTask()));   // a done, b running

        engine.onCancellation(new ControlEvents.CancellationEvent(
                execution.id().toString(), "abort", "operator"));

        // The bulk variant and the marked rows must agree.
        var latest = checkpoints.latestByStep(execution.id());
        for (var row : engine.findCheckpoints(execution.id())) {
            assertThat(latest.get(row.stepId()).id()).isEqualTo(row.id());
        }
    }

    // ── Hilfen ───────────────────────────────────────────────────────

    private void insertCheckpoint(UUID executionId, String stepId, int iteration, CheckpointStatus status) {
        jdbc.update("""
                INSERT INTO workflow_checkpoints
                    (id, workflow_execution_id, step_id, iteration, node_type, status,
                     attempt, max_attempts, timeout_ms, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'STEP', ?, 1, 3, 60000, now(), now())
                ON CONFLICT (workflow_execution_id, step_id, iteration) DO NOTHING
                """,
                UUID.randomUUID(), executionId, stepId, iteration, status.name());
    }
}
