package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * All timestamps come from the same clock.
 *
 * <p>Previously the stores wrote their timestamps partly with the database's
 * {@code now()}, partly from the injected {@code Clock}. That was not only unclean,
 * but functionally wrong: {@code SweepReaperService} compares
 * {@code workflow_executions.updated_at} (database time) against a cutoff derived
 * from {@link #NOW}. With a fixed test clock and real
 * database time, <em>every</em> running execution counted as orphaned.
 *
 * <p>The base class fixes the clock to {@link WorkflowEngineTestBase#NOW}
 * (2026-09-25). The container was at 2026-09-29. Every test here that
 * reads from the database, which therefore proves that no time comes from the database:
 * if a clock were still correct from the DB, the values would be shifted by four days.
 */
@DisplayName("Einheitliche Zeitquelle")
class TimestampSourceTest extends WorkflowEngineTestBase {

    /** Tolerance for DB round-trip latency, but far below the four days of drift. */
    private static final Duration TOLERANCE = Duration.ofMinutes(1);

    @Autowired
    private JdbcWorkflowExecutionStore executionStore;

    @Test
    @DisplayName("the created_at/updated_at of the execution come from the clock, not from the DB")
    void executionTimestampsComeFromTheClock() {
        var execution = startLinear("{}");
        UUID id = execution.id();

        var row = executionStore.findById(id).orElseThrow();
        assertThat(row.updatedAt()).isCloseTo(NOW, withinTolerance());
        assertThat(row.startedAt()).isCloseTo(NOW, withinTolerance());

        // workflow_executions has no created_at column; the check of the
        // insert columns happens further below on workflow_checkpoints.
    }

    @Test
    @DisplayName("Checkpoint-Zeitstempel kommen aus der Uhr")
    void checkpointTimestampsComeFromTheClock() {
        var execution = startLinear("{}");
        engine.onResult(completed(lastDispatchedTask()));

        CheckpointRowAssert.assertAllWithin(
                engine.findCheckpoints(execution.id()),
                NOW, TOLERANCE);
    }

    @Test
    @DisplayName("A freshly started execution is NOT 'orphaned'")
    void freshExecutionIsNotConsideredStale() {
        // core of the regression: updated_at from the DB (4 days in the future or
        // past depending on the direction of view) against a cutoff from the clock.
        var execution = startLinear("{}");

        // the sweep computes recoveryCutoff = now - 2 * sweepInterval and searches for
        // executions whose updated_at lies BEFORE it. A fresh one (updated_at = now)
        // is therefore no candidate — exactly this computation failed before with two clocks.
        Instant recoveryCutoff = NOW.minus(Duration.ofSeconds(10));
        assertThat(executionStore.findRunningUpdatedBefore(recoveryCutoff))
                .as("A just-started execution is not a sweep candidate")
                .isEmpty();

        // counter-check with a genuinely stale execution: artificially backdate
        // updated_at, then the sweep must find it.
        jdbc.update("UPDATE workflow_executions SET updated_at = ? WHERE id = ?",
                java.sql.Timestamp.from(NOW.minus(Duration.ofDays(1))), execution.id());
        assertThat(executionStore.findRunningUpdatedBefore(recoveryCutoff))
                .as("An execution not updated for a day must stand out")
                .contains(execution.id());
    }

    @Test
    @DisplayName("Sweep-Checkpoint-Erkennung nutzt dieselbe Uhr")
    void staleCheckpointDetectionUsesTheSameClock() {
        var execution = startLinear("{}");
        // Checkpoint ist IN_PROGRESS, started_at = NOW, timeout 60 s.
        // A sweep shortly afterwards must not yet report it as orphaned.
        assertThat(checkpoints.findStaleInProgress(0, NOW.plus(Duration.ofSeconds(30))))
                .as("Checkpoints within the deadline are not orphaned")
                .isEmpty();

        assertThat(checkpoints.findStaleInProgress(0, NOW.plus(Duration.ofMinutes(5))))
                .as("After the timeout is exceeded the checkpoint is orphaned")
                .isNotEmpty();
    }

    @Test
    @DisplayName("HITL-Ticket-Zeitstempel kommen aus der Uhr")
    void ticketTimestampsComeFromTheClock() {
        var execution = start(approvalWorkflow(), "{}");
        engine.onResult(completed(lastDispatchedTask()));

        var ticket = ticketStore.findOpenByExecution(execution.id()).get(0);
        assertThat(ticket.createdAt())
                .as("the created_at of the ticket comes from the clock")
                .isCloseTo(NOW, withinTolerance());
    }

    /** Tolerance of one minute against the time difference. */
    private static org.assertj.core.data.TemporalUnitOffset withinTolerance() {
        return new org.assertj.core.data.TemporalUnitWithinOffset(
                TOLERANCE.getSeconds(), java.time.temporal.ChronoUnit.SECONDS);
    }

    private static org.assertj.core.data.TemporalUnitOffset offset(Duration tolerance) {
        return new org.assertj.core.data.TemporalUnitWithinOffset(
                tolerance.getSeconds(), java.time.temporal.ChronoUnit.SECONDS);
    }

    /** Small assertion helper for checkpoint lists. */
    static final class CheckpointRowAssert {
        static void assertAllWithin(List<CheckpointRow> rows,
                                    Instant expected, Duration tolerance) {
            assertThat(rows).isNotEmpty();
            for (var row : rows) {
                if (row.createdAt() != null) {
                    assertThat(row.createdAt()).isCloseTo(expected, offset(tolerance));
                }
                if (row.updatedAt() != null) {
                    assertThat(row.updatedAt()).isCloseTo(expected, offset(tolerance));
                }
            }
        }
    }
}
