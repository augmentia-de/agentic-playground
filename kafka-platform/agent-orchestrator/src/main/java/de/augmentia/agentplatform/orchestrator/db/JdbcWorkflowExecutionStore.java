package de.augmentia.agentplatform.orchestrator.db;

import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcWorkflowExecutionStore {

    private static final RowMapper<WorkflowExecutionRow> MAPPER = JdbcWorkflowExecutionStore::map;

    private final JdbcTemplate jdbcTemplate;

    private final java.time.Clock clock;

    /**
     * @param clock source for all timestamps of this table. Deliberately not
     *              the {@code now()} of the database: {@code updated_at} is
     *              compared in {@code findRunningUpdatedBefore} against a cutoff
     *              derived from the same clock. Two clocks make this
     *              decision impossible.
     */
    public JdbcWorkflowExecutionStore(JdbcTemplate jdbcTemplate, java.time.Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    public void create(UUID id, UUID definitionId, String idempotencyKey, String inputParamsJson) {
        jdbcTemplate.update("""
                INSERT INTO workflow_executions (id, workflow_definition_id, idempotency_key, status, input_params, started_at, updated_at)
                VALUES (?, ?, ?, 'PENDING', ?::jsonb, ?, ?)
                """, id, definitionId, idempotencyKey, inputParamsJson, timestamp(clock.instant()), timestamp(clock.instant()));
    }

    public Optional<WorkflowExecutionRow> findById(UUID id) {
        return jdbcTemplate.query("SELECT * FROM workflow_executions WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    public Optional<WorkflowExecutionRow> findByIdempotencyKey(String idempotencyKey) {
        return jdbcTemplate.query("SELECT * FROM workflow_executions WHERE idempotency_key = ?", MAPPER, idempotencyKey)
                .stream().findFirst();
    }

    public Optional<WorkflowExecutionRow> lock(UUID id) {
        return jdbcTemplate.query("SELECT * FROM workflow_executions WHERE id = ? FOR UPDATE", MAPPER, id)
                .stream().findFirst();
    }

    public void updateStatus(UUID id, ExecutionStatus status) {
        jdbcTemplate.update("UPDATE workflow_executions SET status = ?, updated_at = ? WHERE id = ?",
                status.name(), timestamp(clock.instant()), id);
    }

    public void markRunning(UUID id) {
        jdbcTemplate.update("UPDATE workflow_executions SET status = 'RUNNING', updated_at = ? WHERE id = ? AND status IN ('PENDING', 'PAUSED_HITL')", timestamp(clock.instant()), id);
    }

    public void markPausedHitl(UUID id) {
        jdbcTemplate.update("UPDATE workflow_executions SET status = 'PAUSED_HITL', updated_at = ? WHERE id = ? AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')", timestamp(clock.instant()), id);
    }

    public void markCompleted(UUID id, String finalOutputJson) {
        jdbcTemplate.update("""
                UPDATE workflow_executions
                SET status = 'COMPLETED', final_output = ?::jsonb, completed_at = ?, updated_at = ?
                WHERE id = ? AND status <> 'COMPLETED'
                """, finalOutputJson, timestamp(clock.instant()), timestamp(clock.instant()), id);
    }

    public void markCancelled(UUID id, String errorDetailsJson) {
        jdbcTemplate.update("""
                UPDATE workflow_executions
                SET status = 'CANCELLED', error_details = ?::jsonb, completed_at = ?, updated_at = ?
                WHERE id = ? AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                """, errorDetailsJson, timestamp(clock.instant()), timestamp(clock.instant()), id);
    }

    public void markFailed(UUID id, String errorDetailsJson) {
        jdbcTemplate.update("""
                UPDATE workflow_executions
                SET status = 'FAILED', error_details = ?::jsonb, completed_at = ?, updated_at = ?
                WHERE id = ? AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                """, errorDetailsJson, timestamp(clock.instant()), timestamp(clock.instant()), id);
    }

    public List<UUID> findRunningUpdatedBefore(Instant cutoff) {
        return jdbcTemplate.queryForList("""
                SELECT id FROM workflow_executions
                WHERE status = 'RUNNING' AND updated_at < ?
                ORDER BY id
                """, UUID.class, Timestamp.from(cutoff));
    }

    public List<UUID> findRunningStalledByFailed() {
        return jdbcTemplate.queryForList("""
                SELECT DISTINCT e.id
                FROM workflow_executions e
                WHERE e.status = 'RUNNING'
                  AND EXISTS (
                      SELECT 1 FROM workflow_checkpoints c
                      WHERE c.workflow_execution_id = e.id AND c.status IN ('FAILED', 'WAITING_HITL')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM workflow_checkpoints c
                      WHERE c.workflow_execution_id = e.id AND c.status = 'IN_PROGRESS'
                  )
                ORDER BY e.id
                """, UUID.class);
    }

    private static WorkflowExecutionRow map(ResultSet rs, int rowNum) throws SQLException {
        return new WorkflowExecutionRow(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_definition_id", UUID.class),
                rs.getString("idempotency_key"),
                ExecutionStatus.valueOf(rs.getString("status")),
                rs.getString("input_params"),
                rs.getString("final_output"),
                rs.getString("error_details"),
                instant(rs, "started_at"),
                instant(rs, "completed_at"),
                instant(rs, "updated_at")
        );
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}
