package de.augmentia.agentplatform.orchestrator.db;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.CheckpointSnapshot;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointInsert;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcCheckpointStore {

    private static final RowMapper<CheckpointRow> MAPPER = JdbcCheckpointStore::map;

    private final JdbcTemplate jdbcTemplate;
    private final java.time.Clock clock;

    /**
     * @param clock source for all timestamps. Deliberately the application clock and not
     *              {@code now()} in the database: {@code updated_at} is
     *              later compared against a cutoff derived from the same clock
     *              ({@code findRunningUpdatedBefore}). Two clocks
     *              would make this decision impossible.
     */
    public JdbcCheckpointStore(JdbcTemplate jdbcTemplate, java.time.Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    public Map<String, CheckpointSnapshot> snapshotByStep(UUID executionId) {
        List<CheckpointRow> rows = jdbcTemplate.query("""
                SELECT DISTINCT ON (step_id) *
                FROM workflow_checkpoints
                WHERE workflow_execution_id = ?
                ORDER BY step_id, iteration DESC
                """, MAPPER, executionId);
        Map<String, CheckpointSnapshot> snapshots = new LinkedHashMap<>();
        for (CheckpointRow row : rows) {
            snapshots.put(row.stepId(), new CheckpointSnapshot(
                    row.workflowExecutionId().toString(),
                    row.stepId(),
                    row.nodeType(),
                    row.status(),
                    row.iteration(),
                    row.decisionBranch(),
                    row.agentType()));
        }
        return snapshots;
    }

    public Map<String, Object> outputContext(UUID executionId) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                SELECT step_id, output_payload
                FROM workflow_checkpoints
                WHERE workflow_execution_id = ? AND output_payload IS NOT NULL
                ORDER BY iteration ASC, updated_at ASC
                """, executionId);
        Map<String, Object> context = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            Object stepId = row.get("step_id");
            Object payload = row.get("output_payload");
            if (stepId == null || payload == null) {
                continue;
            }
            context.put(stepId.toString(), parseJson(payload.toString()));
        }
        return context;
    }

    public List<CheckpointRow> findByExecution(UUID executionId) {
        return jdbcTemplate.query("SELECT * FROM workflow_checkpoints WHERE workflow_execution_id = ? ORDER BY step_id, iteration",
                MAPPER, executionId);
    }

    public Optional<CheckpointRow> findById(UUID id) {
        return jdbcTemplate.query("SELECT * FROM workflow_checkpoints WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    public Optional<CheckpointRow> latest(UUID executionId, String stepId) {
        return jdbcTemplate.query("""
                SELECT * FROM workflow_checkpoints
                WHERE workflow_execution_id = ? AND step_id = ?
                ORDER BY iteration DESC
                LIMIT 1
                """, MAPPER, executionId, stepId).stream().findFirst();
    }

    /**
     * All latest checkpoints of an execution in <em>one</em> query, per
     * {@code step_id} the checkpoint with the highest iteration.
     *
     * <p>Replaces N individual queries via {@link #latest(UUID, String)} in
     * operations that touch every node of a DAG (e.g. abort of a
     * workflow). The ordering corresponds to {@code latest()}: the highest iteration
     * wins, on a tie the most recently inserted row wins.
     */
    public Map<String, CheckpointRow> latestByStep(UUID executionId) {
        List<CheckpointRow> rows = jdbcTemplate.query("""
                SELECT DISTINCT ON (step_id) *
                FROM workflow_checkpoints
                WHERE workflow_execution_id = ?
                ORDER BY step_id, iteration DESC, created_at DESC
                """, MAPPER, executionId);
        Map<String, CheckpointRow> byStep = new LinkedHashMap<>();
        for (CheckpointRow row : rows) {
            byStep.put(row.stepId(), row);
        }
        return byStep;
    }

    public Optional<CheckpointRow> findByTaskId(UUID taskId) {
        return jdbcTemplate.query("SELECT * FROM workflow_checkpoints WHERE task_id = ?", MAPPER, taskId).stream().findFirst();
    }

    public boolean insertIfAbsent(CheckpointInsert insert) {
        int rows = jdbcTemplate.update("""
                INSERT INTO workflow_checkpoints (
                    id, workflow_execution_id, step_id, iteration, node_type, agent_type, task_id, status,
                    decision_branch, branch_id, input_payload, output_payload, attempt, max_attempts, timeout_ms,
                    started_at, completed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workflow_execution_id, step_id, iteration) DO NOTHING
                """,
                insert.id(),
                insert.workflowExecutionId(),
                insert.stepId(),
                insert.iteration(),
                insert.nodeType().name(),
                insert.agentType(),
                insert.taskId(),
                insert.status().name(),
                insert.decisionBranch(),
                insert.branchId(),
                insert.inputPayloadJson(),
                insert.outputPayloadJson(),
                insert.attempt(),
                insert.maxAttempts(),
                insert.timeoutMs(),
                timestamp(insert.startedAt()),
                timestamp(insert.completedAt()),
                timestamp(clock.instant()),
                timestamp(clock.instant()));
        return rows > 0;
    }

    public void updateRoutingStatus(UUID id, CheckpointStatus status, String decisionBranch, Instant completedAt) {
        updateRoutingStatus(id, status, decisionBranch, null, completedAt);
    }

    /**
     * Wie {@link #updateRoutingStatus(UUID, CheckpointStatus, String, Instant)},
     * additionally sets the branch ID. Both columns are only overwritten via COALESCE
     * if a value is passed along — existing entries are
     * preserved if a later routing pass does not know them.
     */
    public void updateRoutingStatus(UUID id, CheckpointStatus status, String decisionBranch,
                                    String branchId, Instant completedAt) {
        jdbcTemplate.update("""
                UPDATE workflow_checkpoints
                SET status = ?,
                    decision_branch = COALESCE(?, decision_branch),
                    branch_id = COALESCE(?, branch_id),
                    completed_at = COALESCE(?, completed_at),
                    updated_at = ?
                WHERE id = ?
                """, status.name(), decisionBranch, branchId, timestamp(completedAt),
                timestamp(clock.instant()), id);
    }

    public void complete(UUID id, String outputPayloadJson) {
        jdbcTemplate.update("""
                UPDATE workflow_checkpoints
                SET status = 'COMPLETED', output_payload = ?::jsonb, error_message = NULL, completed_at = ?, updated_at = ?
                WHERE id = ?
                """, outputPayloadJson, timestamp(clock.instant()),
                timestamp(clock.instant()), id);
    }

    public void saveOutput(UUID id, String outputPayloadJson) {
        jdbcTemplate.update("UPDATE workflow_checkpoints SET output_payload = ?::jsonb, updated_at = ? WHERE id = ?",
                outputPayloadJson, timestamp(clock.instant()), id);
    }

    public void markWaitingHitl(UUID id, String reason) {
        jdbcTemplate.update("""
                UPDATE workflow_checkpoints
                SET status = 'WAITING_HITL', decision_branch = ?, updated_at = ?
                WHERE id = ?
                """, reason, timestamp(clock.instant()), id);
    }

    public void markFailed(UUID id, String errorMessage) {
        jdbcTemplate.update("""
                UPDATE workflow_checkpoints
                SET status = 'FAILED', error_message = ?, completed_at = ?, updated_at = ?
                WHERE id = ?
                """, errorMessage, timestamp(clock.instant()),
                timestamp(clock.instant()), id);
    }

    public void markCancelled(UUID id) {
        jdbcTemplate.update("""
                UPDATE workflow_checkpoints
                SET status = 'CANCELLED', completed_at = ?, updated_at = ?
                WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS', 'WAITING_HITL', 'FAILED')
                """, timestamp(clock.instant()), timestamp(clock.instant()), id);
    }

    public void dispatchNewTask(UUID id, UUID taskId, int attempt, String inputPayloadJson, Instant startedAt) {
        jdbcTemplate.update("""
                UPDATE workflow_checkpoints
                SET task_id = ?, attempt = ?, status = 'IN_PROGRESS', input_payload = ?::jsonb,
                    error_message = NULL, started_at = ?, completed_at = NULL, updated_at = ?
                WHERE id = ?
                """, taskId, attempt, inputPayloadJson, timestamp(startedAt),
                timestamp(clock.instant()), id);
    }

    public void retryTask(UUID id, UUID taskId, int attempt, String inputPayloadJson, String errorMessage, Instant startedAt) {
        jdbcTemplate.update("""
                UPDATE workflow_checkpoints
                SET task_id = ?, attempt = ?, retry_count = retry_count + 1, status = 'IN_PROGRESS',
                    input_payload = ?::jsonb, error_message = ?, started_at = ?, completed_at = NULL, updated_at = ?
                WHERE id = ?
                """, taskId, attempt, inputPayloadJson, errorMessage, timestamp(startedAt),
                timestamp(clock.instant()), id);
    }

    public List<CheckpointRow> findStaleInProgress(long graceMillis, Instant now) {
        return jdbcTemplate.query("""
                SELECT c.*
                FROM workflow_checkpoints c
                JOIN workflow_executions e ON e.id = c.workflow_execution_id
                WHERE c.status = 'IN_PROGRESS'
                  AND c.started_at IS NOT NULL
                  AND e.status IN ('RUNNING', 'PAUSED_HITL')
                  AND c.started_at + ((c.timeout_ms + ?) * interval '1 millisecond') < ?
                ORDER BY c.workflow_execution_id, c.step_id, c.iteration
                """, MAPPER, graceMillis, Timestamp.from(now));
    }

    private static Map<String, Object> parseJson(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        Map<String, Object> parsed = EventJson.read(json, Map.class);
        return parsed == null ? Map.of() : parsed;
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static CheckpointRow map(ResultSet rs, int rowNum) throws SQLException {
        return new CheckpointRow(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_execution_id", UUID.class),
                rs.getString("step_id"),
                rs.getInt("iteration"),
                NodeType.valueOf(rs.getString("node_type")),
                rs.getString("agent_type"),
                rs.getObject("task_id", UUID.class),
                CheckpointStatus.valueOf(rs.getString("status")),
                rs.getString("decision_branch"),
        rs.getString("branch_id"),
                rs.getString("input_payload"),
                rs.getString("output_payload"),
                rs.getInt("retry_count"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                rs.getLong("timeout_ms"),
                rs.getString("error_message"),
                instant(rs, "started_at"),
                instant(rs, "completed_at"),
                instant(rs, "created_at"),
                instant(rs, "updated_at")
        );
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
