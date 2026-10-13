package de.augmentia.agentplatform.orchestrator.db;

import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
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
public class JdbcHitlTicketStore {

    private static final RowMapper<HitlTicketRow> MAPPER = JdbcHitlTicketStore::map;

    private final JdbcTemplate jdbcTemplate;

    private final java.time.Clock clock;

    /**
     * @param clock source for all timestamps of this table. Deliberately not
     *              the {@code now()} of the database: {@code updated_at} is
     *              compared in {@code findRunningUpdatedBefore} against a cutoff
     *              derived from the same clock. Two clocks make this
     *              decision impossible.
     */
    public JdbcHitlTicketStore(JdbcTemplate jdbcTemplate, java.time.Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    public UUID insert(UUID executionId, UUID checkpointId, String stepId, int iteration, String reason,
                        String contextJson, Instant expiresAt) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO hitl_tickets (id, workflow_execution_id, checkpoint_id, step_id, iteration, reason, status, context_data, expires_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?::jsonb, ?, ?)
                """, id, executionId, checkpointId, stepId, iteration, reason, contextJson,
                timestamp(expiresAt), timestamp(clock.instant()));
        return id;
    }

    public Optional<HitlTicketRow> findById(UUID id) {
        return jdbcTemplate.query("SELECT * FROM hitl_tickets WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    public List<HitlTicketRow> findByStatus(HitlTicketStatus status) {
        return jdbcTemplate.query("SELECT * FROM hitl_tickets WHERE status = ? ORDER BY created_at, id",
                MAPPER, status.name());
    }

    public List<HitlTicketRow> findOpenByExecution(UUID executionId) {
        return jdbcTemplate.query("SELECT * FROM hitl_tickets WHERE workflow_execution_id = ? AND status = 'OPEN' ORDER BY created_at, id",
                MAPPER, executionId);
    }

    public boolean hasOpen(UUID executionId, String stepId, int iteration, String reason) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM hitl_tickets
                WHERE workflow_execution_id = ? AND step_id = ? AND iteration = ? AND reason = ? AND status = 'OPEN'
                """, Integer.class, executionId, stepId, iteration, reason);
        return count != null && count > 0;
    }

    public Optional<HitlTicketRow> findOpen(UUID executionId, String stepId, int iteration, String reason) {
        return jdbcTemplate.query("""
                SELECT * FROM hitl_tickets
                WHERE workflow_execution_id = ? AND step_id = ? AND iteration = ? AND reason = ? AND status = 'OPEN'
                ORDER BY created_at, id
                LIMIT 1
                """, MAPPER, executionId, stepId, iteration, reason).stream().findFirst();
    }

    public void resolve(UUID id, HitlTicketStatus status, String resolvedBy, String resolutionNotes) {
        jdbcTemplate.update("""
                UPDATE hitl_tickets
                SET status = ?, resolved_by = ?, resolution_notes = ?, resolved_at = ?
                WHERE id = ? AND status = 'OPEN'
                """, status.name(), resolvedBy, resolutionNotes, timestamp(clock.instant()), id);
    }

    public List<HitlTicketRow> findExpired(Instant now) {
        return jdbcTemplate.query("""
                SELECT * FROM hitl_tickets
                WHERE status = 'OPEN' AND expires_at IS NOT NULL AND expires_at < ?
                ORDER BY created_at, id
                """, MAPPER, timestamp(now));
    }

    public void escalate(UUID id, int escalationCount, Instant newExpiresAt) {
        jdbcTemplate.update("""
                UPDATE hitl_tickets
                SET escalation_count = ?, expires_at = ?, status = 'OPEN'
                WHERE id = ?
                """, escalationCount, timestamp(newExpiresAt), id);
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static HitlTicketRow map(ResultSet rs, int rowNum) throws SQLException {
        return new HitlTicketRow(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_execution_id", UUID.class),
                rs.getObject("checkpoint_id", UUID.class),
                rs.getString("step_id"),
                rs.getInt("iteration"),
                rs.getString("reason"),
                HitlTicketStatus.valueOf(rs.getString("status")),
                rs.getString("context_data"),
                rs.getInt("escalation_count"),
                instant(rs, "expires_at"),
                instant(rs, "created_at"),
                instant(rs, "resolved_at"),
                rs.getString("resolved_by"),
                rs.getString("resolution_notes")
        );
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
