package de.augmentia.agentplatform.orchestrator.db;

import de.augmentia.agentplatform.orchestrator.domain.WorkflowDefinitionRow;
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
public class JdbcWorkflowDefinitionStore {

    private static final RowMapper<WorkflowDefinitionRow> MAPPER = JdbcWorkflowDefinitionStore::map;

    private final JdbcTemplate jdbcTemplate;

    public JdbcWorkflowDefinitionStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<WorkflowDefinitionRow> findById(UUID id) {
        return jdbcTemplate.query("SELECT * FROM workflow_definitions WHERE id = ?", MAPPER, id)
                .stream().findFirst();
    }

    public Optional<WorkflowDefinitionRow> findForStart(String name, Integer version) {
        if (version != null) {
            return jdbcTemplate.query("SELECT * FROM workflow_definitions WHERE name = ? AND version = ?",
                            MAPPER, name, version)
                    .stream().findFirst();
        }
        return jdbcTemplate.query("SELECT * FROM workflow_definitions WHERE name = ? AND is_active = true ORDER BY version DESC LIMIT 1",
                        MAPPER, name)
                .stream().findFirst();
    }

    public boolean exists(String name, int version) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM workflow_definitions WHERE name = ? AND version = ?",
                Integer.class, name, version);
        return count != null && count > 0;
    }

    public UUID insert(String name, int version, String dagStructureJson, boolean activate) {
        UUID id = UUID.randomUUID();
        if (activate) {
            jdbcTemplate.update("UPDATE workflow_definitions SET is_active = false WHERE name = ?", name);
        }
        jdbcTemplate.update("""
                INSERT INTO workflow_definitions (id, name, version, dag_structure, is_active, created_at)
                VALUES (?, ?, ?, ?::jsonb, ?, now())
                """, id, name, version, dagStructureJson, activate);
        return id;
    }

    public List<WorkflowDefinitionRow> list(Boolean activeOnly) {
        if (Boolean.TRUE.equals(activeOnly)) {
            return jdbcTemplate.query("SELECT * FROM workflow_definitions WHERE is_active = true ORDER BY name, version DESC", MAPPER);
        }
        return jdbcTemplate.query("SELECT * FROM workflow_definitions ORDER BY name, version DESC", MAPPER);
    }

    private static WorkflowDefinitionRow map(ResultSet rs, int rowNum) throws SQLException {
        return new WorkflowDefinitionRow(
                rs.getObject("id", UUID.class),
                rs.getString("name"),
                rs.getInt("version"),
                rs.getString("dag_structure"),
                rs.getBoolean("is_active"),
                instant(rs, "created_at")
        );
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
