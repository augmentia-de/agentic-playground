package de.augmentia.agentplatform.orchestrator.db;

import de.augmentia.agentplatform.bridge.AgentDefinitionBridge;
import de.augmentia.agentplatform.orchestrator.domain.AgentDefinitionRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcAgentDefinitionStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcAgentDefinitionStore.class);

    private static final RowMapper<AgentDefinitionRow> MAPPER = JdbcAgentDefinitionStore::map;

    private final JdbcTemplate jdbcTemplate;

    public JdbcAgentDefinitionStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<AgentDefinitionRow> findByType(String agentType) {
        return jdbcTemplate.query("SELECT * FROM agent_definitions WHERE agent_type = ?", MAPPER, agentType)
                .stream().findFirst();
    }

    public List<AgentDefinitionRow> list() {
        return jdbcTemplate.query("SELECT * FROM agent_definitions ORDER BY agent_type", MAPPER);
    }

    public void upsert(String agentType, String description, String systemPrompt,
                       String modelName, double temperature, int maxTokens,
                       int timeoutSeconds, int maxRetryAttempts, int version) {
        upsert(agentType, description, systemPrompt, modelName, temperature, maxTokens,
                timeoutSeconds, maxRetryAttempts, version,
                AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS);
    }

    /**
     * @param maxReActIterations iteration limit for the agent. Must be {@code > 0};
     *                           the column has a CHECK that enforces it.
     */
    public void upsert(String agentType, String description, String systemPrompt,
                       String modelName, double temperature, int maxTokens,
                       int timeoutSeconds, int maxRetryAttempts, int version,
                       int maxReActIterations) {
        jdbcTemplate.update("""
                INSERT INTO agent_definitions (agent_type, description, system_prompt, model_name, temperature, max_tokens, timeout_seconds, max_retry_attempts, version, max_react_iterations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (agent_type) DO UPDATE SET
                    description = EXCLUDED.description,
                    system_prompt = EXCLUDED.system_prompt,
                    model_name = EXCLUDED.model_name,
                    temperature = EXCLUDED.temperature,
                    max_tokens = EXCLUDED.max_tokens,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    max_retry_attempts = EXCLUDED.max_retry_attempts,
                    version = EXCLUDED.version,
                    max_react_iterations = EXCLUDED.max_react_iterations,
                    updated_at = CURRENT_TIMESTAMP
                """, agentType, description, systemPrompt, modelName, temperature, maxTokens, timeoutSeconds, maxRetryAttempts, version, maxReActIterations);
    }

    /**
     * Creates a placeholder agent so a task for an unknown agent type is executable at all.
     *
     * <p>Not a substitute for registering the real agent. The stub answers with a generic
     * prompt and none of the configured behaviour, so the warning names the agent type and the
     * endpoint that fixes it.
     *
     * <p>The model key must be one the worker can resolve. It used to be hardcoded to
     * {@code gpt-4o-mini}, while the worker registers exactly one key
     * ({@code worker.quad} resolves agents with {@code models.get(modelKey)} and throws on
     * anything else). Every task of an unconverted agent therefore failed three times with
     * "Unknown model key" and ended in {@code PAUSED_HITL} — a guaranteed failure disguised as
     * a workflow that ran. The key is configurable and defaults to {@code simple}.
     */
    public void ensureStub(String agentType, String modelKey) {
        if (findByType(agentType).isPresent()) return;
        log.warn("No agent definition for agent type '{}' — creating a generic stub on model '{}' "
                        + "(1024 tokens, no tools). Steps of this type will NOT run as designed. "
                        + "Convert the agent first: POST /api/v1/unified/agents/convert",
                agentType, modelKey);
        upsert(agentType,
                "Auto-registered agent: " + agentType,
                "You are " + agentType + ". Complete the task as specified.",
                modelKey,
                0.10,
                1024,
                60,
                3,
                1,
                AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS);
    }

    /** Every agent type currently registered, for topic provisioning. */
    public List<String> allAgentTypes() {
        return jdbcTemplate.queryForList("SELECT agent_type FROM agent_definitions", String.class);
    }

    private static AgentDefinitionRow map(ResultSet rs, int rowNum) throws SQLException {
        return new AgentDefinitionRow(
                rs.getString("agent_type"),
                rs.getString("description"),
                rs.getString("system_prompt"),
                rs.getString("model_name"),
                rs.getDouble("temperature"),
                rs.getInt("max_tokens"),
                rs.getInt("timeout_seconds"),
                rs.getInt("max_retry_attempts"),
                rs.getInt("version"),
                rs.getInt("max_react_iterations")
        );
    }
}
