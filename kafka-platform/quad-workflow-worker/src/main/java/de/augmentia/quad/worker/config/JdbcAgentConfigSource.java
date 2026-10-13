package de.augmentia.quad.worker.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Loads the agent configuration from the shared DB table
 * {@code agent_definitions} &mdash; the <em>only</em> agent source available to
 * the worker in DB mode.
 *
 * <p><strong>Activation (property gate):</strong> This source is registered as a bean
 * in {@link QuadWorkerConfig} <em>only</em> when
 * {@code worker.quad.agent-source=db} is set. If the property is not set (default = YAML),
 * the worker stays <strong>runnable without a database</strong> — the QUAD-only mode needs
 * no DB. {@link PropertiesAgentConfigSource} (YAML) therefore remains the always-active
 * fallback source.
 *
 * <p><strong>Mapping (DB column → record field):</strong>
 * <pre>
 * agent_type       → agentId
 * system_prompt    → systemPrompt
 * model_name       → modelKey
 * temperature      → temperature
 * max_tokens       → maxOutputTokens
 * tools_config     → allowedTools (parsed from a JSON array)
 * max_react_iterations → maxReActIterations (default from the configuration,
 *                    previously fixed at 10)
 * (no columns)     → structuredOutputEnabled (default false)
 * </pre>
 *
 * <p>Columns that are not mapped (description, timeout_seconds,
 * max_retry_attempts, version) exist only in the schema and are currently unused by the
 * worker record.
 *
 * <h3>Caching</h3>
 * {@link #find(String)} runs per task and was originally served by one {@code SELECT} per
 * call. {@code agent_definitions} however changes rarely, while the worker re-reads the
 * same row for every task. The source therefore caches per {@code agent_type} with a short
 * TTL.
 *
 * <p>Known limit: a changed definition only becomes visible after the TTL has expired
 * (default 30 s). A negative lookup is deliberately <em>not</em> cached, so that a
 * definition created afterwards takes effect immediately.
 */
@Order(1)
public class JdbcAgentConfigSource implements AgentConfigSource {

    private static final String SQL_FIND_BY_AGENT_TYPE = """
            SELECT agent_type,
                   description,
                   system_prompt,
                   model_name,
                   temperature,
                   max_tokens,
                   tools_config,
                   timeout_seconds,
                   max_retry_attempts,
                   version,
                   max_react_iterations
            FROM agent_definitions
            WHERE agent_type = ?
            """;

    /**
     * Applies when the column is {@code NULL} or not positive. Matches what was
     * previously fixed at 10 here. Configurable via the constructor so that an
     * operation can adjust the value without a recompile and restart.
     */
    private final int defaultMaxReActIterations;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final Cache<String, WorkerProperties.AgentConfig> cache;

    public JdbcAgentConfigSource(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this(jdbcTemplate, objectMapper, Duration.ofSeconds(30));
    }

    public JdbcAgentConfigSource(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, Duration ttl) {
        this(jdbcTemplate, objectMapper, ttl, WorkerProperties.DEFAULT_MAX_REACT_ITERATIONS);
    }

    public JdbcAgentConfigSource(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, Duration ttl,
                                 int defaultMaxReActIterations) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.defaultMaxReActIterations = defaultMaxReActIterations > 0
                ? defaultMaxReActIterations
                : WorkerProperties.DEFAULT_MAX_REACT_ITERATIONS;
        this.cache = Caffeine.newBuilder()
                .maximumSize(1_000)
                .expireAfterWrite(ttl)
                .build();
    }

    @Override
    public Optional<WorkerProperties.AgentConfig> find(String agentType) {
        WorkerProperties.AgentConfig cached = cache.getIfPresent(agentType);
        if (cached != null) {
            return Optional.of(cached);
        }

        java.util.List<WorkerProperties.AgentConfig> rows =
                jdbcTemplate.query(
                        SQL_FIND_BY_AGENT_TYPE,
                        (rs, rowNum) -> mapRow(rs),
                        agentType);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        WorkerProperties.AgentConfig config = rows.get(0);
        cache.put(agentType, config);
        return Optional.of(config);
    }

    /**
     * Transforms an {@code agent_definitions} row into a
     * {@link WorkerProperties.AgentConfig}. Fields the DB does not know,
     * defaults received, so that the worker keeps running without YAML adjustment.
     */
    private WorkerProperties.AgentConfig mapRow(ResultSet rs) throws SQLException {
        String agentType = rs.getString("agent_type");
        String systemPrompt = rs.getString("system_prompt");
        String modelName = rs.getString("model_name");
        double temperature = rs.getDouble("temperature");
        int maxTokens = rs.getInt("max_tokens");
        int maxReActIterations = rs.getInt("max_react_iterations");
        String toolsConfig = rs.getString("tools_config");

        return new WorkerProperties.AgentConfig(
                agentType,                     // agentId = agent_type
                modelName,                     // modelKey = model_name (ChatModelRegistry-Key)
                temperature,                   // temperature
                maxReActIterations > 0 ? maxReActIterations : defaultMaxReActIterations,
                maxTokens,                     // maxOutputTokens = max_tokens
                parseToolsConfig(toolsConfig), // allowedTools = tools_config
                false,                         // structuredOutputEnabled (no DB column)
                systemPrompt,                  // systemPrompt = system_prompt
                null,                          // skillsDir (no DB column)
                null,                          // initialSkills (no DB column)
                false                          // skillSearchEnabled (no DB column)
        );
    }

    /**
     * Parses {@code tools_config} (JSONB array of tool names) into a set.
     * Empty/null JSON → empty set (no catalog tool, only the agent's own tools).
     */
    private java.util.Set<String> parseToolsConfig(String json) {
        if (json == null || json.isBlank() || "null".equalsIgnoreCase(json)) {
            return java.util.Set.of();
        }
        try {
            java.util.List<String> tools = objectMapper.readValue(
                    json, new TypeReference<java.util.List<String>>() {});
            return java.util.Set.copyOf(tools);
        } catch (Exception e) {
            return java.util.Set.of();
        }
    }

    @Override
    public String name() {
        return "jdbc-agent_definitions";
    }
}
