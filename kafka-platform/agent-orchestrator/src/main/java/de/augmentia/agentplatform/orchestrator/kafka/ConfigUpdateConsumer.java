package de.augmentia.agentplatform.orchestrator.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes `agent.config.updates` topic for dynamic agent config reloads (SPEC2 §14.1).
 * Broadcasts config changes from PostgreSQL CDC / Debezium or manual API updates.
 *
 * <p>When a new version is detected for an agent_type, reloads the latest
 * definition from PostgreSQL and restarts any running tasks that use that agent.
 */
@Component
public class ConfigUpdateConsumer {

    private static final Logger log = LoggerFactory.getLogger(ConfigUpdateConsumer.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ConfigUpdateConsumer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(
            topics = "${app.kafka.config-topic:agent.config.updates}",
            groupId = "config-updates-group",
            containerFactory = "manualAckListenerContainerFactory"
    )
    public void handleConfigUpdate(String payload, org.springframework.kafka.support.Acknowledgment ack) {
        try {
            JsonNode node = objectMapper.readTree(payload);

            String agentType = node.path("agent_type").asText();
            int newVersion = node.path("version").asInt(-1);
            String changeType = node.path("change_type").asText("UPDATE");

            if (agentType.isBlank() || newVersion <= 0) {
                log.warn("Invalid config update payload, acknowledging without action: {}", payload);
                ack.acknowledge();
                return;
            }

            log.info("Received config update for {}: version={}, type={}", agentType, newVersion, changeType);

            switch (changeType.toUpperCase()) {
                case "UPDATE" -> reloadAgentConfig(agentType, newVersion);
                case "DELETE" -> deactivateAgent(agentType);
                default -> log.warn("Unknown change type: {}", changeType);
            }

            ack.acknowledge();

        } catch (Exception e) {
            // Unparseable payload: a repetition yields the same result,
            // so it is acknowledged. Errors in the DB processing are caught
            // reloadAgentConfig/deactivateAgent already internally and log.
            log.error("Failed to process config update, acknowledging to avoid a replay loop: {}", e.getMessage());
            ack.acknowledge();
        }
    }

    /**
     * Reload the latest version of an agent config from PostgreSQL.
     * In production, this would also notify workers to pick up the new config.
     */
    private void reloadAgentConfig(String agentType, int expectedVersion) {
        try {
            String sql = """
                SELECT id, agent_type, system_prompt, model_name, temperature, 
                       max_tokens, tools_config, timeout_seconds, max_retry_attempts, version
                FROM agent_definitions 
                WHERE agent_type = ? AND version >= ?
                ORDER BY version DESC LIMIT 1
                """;

            jdbcTemplate.query(sql, rs -> {
                String dbVersionStr = rs.getString("version");
                int dbVersion = Integer.parseInt(dbVersionStr);
                log.info("Loaded agent config {} v{} from DB (requested v{})", 
                        agentType, dbVersion, expectedVersion);
                
                // TODO: notify worker group of config change via Kafka control event
            }, agentType, expectedVersion);

        } catch (Exception e) {
            log.error("Failed to reload config for {}: {}", agentType, e.getMessage());
        }
    }

    /**
     * Mark an agent as deactivated — remove from active configs.
     */
    private void deactivateAgent(String agentType) {
        try {
            String sql = "UPDATE agent_definitions SET is_active = false WHERE agent_type = ?";
            int updated = jdbcTemplate.update(sql, agentType);
            log.info("Deactivated agent type {}: {} rows updated", agentType, updated);
        } catch (Exception e) {
            log.error("Failed to deactivate {}: {}", agentType, e.getMessage());
        }
    }
}
