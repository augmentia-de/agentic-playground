package de.augmentia.quad.worker.config;

import java.util.Optional;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Reads the agent configuration from {@link WorkerProperties} (YAML/application.yml).
 *
 * <p>This source is always active and serves as a fallback when the database
 * (AgentConfigSource in {@code db} mode) yields no hit — and as the
 * sole source as long as the worker runs without a database
 * (property {@code worker.quad.agent-source=yaml} / default value).
 *
 * <p>The order of the sources is determined via {@link Order}:
 * {@code JdbcAgentConfigSource} ({@code @Order(1)}) is queried first,
 * this YAML source ({@code @Order(2)}) only as a fallback.
 */
@Component
@Order(2)
public class PropertiesAgentConfigSource implements AgentConfigSource {

    private final WorkerProperties properties;

    public PropertiesAgentConfigSource(WorkerProperties properties) {
        this.properties = properties;
    }

    @Override
    public Optional<WorkerProperties.AgentConfig> find(String agentType) {
        WorkerProperties.AgentConfig config = properties.agents().get(agentType);
        return Optional.ofNullable(config);
    }
}
