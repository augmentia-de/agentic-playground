package de.augmentia.quad.worker.config;

import java.util.Optional;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Source for a single agent configuration.
 *
 * <p>Multiple sources are injected as an ordered list. The first source
 * that yields a hit wins (see {@link WorkerAgentFactory}).
 */
public interface AgentConfigSource {

    /**
     * Looks up the configuration for the given {@code agentType}.
     *
     * @param agentType type of the agent (e.g. {@code researcher})
     * @return the configuration or {@link Optional#empty()} if this source
     *         does not know the type (then the next source resp. the
     *         YAML fallback applies)
     */
    Optional<WorkerProperties.AgentConfig> find(String agentType);

    /**
     * Display name of the source (for logs/diagnostics).
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
