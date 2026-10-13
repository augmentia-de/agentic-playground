package de.augmentia.quad.worker.tool;

import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.tool.QuadToolRegistry;
import de.augmentia.quad.core.tool.ToolArgsMapper;
import de.augmentia.quad.core.tool.ToolMethod;

import java.util.Set;

/**
 * Creates a new QuadToolRegistry copy per task with the released tools.
 */
public class AgentToolRegistryFactory {

    private final ToolCatalog catalog;
    private final ToolArgsMapper argsMapper;

    public AgentToolRegistryFactory(ToolCatalog catalog, ToolArgsMapper argsMapper) {
        this.catalog = catalog;
        this.argsMapper = argsMapper;
    }

    /**
     * Creates a new registry for an agent.
     * - first registers the agent's own tools (via registerFromAgent)
     * - then adds the allowed global tools
     */
    public QuadToolRegistry create(Agent agent, Set<String> allowedTools) {
        QuadToolRegistry registry = new QuadToolRegistry(argsMapper);
        registry.registerFromAgent(agent);

        allowedTools.stream()
            .sorted()
            .forEach(name -> registry.register(name, catalog.require(name)));

        return registry;
    }

    /**
     * Creates a registry with the allowed catalog tools only, without registering the
     * agent's own tools.
     *
     * <p>Used together with
     * {@link de.augmentia.quad.core.agent.AgentBuilder#withToolRegistry}: the builder
     * registers the agent's own {@code @Tool} methods, so pre-registering them here would
     * trigger a duplicate-name error.</p>
     */
    public QuadToolRegistry createCatalogOnly(Set<String> allowedTools) {
        QuadToolRegistry registry = new QuadToolRegistry(argsMapper);
        if (allowedTools != null) {
            allowedTools.stream()
                .sorted()
                .forEach(name -> registry.register(name, catalog.require(name)));
        }
        return registry;
    }
}