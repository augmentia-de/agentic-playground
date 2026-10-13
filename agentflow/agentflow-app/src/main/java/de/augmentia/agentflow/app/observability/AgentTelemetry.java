package de.augmentia.agentflow.app.observability;

import de.augmentia.quad.core.agent.runtime.AgentEventPublisher;
import de.augmentia.quad.core.observability.AgentMetrics;
import de.augmentia.quad.core.observability.MetricsHook;

/**
 * Wires quad-core's {@link MetricsHook} into an agent's event publisher so the shared
 * {@link AgentMetrics} (Micrometer → {@code /actuator/prometheus}) is actually recorded.
 *
 * <p>Without this the {@code agent_*} meters exist (registered by {@code MetricsConfig}) but stay
 * at zero, because the hook is only attached where it is explicitly added.</p>
 */
public final class AgentTelemetry {

    private AgentTelemetry() {
    }

    /** A fresh event publisher that records {@code agent_*} metrics for a single agent run. */
    public static AgentEventPublisher metricsPublisher(AgentMetrics metrics) {
        AgentEventPublisher publisher = new AgentEventPublisher();
        if (metrics != null) {
            publisher.addEventListener(new MetricsHook(metrics)::onEvent);
        }
        return publisher;
    }
}
