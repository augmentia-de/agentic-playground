package de.augmentia.agentplatform.worker.agent;

import de.augmentia.agentplatform.common.agent.AgentExecutor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class AgentExecutorRegistry {

    private final Map<String, AgentExecutor> executorsByType;
    private final AgentExecutor fallback;

    public AgentExecutorRegistry(List<AgentExecutor> executors) {
        Map<String, AgentExecutor> exact = new HashMap<>();
        AgentExecutor wildcard = null;
        for (AgentExecutor executor : executors) {
            String agentType = executor.agentType();
            if (agentType == null || agentType.isBlank()) {
                continue;
            }
            if ("*".equals(agentType)) {
                wildcard = executor;
            } else {
                exact.put(agentType.toLowerCase(Locale.ROOT), executor);
            }
        }
        this.executorsByType = Map.copyOf(exact);
        this.fallback = wildcard == null ? new DeterministicMockAgentExecutor() : wildcard;
    }

    public AgentExecutor executorFor(String agentType) {
        if (agentType == null || agentType.isBlank()) {
            return fallback;
        }
        return executorsByType.getOrDefault(agentType.toLowerCase(Locale.ROOT), fallback);
    }
}
