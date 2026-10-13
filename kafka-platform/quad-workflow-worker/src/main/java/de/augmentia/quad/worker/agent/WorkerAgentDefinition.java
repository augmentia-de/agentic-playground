package de.augmentia.quad.worker.agent;

import java.util.Set;

/**
 * Configuration for an agent type in the worker.
 * Loaded from application.yml or from an AgentCatalog.
 */
public record WorkerAgentDefinition(
    String agentType,
    String agentId,
    String systemPrompt,
    String modelKey,
    Double temperature,
    Integer maxOutputTokens,
    Set<String> allowedTools,
    int maxReActIterations,
    long timeoutMs,
    String jsonOutputSchema,
    boolean structuredOutputEnabled
) {
    /**
     * Default values if not specified.
     */
    public WorkerAgentDefinition(String agentType, String agentId, String modelKey, Set<String> allowedTools) {
        this(agentType, agentId, null, modelKey, 0.2, null, allowedTools, 10, 120000L, null, true);
    }
}