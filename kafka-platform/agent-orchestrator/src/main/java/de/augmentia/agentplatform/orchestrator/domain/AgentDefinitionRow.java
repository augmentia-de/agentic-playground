package de.augmentia.agentplatform.orchestrator.domain;

public record AgentDefinitionRow(
        String agentType,
        String description,
        String systemPrompt,
        String modelName,
        double temperature,
        int maxTokens,
        int timeoutSeconds,
        int maxRetryAttempts,
        int version,
        int maxReActIterations
) {}
