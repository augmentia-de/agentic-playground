package de.augmentia.agentflow.app.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AgentFlowLlmHeaderPropertiesTest {

    @Test
    void partiallyDynamicKeepsStaticText() {
        AgentFlowLlmHeaderProperties p = new AgentFlowLlmHeaderProperties();
        p.getHeaders().put("User-Agent", "agentflow/1.0 (run {{requestId}})");
        assertThat(p.resolve("run-9", "k")).containsEntry("User-Agent", "agentflow/1.0 (run run-9)");
    }

    @Test
    void nullRunIdUsesProcessFallback() {
        AgentFlowLlmHeaderProperties p = new AgentFlowLlmHeaderProperties();
        p.getHeaders().put("x-session-id", "{{requestId}}");
        assertThat(p.resolve(null, "k").get("x-session-id")).startsWith("agentflow-");
    }

    @Test
    void configuredFallbackWins() {
        AgentFlowLlmHeaderProperties p = new AgentFlowLlmHeaderProperties();
        p.setFallbackRequestId("fixed");
        p.getHeaders().put("x-session-id", "{{requestId}}");
        assertThat(p.resolve(null, "k")).containsEntry("x-session-id", "fixed");
    }

    @Test
    void apiKeyPlaceholderIsSubstituted() {
        AgentFlowLlmHeaderProperties p = new AgentFlowLlmHeaderProperties();
        p.getHeaders().put("Authorization", "Bearer {{apiKey}}");
        assertThat(p.resolve("run", "secret")).containsEntry("Authorization", "Bearer secret");
        assertThat(p.resolve("run", null)).containsEntry("Authorization", "Bearer ");
    }

    @Test
    void staticHeaderUnchangedAndOrderPreserved() {
        AgentFlowLlmHeaderProperties p = new AgentFlowLlmHeaderProperties();
        p.getHeaders().put("HTTP-Referer", "https://agentflow.local");
        p.getHeaders().put("x-session-id", "{{sessionId}}");
        assertThat(p.resolve("r1", "k").keySet()).containsExactly("HTTP-Referer", "x-session-id");
    }

    @Test
    void detectsDynamicHeaders() {
        AgentFlowLlmHeaderProperties p = new AgentFlowLlmHeaderProperties();
        assertThat(p.hasDynamicRequestIdHeader()).isFalse();
        p.getHeaders().put("x-session-id", "{{requestId}}");
        assertThat(p.hasDynamicRequestIdHeader()).isTrue();
        assertThat(p.dynamicHeaderNames()).containsExactly("x-session-id");
        assertThat(p.isConfigured()).isTrue();
    }
}
