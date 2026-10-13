package de.augmentia.agentflow.app.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LlmConfigBridgeTest {

    private static final String[] KEYS = {
            "OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL",
            "QUAD_LLM_USER_AGENT", "QUAD_LLM_SESSION_ID", "QUAD_LLM_SESSION_HEADER", "QUAD_LLM_HEADERS"
    };

    @Test
    void bridgesOpenAiConfigWithNoHeaders() {
        withRestoredSystemProperties(() -> {
            new LlmConfigBridge("https://api.openai.com/v1", "secret-key-1234", "gpt-4o-mini",
                    new AgentFlowLlmHeaderProperties()).apply();

            assertThat(System.getProperty("OPENAI_BASE_URL")).isEqualTo("https://api.openai.com/v1");
            assertThat(System.getProperty("OPENAI_API_KEY")).isEqualTo("secret-key-1234");
            assertThat(System.getProperty("OPENAI_MODEL")).isEqualTo("gpt-4o-mini");
            // no headers configured → nothing set for the LangChain4j path
            assertThat(System.getProperty("QUAD_LLM_USER_AGENT")).isNull();
            assertThat(System.getProperty("QUAD_LLM_SESSION_ID")).isNull();
            assertThat(System.getProperty("QUAD_LLM_SESSION_HEADER")).isNull();
            assertThat(System.getProperty("QUAD_LLM_HEADERS")).isNull();
        });
    }

    @Test
    void mapsConfiguredHeadersToQuadKeys() {
        withRestoredSystemProperties(() -> {
            AgentFlowLlmHeaderProperties props = new AgentFlowLlmHeaderProperties();
            props.setFallbackRequestId("fixed");
            props.getHeaders().put("User-Agent", "my-agent (run {{requestId}})");
            props.getHeaders().put("x-session-id", "{{requestId}}");
            props.getHeaders().put("HTTP-Referer", "https://agentflow.local");

            new LlmConfigBridge("https://opencode.ai/zen/go/v1", "k", "m", props).apply();

            assertThat(System.getProperty("QUAD_LLM_USER_AGENT")).isEqualTo("my-agent (run fixed)");
            assertThat(System.getProperty("QUAD_LLM_SESSION_HEADER")).isEqualTo("x-session-id");
            assertThat(System.getProperty("QUAD_LLM_SESSION_ID")).isEqualTo("fixed");
            assertThat(System.getProperty("QUAD_LLM_HEADERS")).isEqualTo("HTTP-Referer=https://agentflow.local");
        });
    }

    @Test
    void staticAuthorizationHeaderGoesToExtras() {
        withRestoredSystemProperties(() -> {
            AgentFlowLlmHeaderProperties props = new AgentFlowLlmHeaderProperties();
            props.getHeaders().put("Authorization", "Bearer {{apiKey}}");

            new LlmConfigBridge("http://x/v1", "secret", "m", props).apply();

            assertThat(System.getProperty("QUAD_LLM_HEADERS")).isEqualTo("Authorization=Bearer secret");
            assertThat(System.getProperty("QUAD_LLM_SESSION_HEADER")).isNull();
        });
    }

    private void withRestoredSystemProperties(Runnable body) {
        // The bridge sets both the plain key and the vault.-prefixed key (highest ConfigReader precedence).
        String[] keys = new String[KEYS.length * 2];
        for (int i = 0; i < KEYS.length; i++) {
            keys[i * 2] = KEYS[i];
            keys[i * 2 + 1] = "vault." + KEYS[i];
        }
        String[] saved = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            saved[i] = System.getProperty(keys[i]);
            System.clearProperty(keys[i]);
        }
        try {
            body.run();
        } finally {
            for (int i = 0; i < keys.length; i++) {
                if (saved[i] == null) {
                    System.clearProperty(keys[i]);
                } else {
                    System.setProperty(keys[i], saved[i]);
                }
            }
        }
    }
}
