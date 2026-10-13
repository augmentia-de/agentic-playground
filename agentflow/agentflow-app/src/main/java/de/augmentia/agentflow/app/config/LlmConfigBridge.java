package de.augmentia.agentflow.app.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bridges the OpenAI configuration ({@code spring.ai.openai.*}) onto the keys that quad-core's
 * LangChain4j {@code ModelFactory}/{@code ModelHeaders} read ({@code OPENAI_*}, {@code QUAD_LLM_*}),
 * so that the Spring AI graph <i>and</i> the quad/LangChain4j agents use the same base URL, key and
 * model.
 *
 * <p>Headers are taken solely from {@code agentflow.llm.headers.*} (see
 * {@link AgentFlowLlmHeaderProperties}); if none are configured, <b>no</b> {@code QUAD_LLM_*} headers
 * are set. There is no provider-specific default.</p>
 *
 * <p>{@code ConfigReader} precedence is {@code vault.} → env → sysprop. The bridge therefore sets the
 * {@code vault.}-prefixed system property (highest precedence) so agentflow's configuration wins over
 * environment variables loaded from a shared {@code .env}.</p>
 */
@Component
public class LlmConfigBridge {

    private static final Logger log = LoggerFactory.getLogger(LlmConfigBridge.class);

    private final String baseUrl;
    private final String apiKey;
    private final String model;
    private final AgentFlowLlmHeaderProperties headerProperties;

    public LlmConfigBridge(@Value("${spring.ai.openai.base-url:}") String baseUrl,
                           @Value("${spring.ai.openai.api-key:}") String apiKey,
                           @Value("${spring.ai.openai.chat.options.model:}") String model,
                           AgentFlowLlmHeaderProperties headerProperties) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.model = model;
        this.headerProperties = headerProperties;
    }

    /** Copies the effective configuration to the keys quad-core reads. Runs before any model is built. */
    @PostConstruct
    public void apply() {
        set("OPENAI_BASE_URL", baseUrl);
        set("OPENAI_API_KEY", apiKey);
        set("OPENAI_MODEL", model);

        if (headerProperties.isConfigured()) {
            applyConfiguredHeaders();
        }

        String headersState = !headerProperties.isConfigured()
                ? "none"
                : headerProperties.dynamicHeaderNames().isEmpty()
                        ? "configured"
                        : "configured (dynamic: " + headerProperties.dynamicHeaderNames() + ")";
        log.info("LLM bridge: quad ModelFactory -> baseUrl={} model={} apiKey={} headers={}",
                baseUrl, model, mask(apiKey), headersState);
    }

    private void applyConfiguredHeaders() {
        Map<String, String> resolved = headerProperties.resolve(null, apiKey);

        String userAgent = resolved.get("User-Agent");
        if (userAgent != null) {
            set("QUAD_LLM_USER_AGENT", userAgent);
        }

        String sessionHeader = sessionHeaderName();
        if (sessionHeader != null) {
            set("QUAD_LLM_SESSION_HEADER", sessionHeader);
            set("QUAD_LLM_SESSION_ID", resolved.get(sessionHeader));
        }

        Map<String, String> extras = new LinkedHashMap<>(resolved);
        extras.remove("User-Agent");
        if (sessionHeader != null) {
            extras.remove(sessionHeader);
        }
        if (!extras.isEmpty()) {
            StringBuilder csv = new StringBuilder();
            extras.forEach((name, value) -> {
                if (csv.length() > 0) {
                    csv.append(',');
                }
                csv.append(name).append('=').append(value);
            });
            set("QUAD_LLM_HEADERS", csv.toString());
        }
    }

    /** The first configured header whose template is dynamic ({{requestId}}/{{sessionId}}). */
    private String sessionHeaderName() {
        for (Map.Entry<String, String> e : headerProperties.getHeaders().entrySet()) {
            if (!"User-Agent".equalsIgnoreCase(e.getKey())
                    && AgentFlowLlmHeaderProperties.isDynamic(e.getValue())) {
                return e.getKey();
            }
        }
        return null;
    }

    /**
     * Sets the value with the highest {@code ConfigReader} precedence. The {@code vault.}-prefixed
     * system property wins over environment variables (the start script loads the shared
     * {@code quad-quarkus/.env} into the environment), so agentflow's configuration is authoritative.
     * The plain key is set as well for readers that bypass {@code ConfigReader}.
     */
    private void set(String key, String value) {
        if (value != null && !value.isBlank()) {
            System.setProperty(key, value);
            System.setProperty("vault." + key, value);
        }
    }

    private static String mask(String key) {
        if (key == null || key.isBlank()) {
            return "(unset)";
        }
        return key.length() <= 8 ? "***" : key.substring(0, 6) + "...";
    }
}
