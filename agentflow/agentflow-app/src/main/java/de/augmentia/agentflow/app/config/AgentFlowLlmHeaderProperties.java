package de.augmentia.agentflow.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Configurable, partly-dynamic HTTP headers for <b>all</b> LLM calls (see {@code eys.md}).
 *
 * <p>Properties: {@code agentflow.llm.headers.<Name>=<Template>} (order preserved) and
 * {@code agentflow.llm.fallback-request-id}. Templates may contain placeholders, resolved per
 * request:</p>
 * <ul>
 *   <li>{@code {{requestId}}} / {@code {{sessionId}}} → current run/session id (fallback: configured
 *       fallback, then a stable per-process id)</li>
 *   <li>{@code {{apiKey}}} → the configured API key</li>
 * </ul>
 *
 * <p>Double braces ({@code {{…}}}) are intentional — {@code ${…}} would be resolved by Spring
 * itself.</p>
 */
@ConfigurationProperties(prefix = "agentflow.llm")
public class AgentFlowLlmHeaderProperties {

    public static final String REQUEST_ID_PLACEHOLDER = "{{requestId}}";
    public static final String SESSION_ID_PLACEHOLDER = "{{sessionId}}";
    public static final String API_KEY_PLACEHOLDER = "{{apiKey}}";

    /** Stable fallback id for the whole process (never blank). */
    static final String PROCESS_REQUEST_ID = "agentflow-" + UUID.randomUUID();

    private final Map<String, String> headers = new LinkedHashMap<>();
    private String fallbackRequestId;

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getFallbackRequestId() {
        return fallbackRequestId;
    }

    public void setFallbackRequestId(String fallbackRequestId) {
        this.fallbackRequestId = fallbackRequestId;
    }

    public boolean isConfigured() {
        return !headers.isEmpty();
    }

    public String effectiveRequestId(String requestId) {
        if (requestId != null && !requestId.isBlank()) {
            return requestId;
        }
        if (fallbackRequestId != null && !fallbackRequestId.isBlank()) {
            return fallbackRequestId;
        }
        return PROCESS_REQUEST_ID;
    }

    /** Resolves all configured headers for the given run id + api key. */
    public Map<String, String> resolve(String requestId, String apiKey) {
        String id = effectiveRequestId(requestId);
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : headers.entrySet()) {
            out.put(e.getKey(), resolveValue(e.getValue(), id, apiKey));
        }
        return out;
    }

    static String resolveValue(String template, String requestId, String apiKey) {
        if (template == null) {
            return "";
        }
        String id = requestId != null ? requestId : PROCESS_REQUEST_ID;
        String key = apiKey != null ? apiKey : "";
        return template.replace(REQUEST_ID_PLACEHOLDER, id)
                .replace(SESSION_ID_PLACEHOLDER, id)
                .replace(API_KEY_PLACEHOLDER, key);
    }

    public List<String> dynamicHeaderNames() {
        List<String> names = new ArrayList<>();
        for (Map.Entry<String, String> e : headers.entrySet()) {
            if (isDynamic(e.getValue())) {
                names.add(e.getKey());
            }
        }
        return names;
    }

    public boolean hasDynamicRequestIdHeader() {
        for (String value : headers.values()) {
            if (isDynamic(value)) {
                return true;
            }
        }
        return false;
    }

    static boolean isDynamic(String value) {
        return value != null && (value.contains(REQUEST_ID_PLACEHOLDER) || value.contains(SESSION_ID_PLACEHOLDER));
    }
}
