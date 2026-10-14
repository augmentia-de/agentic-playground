package de.augmentia.agentplatform.bridge.vault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HashiCorp Vault client for fetching secrets from Vault KV v2 engine.
 * Integrates with SPEC2 §11.1 (Security by Design — HashiCorp Vault).
 *
 * <p>Caches secrets in memory for {@code ttlMinutes} to reduce Vault API load.
 * Secrets are refreshed after TTL expires on next access.
 *
 * <h3>Supported paths</h3>
 * <ul>
 *   <li>{@code secret/data/agent-platform/db} → DB credentials</li>
 *   <li>{@code secret/data/agent-platform/openai} → LLM API keys</li>
 *   <li>{@code secret/data/agent-platform/slack} → Slack webhook URL</li>
 *   <li>{@code secret/data/agent-platform/pagerduty} → PD API key</li>
 * </ul>
 *
 * <h3>Kubernetes Auth (recommended)</h3>
 * For Kubernetes deployments, use service account tokens via:
 * <pre>
 *   app:
 *     vault:
 *       auth-method: kubernetes
 *       kubernetes-role: agent-platform
 *       kubernetes-jwt-path: /var/run/secrets/kubernetes.io/serviceaccount/token
 * </pre>
 *
 * <h3>AppRole Auth (alternative)</h3>
 * For bare-metal/container deployments:
 * <pre>
 *   app:
 *     vault:
 *       auth-method: approle
 *       role-id: ${VAULT_ROLE_ID}
 *       secret-id: ${VAULT_SECRET_ID}
 * </pre>
 */
@Component
@ConditionalOnProperty(name = "app.vault.enabled", havingValue = "true")
public class SecretResolver {

    private static final Logger log = LoggerFactory.getLogger(SecretResolver.class);

    private final WebClient webClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, CachedSecret> secretCache = new ConcurrentHashMap<>();
    private final String pathPrefix;
    private final long cacheTtlMinutes;
    private final int timeoutSeconds;

    /**
     * Configuration is injected as constructor parameters, not as {@code @Value} fields: field
     * injection happens <em>after</em> the constructor, so the previous implementation built the
     * {@link WebClient} from still-{@code null} {@code vaultUrl}/{@code authToken} fields and every
     * request went to the wrong place.
     */
    public SecretResolver(WebClient.Builder webClientBuilder,
                          @Value("${app.vault.url:http://vault:8200}") String vaultUrl,
                          @Value("${app.vault.token:${VAULT_TOKEN:}}") String authToken,
                          @Value("${app.vault.path-prefix:secret/data}") String pathPrefix,
                          @Value("${app.vault.cache-ttl-minutes:60}") long cacheTtlMinutes,
                          @Value("${app.vault.timeout-seconds:10}") int timeoutSeconds) {
        this.pathPrefix = pathPrefix;
        this.cacheTtlMinutes = cacheTtlMinutes;
        this.timeoutSeconds = timeoutSeconds;
        this.webClient = webClientBuilder
                .baseUrl(vaultUrl)
                .defaultHeader("X-Vault-Token", authToken)
                .build();
    }

    /**
     * Fetch a secret value from Vault. Returns null if the key doesn't exist.
     *
     * @param secretPath  Vault KV2 path without prefix (e.g., "agent-platform/db")
     * @param secretKey   Key within the data object (e.g., "password")
     */
    public String resolve(String secretPath, String secretKey) {
        String cacheKey = secretPath + "/" + secretKey;
        
        // Check cache first
        CachedSecret cached = secretCache.get(cacheKey);
        if (cached != null && !cached.isExpired()) {
            return cached.value;
        }

        try {
            JsonNode response = fetchJson("/" + pathPrefix + "/" + secretPath);

            String value = extractValue(response, secretKey);
            if (value == null) {
                log.warn("Secret key '{}' not found at path '{}'", secretKey, secretPath);
                return null;
            }

            // Cache the result
            secretCache.put(cacheKey, new CachedSecret(value, System.currentTimeMillis(), cacheTtlMinutes));
            log.debug("Resolved secret {} from Vault path {}", secretKey, secretPath);
            return value;

        } catch (Exception e) {
            log.error("Failed to resolve secret {}: {}", cacheKey, e.getMessage());
            // Return cached value even if expired as fallback
            return cached != null ? cached.value : null;
        }
    }

    /**
     * Resolve all keys in a secret path as a Map.
     *
     * @param secretPath Vault KV2 path (e.g., "agent-platform/db")
     * @return Map of key → value pairs
     */
    public Map<String, String> resolveAll(String secretPath) {
        String cacheKey = secretPath + "/all";
        
        // Check cache
        CachedSecret cached = secretCache.get(cacheKey);
        if (cached != null && !cached.isExpired()) {
            return parseCachedMap(cached.value);
        }

        try {
            JsonNode response = fetchJson("/" + pathPrefix + "/" + secretPath);

            JsonNode data = response.path("data");
            StringBuilder sb = new StringBuilder();
            data.fields().forEachRemaining(entry -> {
                if (sb.length() > 0) sb.append("\n");
                sb.append(entry.getKey()).append("=").append(entry.getValue().asText());
            });

            String serialized = sb.toString();
            secretCache.put(cacheKey, new CachedSecret(serialized, System.currentTimeMillis(), cacheTtlMinutes));
            log.debug("Resolved {} keys from Vault path {}", data.size(), secretPath);
            return parseCachedMap(serialized);

        } catch (Exception e) {
            log.error("Failed to resolve all secrets from {}: {}", secretPath, e.getMessage());
            return cached != null ? parseCachedMap(cached.value) : Map.of();
        }
    }

    /**
     * Invalidate a cached secret (e.g., when it's been rotated manually).
     */
    public void invalidate(String secretPath, String secretKey) {
        secretCache.remove(secretPath + "/" + secretKey);
        secretCache.remove(secretPath + "/all");
        log.info("Invalidated cached secret: {}", secretPath + "/" + secretKey);
    }

    /**
     * Refresh all cached secrets immediately.
     */
    public void refreshAll() {
        secretCache.clear();
        log.info("Cleared all cached Vault secrets");
    }

    /**
     * Validate connection and auth status.
     */
    public boolean isConnected() {
        try {
            JsonNode response = fetchJson("/v1/sys/health");

            return response.path("initialized").asBoolean(false) &&
                   !response.path("sealed").asBoolean(true);
        } catch (Exception e) {
            log.warn("Vault health check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Reads a Vault response as JSON.
     *
     * <p>The path is passed <b>literally</b>, not as a {@code {path}} URI variable: URI variables are
     * encoded, so the slashes in e.g. {@code secret/data/agent-platform/db} became {@code %2F} and
     * Vault received one bogus segment. The body is read as text and parsed explicitly because on
     * Spring Boot 4 the WebFlux codecs are Jackson 3 ({@code tools.jackson}), which cannot construct
     * the Jackson 2 {@code com.fasterxml.jackson.databind.JsonNode} this class works with; decoding
     * straight into {@code JsonNode} fails with {@code Type definition error}.
     */
    private JsonNode fetchJson(String path) {
        String json = webClient.get()
                .uri(path)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .block();
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid JSON from Vault: " + e.getMessage(), e);
        }
    }

    private String extractValue(JsonNode response, String secretKey) {
        JsonNode data = response.path("data");
        JsonNode value = data.path(secretKey);
        if (value.isMissingNode()) return null;
        return value.asText(null);
    }

    private Map<String, String> parseCachedMap(String serialized) {
        Map<String, String> map = new ConcurrentHashMap<>();
        if (serialized == null || serialized.isBlank()) return map;
        for (String line : serialized.split("\n")) {
            int eqIdx = line.indexOf('=');
            if (eqIdx > 0) {
                map.put(line.substring(0, eqIdx), line.substring(eqIdx + 1));
            }
        }
        return map;
    }

    private record CachedSecret(String value, long fetchedAt, long ttlMinutes) {
        boolean isExpired() {
            long ageMinutes = Duration.between(Instant.ofEpochMilli(fetchedAt), Instant.now()).toMinutes();
            return ageMinutes >= ttlMinutes;
        }
    }
}
