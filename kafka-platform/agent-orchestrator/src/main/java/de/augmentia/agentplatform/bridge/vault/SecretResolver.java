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

    @Value("${app.vault.url:http://vault:8200}")
    private String vaultUrl;
    @Value("${app.vault.token:${VAULT_TOKEN}}")
    private String authToken;
    @Value("${app.vault.path-prefix:secret/data}")
    private String pathPrefix;
    @Value("${app.vault.cache-ttl-minutes:60}")
    private long cacheTtlMinutes;
    @Value("${app.vault.timeout-seconds:10}")
    private int timeoutSeconds;

    private final WebClient webClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, CachedSecret> secretCache = new ConcurrentHashMap<>();

    public SecretResolver(WebClient.Builder webClientBuilder) {
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
            JsonNode response = webClient.get()
                    .uri("/{path}", pathPrefix + "/" + secretPath)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .block();

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
            JsonNode response = webClient.get()
                    .uri("/{path}", pathPrefix + "/" + secretPath)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .block();

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
            JsonNode response = webClient.get()
                    .uri("/v1/sys/health")
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .block();

            return response.path("initialized").asBoolean(false) && 
                   response.path("sealed").asBoolean(true);
        } catch (Exception e) {
            log.warn("Vault health check failed: {}", e.getMessage());
            return false;
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
