package de.augmentia.agentplatform.orchestrator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HashiCorp Vault integration — Token, AppRole, Kubernetes auth methods.
 */
@Component
@ConditionalOnProperty(name = "app.vault.enabled", havingValue = "true")
public class VaultConfig {

    private static final Logger log = LoggerFactory.getLogger(VaultConfig.class);

    @Value("${app.vault.url:http://vault:8200}")
    private String vaultUrl;

    @Value("${app.vault.token:${VAULT_TOKEN}}")
    private String vaultToken;

    @Value("${app.vault.path-prefix:secret/data}")
    private String pathPrefix;

    @Value("${app.vault.cache-ttl-minutes:60}")
    private long cacheTtlMinutes;

    @Value("${app.vault.auth-method:token}")
    private String authMethod;

    @Value("${app.vault.approle.role-id:#{null}}")
    private String roleId;

    @Value("${app.vault.approle.secret-id:#{null}}")
    private String secretId;

    private final Map<String, CachedSecret> secretCache = new ConcurrentHashMap<>();
    private final Environment env;
    private volatile String effectiveToken;

    public VaultConfig(Environment env) { this.env = env; }

    @PostConstruct
    public void init() {
        if (!isEnabled()) return;
        authenticate();
        preFillCommonPaths();
        log.info("Vault initialized: url={}, auth={}", vaultUrl, authMethod);
    }

    // ── Auth ────────────────────────────────────────────────────────

    private void authenticate() {
        try {
            switch (authMethod.toLowerCase()) {
                case "token" -> {
                    effectiveToken = vaultToken != null && !vaultToken.isBlank()
                            ? vaultToken
                            : System.getenv("VAULT_TOKEN");
                    if (effectiveToken == null) throw new IllegalStateException("No token");
                }
                case "approle" -> {
                    effectiveToken = doAppRoleLogin();
                }
                case "kubernetes" -> {
                    effectiveToken = doKubernetesLogin();
                }
                default -> throw new IllegalArgumentException("Unknown auth method: " + authMethod);
            }
            log.info("Vault auth successful via {}", authMethod);
        } catch (Exception e) {
            log.error("Vault authentication failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String doAppRoleLogin() {
        String rid = roleId != null ? roleId : System.getenv("VAULT_ROLE_ID");
        String sid = secretId != null ? secretId : System.getenv("VAULT_SECRET_ID");
        if (rid == null || sid == null) throw new IllegalStateException("AppRole needs role-id + secret-id");

        WebClient c = WebClient.builder().baseUrl(vaultUrl).build();
        Map<String, Object> body = Map.of("role_id", rid, "secret_id", sid);
        var resp = c.post().uri("/v1/auth/approle/login").bodyValue(body)
                .retrieve().bodyToMono(Map.class).block();

        Map<String, Object> auth = (Map<String, Object>) resp.get("auth");
        if (auth == null) throw new RuntimeException("Auth response missing");
        return auth.get("client_token").toString();
    }

    private String doKubernetesLogin() {
        String jwtPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";
        String jwt;
        try {
            jwt = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(jwtPath)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read K8s service account token: " + e.getMessage(), e);
        }
        String role = env.getProperty("app.vault.kubernetes-role", "agent-platform");

        WebClient c = WebClient.builder().baseUrl(vaultUrl).build();
        Map<String, Object> body = Map.of("jwt", jwt, "role", role);
        var resp = c.post().uri("/v1/auth/kubernetes/login").bodyValue(body)
                .retrieve().bodyToMono(Map.class).block();

        Map<String, Object> auth = (Map<String, Object>) resp.get("auth");
        if (auth == null) throw new RuntimeException("Auth response missing");
        return auth.get("client_token").toString();
    }

    // ── Pre-fill paths ──────────────────────────────────────────────

    private void preFillCommonPaths() {
        Set<VaultPath> paths = Set.of(
                new VaultPath("agent-platform/agent-orchestrator/database", "db credentials"),
                new VaultPath("agent-platform/shared/llm-api-keys", "LLM API keys"),
                new VaultPath("agent-platform/shared/oauth-secrets", "OAuth secrets"),
                new VaultPath("agent-platform/agent-orchestrator/redis", "Redis password")
        );

        for (VaultPath vp : paths) {
            try {
                String json = webClient().get()
                        .uri("/{path}", pathPrefix + "/" + vp.key)
                        .header("X-Vault-Token", effectiveToken)
                        .retrieve().bodyToMono(String.class).timeout(Duration.ofSeconds(10)).block();

                if (json != null && !json.contains("\"errors\"")) {
                    log.debug("Vault path {} already exists", vp.key);
                } else {
                    try {
                        webClient().post().uri("/v1/sys/mounts/secrets")
                                .header("X-Vault-Token", effectiveToken)
                                .bodyValue("{\"type\":\"kv-v2\"}").retrieve()
                                .toBodilessEntity().block();
                    } catch (Exception ignored) { /* may already exist */ }
                    log.info("Created Vault KV v2 mount at: {}", pathPrefix);
                }
            } catch (Exception ex) {
                log.warn("Vault path access failed ({}) - {}: {}", vp.key, vp.description, ex.getMessage());
            }
        }
    }

    // ── Secrets resolution ──────────────────────────────────────────

    public String resolve(String vaultKey) {
        CachedSecret entry = secretCache.get(vaultKey);
        if (entry != null && !entry.isExpired(cacheTtlMinutes)) {
            return entry.value;
        }

        try {
            Map<String, Object> resp = webClient().get()
                    .uri("/{path}", pathPrefix + "/" + vaultKey)
                    .header("X-Vault-Token", effectiveToken)
                    .retrieve().bodyToMono(Map.class).timeout(Duration.ofSeconds(10)).block();

            Map<String, Object> data = (Map<String, Object>) resp.get("data");
            if (data == null) return null;
            Object val = data.get(vaultKey);
            if (val == null) {
                // Try all keys in data
                for (String key : data.keySet()) {
                    if (key.equals("metadata")) continue;
                    val = data.get(key); break;
                }
            }
            String value = val != null ? val.toString() : null;
            if (value != null) secretCache.put(vaultKey, new CachedSecret(value, System.currentTimeMillis()));
            return value;
        } catch (Exception e) {
            log.error("Failed to resolve Vault key '{}': {}", vaultKey, e.getMessage());
            return entry != null ? entry.value : null;
        }
    }

    public Map<String, String> resolveAll(String path) {
        String cacheKey = path + "/all";
        CachedSecret entry = secretCache.get(cacheKey);
        if (entry != null && !entry.isExpired(cacheTtlMinutes)) {
            return parseCached(entry.value);
        }

        try {
            Map<String, Object> resp = webClient().get()
                    .uri("/{path}", pathPrefix + "/" + path)
                    .header("X-Vault-Token", effectiveToken)
                    .retrieve().bodyToMono(Map.class).timeout(Duration.ofSeconds(10)).block();

            Map<String, Object> data = (Map<String, Object>) resp.get("data");
            StringBuilder sb = new StringBuilder();
            if (data != null) {
                data.forEach((k, v) -> {
                    if (k.equals("metadata")) return;
                    if (sb.length() > 0) sb.append("\n");
                    sb.append(k).append("=").append(v);
                });
            }
            String serialized = sb.toString();
            secretCache.put(cacheKey, new CachedSecret(serialized, System.currentTimeMillis()));
            return parseCached(serialized);
        } catch (Exception e) {
            log.error("Failed to resolve Vault path '{}': {}", path, e.getMessage());
            return entry != null ? parseCached(entry.value) : Map.of();
        }
    }

    private Map<String, String> parseCached(String s) {
        Map<String, String> m = new ConcurrentHashMap<>();
        if (s == null || s.isBlank()) return m;
        for (String line : s.split("\n")) {
            int eq = line.indexOf('=');
            if (eq > 0) m.put(line.substring(0, eq), line.substring(eq + 1));
        }
        return m;
    }

    // ── Maintenance ──────────────────────────────────────────────────

    public void invalidate(String vaultKey) {
        secretCache.remove(vaultKey);
        secretCache.remove(vaultKey + "/all");
        log.info("Invalidated cached Vault key: {}", vaultKey);
    }

    public void refreshAll() { secretCache.clear(); log.info("Cleared all cached Vault secrets"); }

    public boolean isConnected() {
        try {
            Map<String, Object> resp = webClient().get()
                    .uri("/v1/sys/health")
                    .header("X-Vault-Token", effectiveToken)
                    .retrieve().bodyToMono(Map.class).block();

            Object initObj = resp.get("initialized");
            Object sealObj = resp.get("sealed");
            boolean init = initObj instanceof Boolean b ? b : Boolean.parseBoolean(initObj.toString());
            boolean sealed = sealObj instanceof Boolean b ? b : Boolean.parseBoolean(sealObj.toString());
            return init && !sealed;
        } catch (Exception e) {
            log.warn("Vault health check failed: {}", e.getMessage());
            return false;
        }
    }

    public boolean isEnabled() {
        return Boolean.parseBoolean(env.getProperty("app.vault.enabled", "false"));
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private WebClient webClient() {
        return WebClient.builder().baseUrl(vaultUrl).build();
    }

    record VaultPath(String key, String description) {}

    record CachedSecret(String value, long fetchedAt) {
        boolean isExpired(long ttlMinutes) {
            return Duration.between(java.time.Instant.ofEpochMilli(fetchedAt), java.time.Instant.now()).toMinutes() >= ttlMinutes;
        }
    }
}
