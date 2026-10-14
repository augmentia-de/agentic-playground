package de.augmentia.quad.core.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RFC 8693 Token Exchange client.
 * Exchanges an incoming user token for a new token scoped to a target audience (MCP server).
 * <p>
 * Flow:
 * <ol>
 *   <li>User calls quad with token aud=quad-api</li>
 *   <li>quad calls Keycloak: exchange(user_token, audience=mcp-server)</li>
 *   <li>Keycloak returns new token with aud=mcp-server + act claim</li>
 *   <li>quad uses new token to call MCP server</li>
 * </ol>
 * <p>
 * In production, this should be replaced by Quarkus OIDC Client with grant.type=exchange.
 * This implementation works standalone for testing and non-Quarkus environments.
 */
public class TokenExchangeClient {

    private static final Logger log = Logger.getLogger(TokenExchangeClient.class);

    private final TokenExchangeConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    /**
     * Cached tokens keyed by audience + subject-token fingerprint + scopes.
     *
     * <p>The subject token is stored only as a SHA-256 fingerprint, so an exchanged token is never
     * served for a different subject (the previous audience-only key leaked tokens across subjects)
     * and a heap dump does not reveal the raw subject token.</p>
     */
    private final Map<CacheKey, CachedToken> tokenCache = new ConcurrentHashMap<>();

    public TokenExchangeClient(TokenExchangeConfig config) {
        this(config, HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build());
    }

    /** Test seam: allows injecting an {@link HttpClient} without a running identity provider. */
    TokenExchangeClient(TokenExchangeConfig config, HttpClient httpClient) {
        this.config = config;
        this.httpClient = httpClient;
        this.mapper = new ObjectMapper();
    }

    /**
     * Exchange the given subject token for a new token scoped to the target audience.
     *
     * @param subjectToken the incoming user token (JWT)
     * @param targetAudience the MCP server or service to target
     * @param scopes requested scopes (optional, may be empty)
     * @return the exchanged token string, or null on failure
     */
    public String exchange(String subjectToken, String targetAudience, String... scopes) {
        if (subjectToken == null || subjectToken.isBlank()) {
            log.warn("Token exchange skipped: no subject token");
            return null;
        }
        if (config.issuer() == null || config.issuer().isBlank()) {
            log.warn("Token exchange skipped: no issuer configured");
            return subjectToken;
        }

        String[] effectiveScopes = scopes.length > 0 ? scopes : config.scopes();
        CacheKey cacheKey = new CacheKey(
            targetAudience, fingerprint(subjectToken), String.join(" ", effectiveScopes));
        CachedToken cached = tokenCache.get(cacheKey);
        if (cached != null && !cached.isExpired()) {
            return cached.token();
        }

        try {
            String tokenEndpoint = config.issuer().endsWith("/")
                ? config.issuer() + "protocol/openid-connect/token"
                : config.issuer() + "/protocol/openid-connect/token";

            StringBuilder body = new StringBuilder();
            body.append("grant_type=").append(encode("urn:ietf:params:oauth:grant-type:token-exchange"));
            body.append("&subject_token=").append(encode(subjectToken));
            body.append("&subject_token_type=").append(encode(config.subjectTokenType()));
            body.append("&requested_token_type=").append(encode(config.requestedTokenType()));
            body.append("&audience=").append(encode(targetAudience));

            if (effectiveScopes.length > 0) {
                body.append("&scope=").append(encode(String.join(" ", effectiveScopes)));
            }

            String authHeader = "Basic " + Base64.getEncoder().encodeToString(
                (config.clientId() + ":" + config.clientSecret()).getBytes(StandardCharsets.UTF_8));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .timeout(Duration.ofSeconds(5))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode json = mapper.readTree(response.body());
                String newToken = json.get("access_token").asText();
                long expiresIn = json.has("expires_in") ? json.get("expires_in").asLong() : config.tokenLifetimeSeconds();

                tokenCache.put(cacheKey, new CachedToken(newToken, System.currentTimeMillis() + expiresIn * 1000));
                log.debugf("Token exchange OK: audience=%s, expiresIn=%ds", targetAudience, expiresIn);
                return newToken;
            } else {
                log.errorf("Token exchange failed: HTTP %d, body=%s", response.statusCode(),
                    response.body() != null ? response.body().substring(0, Math.min(200, response.body().length())) : "null");
                return null;
            }
        } catch (Exception e) {
            log.errorf(e, "Token exchange failed for audience=%s", targetAudience);
            return null;
        }
    }

    /**
     * Build auth headers for an MCP server connection.
     * Returns headers map with "Authorization" = "Bearer <exchanged_token>".
     * If exchange is disabled or fails, returns empty map.
     */
    public Map<String, String> buildAuthHeaders(String subjectToken, String targetAudience) {
        String exchanged = exchange(subjectToken, targetAudience);
        if (exchanged == null) return Map.of();
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + exchanged);
        return headers;
    }

    /** Clear token cache (e.g. on reinit). */
    public void clearCache() {
        tokenCache.clear();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /** SHA-256 fingerprint of the subject token — never the raw token as a cache key. */
    private static String fingerprint(String subjectToken) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                .digest(subjectToken.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(subjectToken.hashCode());
        }
    }

    /** Cache identity: a token is reused only for the same audience, subject and scopes. */
    private record CacheKey(String audience, String subjectFingerprint, String scopes) {}

    private record CachedToken(String token, long expiresAtMs) {
        boolean isExpired() {
            return System.currentTimeMillis() > expiresAtMs - 30_000; // 30s buffer
        }
    }
}
