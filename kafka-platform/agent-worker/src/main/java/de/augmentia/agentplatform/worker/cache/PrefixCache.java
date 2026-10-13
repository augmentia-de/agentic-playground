package de.augmentia.agentplatform.worker.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Base64;
import java.util.Map;

/**
 * Advanced context prefix cache for agent workers.
 *
 * <p>Reduces LLM token costs by caching byte-identical system prompts /
 * tool descriptions that are reused across many tasks. The cache key is
 * a SHA-256 hash of the prefix content — identical content = cache hit.
 *
 * <h3>Key patterns</h3>
 * <ul>
 *   <li>{@code prefix:cache:{sha256}} → Base64-encoded prompt text (TTL: 1h)</li>
 *   <li>{@code prefix:stats:hits} / {@code prefix:stats:misses} → counters</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 * <pre>
 *   String prefix = resolvePrefix(workflow, node);
 *   String cached = prefixCache.get(prefix);
 *   if (cached == null) {
 *       cached = fetchFromSource(prefix.getSource()); // file, URL, step output
 *       prefixCache.put(cached);
 *   }
 *   String fullPrompt = cached + userInput;
 * </pre>
 */
@Component
public class PrefixCache {

    private static final Logger log = LoggerFactory.getLogger(PrefixCache.class);

    private static final String CACHE_PREFIX = "prefix:cache:";
    private static final Duration TTL = Duration.ofHours(1);

    private final StringRedisTemplate redis;

    public PrefixCache(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Resolve a prefix value: first from Redis cache, then return the raw input.
     * The caller should populate the cache after first retrieval.
     */
    public String getOrFetch(String prefixHash, String fallback) {
        String key = CACHE_PREFIX + prefixHash;
        String cached = redis.opsForValue().get(key);
        if (cached != null) {
            incrementHits();
            log.debug("Prefix cache HIT for hash {}", prefixHash);
            return cached;
        }
        incrementMisses();
        log.debug("Prefix cache MISS for hash {} — using fallback", prefixHash);
        return fallback;
    }

    /**
     * Store a prefix value into Redis with TTL.
     * Idempotent — only sets if not already present.
     */
    public void put(String prefixHash, String value) {
        String key = CACHE_PREFIX + prefixHash;
        Boolean set = redis.opsForValue().setIfAbsent(key, value, TTL);
        if (Boolean.TRUE.equals(set)) {
            log.debug("Stored prefix in cache (hash={}, size={})", prefixHash, value.length());
        } else {
            redis.expire(key, TTL);
        }
    }

    /**
     * Bulk load commonly used prefixes at startup.
     */
    public void preload(Map<String, String> knownPrefixes) {
        for (var entry : knownPrefixes.entrySet()) {
            try {
                put(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.warn("Failed to preload prefix {}: {}", entry.getKey(), e.getMessage());
            }
        }
    }

    /**
     * Compute SHA-256 hash of a string for use as cache key.
     */
    public static String sha256Hash(String input) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /**
     * Create a stable hash from structured prefix components.
     * Hashes: {usePrefix}/{clearPrefix}/{cachePrefix}/{sourceType}:{sourceValue}
     */
    public static String stableHash(
            boolean usePrefix,
            boolean clearPrefix,
            String cachePrefix,
            String sourceType,
            String sourceValue
    ) {
        String raw = String.format("%b|%b|%s|%s|%s",
                usePrefix, clearPrefix,
                cachePrefix != null ? cachePrefix : "",
                sourceType, sourceValue);
        return sha256Hash(raw);
    }

    // ── Stats helpers ────────────────────────────────────────────────

    private void incrementHits() {
        redis.opsForValue().increment("prefix:stats:hits");
    }

    private void incrementMisses() {
        redis.opsForValue().increment("prefix:stats:misses");
    }

    /**
     * Get cache statistics.
     */
    public Map<String, Object> stats() {
        Long hits = redis.opsForValue().increment("prefix:stats:hits") - 1;
        Long misses = redis.opsForValue().increment("prefix:stats:misses") - 1;
        return Map.of(
                "hits", hits,
                "misses", misses,
                "hitRate", hits > 0 ? String.format("%.1f%%", hits * 100.0 / (hits + misses)) : "N/A"
        );
    }
}
