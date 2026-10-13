package de.augmentia.agentplatform.orchestrator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

@Component
public class StartIdempotencyGuard {

    private static final Logger log = LoggerFactory.getLogger(StartIdempotencyGuard.class);
    private static final String KEY_PREFIX = "dedup:workflow:start:";
    private static final Duration TTL = Duration.ofHours(24);

    private final ObjectProvider<StringRedisTemplate> redisProvider;

    public StartIdempotencyGuard(ObjectProvider<StringRedisTemplate> redisProvider) {
        this.redisProvider = redisProvider;
    }

    public Optional<String> reserve(String idempotencyKey, String workflowExecutionId) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Optional.empty();
        }
        StringRedisTemplate redis = redisProvider.getIfAvailable();
        if (redis == null) {
            return Optional.empty();
        }
        try {
            String redisKey = KEY_PREFIX + idempotencyKey;
            Boolean reserved = redis.opsForValue().setIfAbsent(redisKey, workflowExecutionId, TTL);
            if (Boolean.TRUE.equals(reserved)) {
                return Optional.empty();
            }
            return Optional.ofNullable(redis.opsForValue().get(redisKey));
        } catch (RuntimeException ex) {
            log.warn("Redis idempotency guard unavailable, falling back to database constraint", ex);
            return Optional.empty();
        }
    }
}
