package de.augmentia.agentplatform.bridge.redis;

import de.augmentia.agentplatform.common.tenant.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Tracks which workflows were initiated by email.
 * Used by {@code MailResponseHandler} to send result emails back.
 *
 * <p>Tenant-aware key patterns (when tenant is specified):
 * <ul>
 *   <li>{@code email:initator:{tenant}:{workflowId}} initiator email address</li>
 *   <li>{@code email:intake:{tenant}:{intakeMessageId}} workflowId (for dedup / lookup)</li>
 * </ul>
 *
 * <p>Fallback to non-tenant pattern when tenant equals default-tenant.
 * TTL: 7 days (matches workflow execution window).
 */
@Service
public class EmailInitiatorStore {

    private static final Logger log = LoggerFactory.getLogger(EmailInitiatorStore.class);

    private static final String INTAINER_PREFIX = "email:initator:";
    private static final String INTAKE_LOOKUP_PREFIX = "email:intake:";
    private static final Duration TTL = Duration.ofDays(7);

    private final StringRedisTemplate redis;

    public EmailInitiatorStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Record that an email was received and links it to a workflow ID.
     * Uses default-tenant prefix if no explicit tenant provided.
     */
    public void storeInitiator(String workflowId, String initiatorEmail) {
        storeInitiator(workflowId, Tenant.DEFAULT_TENANT, initiatorEmail);
    }

    /**
     * Record that an email was received and links it to a workflow ID with tenant isolation.
     */
    public void storeInitiator(String workflowId, String tenantId, String initiatorEmail) {
        String key = buildIntainerKey(tenantId, workflowId);
        Boolean set = redis.opsForValue().setIfAbsent(key, initiatorEmail, TTL);
        if (Boolean.TRUE.equals(set)) {
            log.debug("Stored email initiator {} for workflow {} [tenant={}] ", initiatorEmail, workflowId, tenantId);
        } else {
            redis.expire(key, TTL);
        }
    }

    /**
     * Look up the initiator email for a given workflow ID.
     */
    public String lookupInitiator(String workflowId) {
        return lookupInitiator(workflowId, Tenant.DEFAULT_TENANT);
    }

    /**
     * Look up the initiator email for a given workflow ID with tenant isolation.
     */
    public String lookupInitiator(String workflowId, String tenantId) {
        String key = buildIntainerKey(tenantId, workflowId);
        return redis.opsForValue().get(key);
    }

    /**
     * Register that an intake message was received, for deduplication.
     */
    public void registerIntake(String messageId, String workflowId) {
        registerIntake(messageId, Tenant.DEFAULT_TENANT, workflowId);
    }

    /**
     * Register that an intake message was received with tenant isolation.
     */
    public void registerIntake(String messageId, String tenantId, String workflowId) {
        String key = buildIntakeLookupKey(tenantId, messageId);
        redis.opsForValue().set(key, workflowId, TTL);
    }

    /**
     * Check if an intake message has already been processed.
     */
    public boolean isAlreadyProcessed(String messageId) {
        return isAlreadyProcessed(messageId, Tenant.DEFAULT_TENANT);
    }

    /**
     * Check if an intake message has already been processed with tenant isolation.
     */
    public boolean isAlreadyProcessed(String messageId, String tenantId) {
        String key = buildIntakeLookupKey(tenantId, messageId);
        Boolean hasKey = redis.hasKey(key);
        return hasKey != null && hasKey;
    }

    /**
     * Cleanup: remove all email-related keys for a completed workflow.
     */
    public void cleanupWorkflow(String workflowId) {
        cleanupWorkflow(workflowId, Tenant.DEFAULT_TENANT);
    }

    /**
     * Cleanup: remove all email-related keys for a completed workflow with tenant isolation.
     */
    public void cleanupWorkflow(String workflowId, String tenantId) {
        String key = buildIntainerKey(tenantId, workflowId);
        redis.delete(key);
        log.debug("Cleaned up email keys for workflow {}", workflowId);
    }

    // ── Key builders ────────────────────────────────────────────────

    private String buildIntainerKey(String tenantId, String workflowId) {
        String base = INTAINER_PREFIX;
        if (Tenant.DEFAULT_TENANT.equals(tenantId)) {
            return base + workflowId;
        }
        return base + tenantId + ":" + workflowId;
    }

    private String buildIntakeLookupKey(String tenantId, String messageId) {
        String base = INTAKE_LOOKUP_PREFIX;
        if (Tenant.DEFAULT_TENANT.equals(tenantId)) {
            return base + messageId;
        }
        return base + tenantId + ":" + messageId;
    }
}
