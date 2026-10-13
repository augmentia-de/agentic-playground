package de.augmentia.quad.worker.session;

import de.augmentia.quad.core.security.SecurityContext;

import java.util.Map;
import java.util.Set;

/**
 * Simple SecurityContext implementation for the worker.
 * Instantiated per task with the information extracted from the TaskEvent.
 */
public class WorkerSecurityContext implements SecurityContext {

    private final String userId;
    private final Set<String> roles;
    private final String sessionId;
    private final Map<String, String> claims;

    public WorkerSecurityContext(String userId, Set<String> roles, String sessionId) {
        this(userId, roles, sessionId, Map.of());
    }

    public WorkerSecurityContext(String userId, Set<String> roles, String sessionId, Map<String, String> claims) {
        this.userId = userId != null ? userId : "anonymous";
        this.roles = roles != null ? roles : Set.of();
        this.sessionId = sessionId;
        this.claims = claims != null ? Map.copyOf(claims) : Map.of();
    }

    @Override
    public String userId() { return userId; }

    @Override
    public Set<String> roles() { return roles; }

    @Override
    public String sessionId() { return sessionId; }

    @Override
    public Map<String, String> claims() { return claims; }

    @Override
    public boolean isAuthenticated() { return !userId.equals("anonymous"); }
}