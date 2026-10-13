package de.augmentia.agentflow.app.security;

/** Request-scoped tenant id (from the {@code X-Tenant-Id} header), used to scope capabilities. */
public final class TenantContext {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private TenantContext() {
    }

    public static String current() {
        return CURRENT.get();
    }

    public static void set(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            CURRENT.remove();
        } else {
            CURRENT.set(tenantId);
        }
    }

    public static void clear() {
        CURRENT.remove();
    }
}
