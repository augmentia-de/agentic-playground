package de.augmentia.agentplatform.common.tenant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThreadLocal-based tenant context for request-scoped tenant extraction.
 * Extracts the tenant ID from the X-Tenant-ID request header (JWT claim),
 * falls back to {@link Tenant#DEFAULT_TENANT} if no tenant is specified.
 */
public final class TenantContext {

    private static final Logger log = LoggerFactory.getLogger(TenantContext.class);
    private static final ThreadLocal<String> CURRENT_TENANT = new ThreadLocal<>();
    public static final String HEADER_TENANT_ID = "X-Tenant-ID";

    private TenantContext() {}

    /** Set the current tenant for this thread (called by TenantInterceptor). */
    public static void setCurrentTenant(String tenantId) {
        CURRENT_TENANT.set(tenantId != null && !tenantId.isBlank() ? tenantId : Tenant.DEFAULT_TENANT);
    }

    /** Get the current tenant ID, or default-tenant if not set. */
    public static String getCurrentTenant() {
        String tenant = CURRENT_TENANT.get();
        return tenant != null ? tenant : Tenant.DEFAULT_TENANT;
    }

    /** Clear the tenant context — should be called after request processing. */
    public static void clear() {
        CURRENT_TENANT.remove();
    }

    /** Reset the thread-local to the default tenant. */
    public static void resetToDefault() {
        CURRENT_TENANT.set(Tenant.DEFAULT_TENANT);
    }
}
