package de.augmentia.agentplatform.common.tenant;

/**
 * Tenant entity for multi-tenant isolation.
 */
public record Tenant(
        String id,
        String name,
        boolean active,
        BillingInfo billingInfo
) {
    public static final String DEFAULT_TENANT = "default-tenant";

    public static Tenant defaultTenant() {
        return new Tenant(DEFAULT_TENANT, "Default Tenant", true, null);
    }

    public record BillingInfo(
            String plan,
            java.time.Instant expiresAt,
            Integer maxWorkflows,
            Double monthlyQuotaUsd
    ) {}
}
