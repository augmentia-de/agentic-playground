package de.augmentia.agentplatform.orchestrator.config;

import de.augmentia.agentplatform.common.tenant.Tenant;
import de.augmentia.agentplatform.common.tenant.TenantContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Wraps JdbcTemplate to automatically apply tenant_id filtering to queries.
 * Every query executed through this wrapper gets WHERE tenant_id = ? appended,
 * with the current tenant ID from TenantContext (defaulting to 'default-tenant').
 */
@Component
public class TenantAwareJdbcTemplate {

    private final JdbcTemplate jdbcTemplate;

    public TenantAwareJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Execute a SELECT returning a list of mapped results with automatic tenant filtering.
     */
    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
        String modifiedSql = addTenantCondition(sql);
        Object[] newArgs = appendTenantArg(args);
        return jdbcTemplate.query(modifiedSql, rowMapper, newArgs);
    }

    /**
     * Execute a SELECT returning a single result with automatic tenant filtering.
     */
    public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) {
        String modifiedSql = addTenantCondition(sql);
        Object[] newArgs = appendTenantArg(args);
        return jdbcTemplate.queryForObject(modifiedSql, requiredType, newArgs);
    }

    /**
     * Execute an UPDATE/DELETE with automatic tenant filtering.
     */
    public int update(String sql, Object... args) {
        String modifiedSql = addTenantCondition(sql);
        Object[] newArgs = appendTenantArg(args);
        return jdbcTemplate.update(modifiedSql, newArgs);
    }

    /**
     * Execute without tenant filtering (for admin or system-level operations).
     */
    public JdbcTemplate getUnfiltered() {
        return jdbcTemplate;
    }

    // ── SQL mutation helpers ────────────────────────────────────────

    private String addTenantCondition(String sql) {
        String trimmed = sql.trim();
        if (!trimmed.startsWith("SELECT") && !trimmed.startsWith("UPDATE") && !trimmed.startsWith("DELETE")) {
            return sql;
        }

        String tenantId = getCurrentTenant();
        if (Tenant.DEFAULT_TENANT.equals(tenantId)) {
            return sql;
        }

        int whereIdx = findWhereClause(trimmed.toUpperCase());

        if (whereIdx > 0) {
            StringBuilder sb = new StringBuilder(sql.length() + 40);
            sb.append(sql.substring(0, whereIdx));
            String preceding = extractKeyword(trimmed, whereIdx);
            if ("WHERE".equalsIgnoreCase(preceding)) {
                sb.append("(tenant_id = ? AND ");
            } else {
                sb.append("tenant_id = ? AND ");
            }
            sb.append(trimmed.substring(whereIdx + preceding.length()));
            return sb.toString();
        }

        return trimmed + " WHERE tenant_id = ?";
    }

    private int findWhereClause(String upperSql) {
        int idx = upperSql.indexOf(" WHERE ");
        if (idx >= 0) return idx;
        idx = upperSql.indexOf("\nWHERE ");
        if (idx >= 0) return idx + 1;
        idx = upperSql.indexOf(" FROM ");
        if (idx >= 0) return idx;
        return -1;
    }

    private String extractKeyword(String originalSql, int upperIdx) {
        int start = upperIdx;
        while (start > 0 && originalSql.charAt(start - 1) != ' ' && originalSql.charAt(start - 1) != '\n') {
            start--;
        }
        String word = "";
        for (int i = start; i <= upperIdx; i++) {
            word += originalSql.charAt(i);
        }
        return word;
    }

    private Object[] appendTenantArg(Object[] args) {
        String tenantId = getCurrentTenant();
        if (args.length == 0) {
            return new Object[]{tenantId};
        }
        Object[] newArgs = new Object[args.length + 1];
        System.arraycopy(args, 0, newArgs, 0, args.length);
        newArgs[args.length] = tenantId;
        return newArgs;
    }

    private String getCurrentTenant() {
        String tenant = TenantContext.getCurrentTenant();
        return tenant != null ? tenant : Tenant.DEFAULT_TENANT;
    }
}
