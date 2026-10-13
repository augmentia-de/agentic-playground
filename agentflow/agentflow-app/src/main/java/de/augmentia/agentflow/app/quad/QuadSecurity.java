package de.augmentia.agentflow.app.quad;

import de.augmentia.quad.core.security.RoleBasedToolGuard;
import de.augmentia.quad.core.security.SecurityContext;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Role-based access control for agent tools, built on quad-core's
 * {@link RoleBasedToolGuard}. Tools not granted to any of the caller's roles are
 * <b>not registered</b> on the agent (deny by absence) — plus the guard is available
 * for the tool executor as defence in depth.
 *
 * <p>Roles → tools (default policy):</p>
 * <ul>
 *   <li>{@code reader}: readFile, listDirectory, grepSearch, findFiles</li>
 *   <li>{@code researcher}: reader + webSearch, webfetch</li>
 *   <li>{@code writer}: reader + writeFile</li>
 * </ul>
 */
public final class QuadSecurity {

    public static final Map<String, Set<String>> ROLE_TOOLS = Map.of(
            "reader", Set.of("readFile", "listDirectory", "grepSearch", "findFiles"),
            "researcher", Set.of("readFile", "listDirectory", "grepSearch", "findFiles", "webSearch", "webfetch"),
            "writer", Set.of("readFile", "listDirectory", "grepSearch", "findFiles", "writeFile"));

    private QuadSecurity() {
    }

    /** A minimal {@link SecurityContext} carrying the caller's roles. */
    public record Roles(Set<String> roles) implements SecurityContext {
        @Override public String userId() { return "agentflow"; }
        @Override public Set<String> roles() { return roles; }
        @Override public String sessionId() { return "agentflow"; }
        @Override public Map<String, String> claims() { return Map.of(); }
        @Override public boolean isAuthenticated() { return true; }
    }

    /** Tool → required roles, inverted from {@link #ROLE_TOOLS}; default deny. */
    public static RoleBasedToolGuard guard() {
        Map<String, Set<String>> toolRoles = new LinkedHashMap<>();
        ROLE_TOOLS.forEach((role, tools) -> tools.forEach(tool ->
                toolRoles.computeIfAbsent(tool, k -> new LinkedHashSet<>()).add(role)));
        return new RoleBasedToolGuard(toolRoles, Map.of(), false);
    }

    /** Built-in tools the given roles may use. */
    public static Set<String> allowedTools(Set<String> roles) {
        SecurityContext ctx = new Roles(roles != null ? Set.copyOf(roles) : Set.of());
        RoleBasedToolGuard guard = guard();
        Set<String> allowed = new LinkedHashSet<>();
        for (String tool : QuadCapabilities.BUILTIN_TOOLS) {
            if (guard.isAllowed(tool, ctx)) {
                allowed.add(tool);
            }
        }
        return allowed;
    }

    /** Human-readable role catalogue (for UI config). */
    public static Map<String, List<String>> catalogue() {
        Map<String, List<String>> out = new LinkedHashMap<>();
        ROLE_TOOLS.forEach((role, tools) -> out.put(role, List.copyOf(tools)));
        return out;
    }
}
