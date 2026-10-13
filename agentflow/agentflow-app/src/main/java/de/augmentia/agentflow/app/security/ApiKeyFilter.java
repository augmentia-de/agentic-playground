package de.augmentia.agentflow.app.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Opt-in API-key gate for {@code /agentflow/**}. Accepts {@code X-API-Key} or
 * {@code Authorization: Bearer <key>}. Health endpoints stay open. When enabled, at least one key
 * must be configured (otherwise every request is rejected — fail closed).
 */
@Component
@ConditionalOnProperty(name = "agentflow.security.enabled", havingValue = "true")
public class ApiKeyFilter extends OncePerRequestFilter {

    private final Set<String> keys;

    public ApiKeyFilter(@Value("${agentflow.security.api-keys:}") String keysCsv) {
        this.keys = Arrays.stream(keysCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getRequestURI();
        if (path.startsWith("/actuator/health") || path.equals("/actuator/info")) {
            chain.doFilter(request, response);
            return;
        }
        if (authenticated(request)) {
            chain.doFilter(request, response);
            return;
        }
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"unauthorized\",\"hint\":\"send X-API-Key or Authorization: Bearer\"}");
    }

    private boolean authenticated(HttpServletRequest request) {
        if (keys.isEmpty()) {
            return false;
        }
        String key = request.getHeader("X-API-Key");
        if (key == null) {
            String authorization = request.getHeader("Authorization");
            if (authorization != null && authorization.startsWith("Bearer ")) {
                key = authorization.substring("Bearer ".length());
            }
        }
        return key != null && keys.contains(key);
    }
}
