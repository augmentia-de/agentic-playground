package de.augmentia.agentplatform.orchestrator.security;

import de.augmentia.agentplatform.common.tenant.TenantContext;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.crypto.SecretKey;
import java.util.Base64;
import java.util.List;

/**
 * JWT authentication filter — validates Bearer tokens on every request.
 * Extracts `sub` and `tenant` claims and sets them into SecurityContext + TenantContext.
 */
@Component
@Order(10)
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthenticationFilter.class);
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Value("${app.security.jwt.secret-base64}")
    private String secretBase64;

    /** Paths that bypass authentication checks entirely */
    private static final String[] PUBLIC_PATHS = {
            "/actuator/", "/health", "/favicon.ico",
            "/api/v1/auth/"  // auth endpoints (register, login)
    };

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, java.io.IOException {

        // 1. Skip public paths
        String path = req.getRequestURI();
        for (String pub : PUBLIC_PATHS) {
            if (path.startsWith(pub)) {
                chain.doFilter(req, resp);
                return;
            }
        }

        // 2. Extract Bearer token from header
        String token = extractToken(req);
        if (token == null || token.isBlank()) {
            chain.doFilter(req, resp);
            return;
        }

        // 3. Validate & populate SecurityContext
        try {
            SecretKey key = Keys.hmacShaKeyFor(Base64.getDecoder().decode(secretBase64));
            
            Claims claims = Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();

            String subject = claims.getSubject();
            String tenant = claims.get("tenant", String.class);

            if (subject != null && !subject.isBlank()) {
                UsernamePasswordAuthenticationToken auth =
                        new UsernamePasswordAuthenticationToken(subject, null, List.of());
                auth.setDetails(new WebAuthenticationDetailsSource().buildDetails(req));
                
                // Propagate tenant to thread-local for TenantAwareJdbcTemplate
                TenantContext.setCurrentTenant(tenant);
                
                SecurityContextHolder.getContext().setAuthentication(auth);
                log.debug("Authenticated user '{}' tenant '{}'", subject, tenant);
            }
            
            chain.doFilter(req, resp);
        } catch (Exception e) {
            log.warn("JWT validation failed: {}", e.getMessage());
            resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            resp.setContentType("application/json");
            resp.getWriter().write("{\"error\":\"Invalid or expired token\"}");
        }
    }

    private String extractToken(HttpServletRequest req) {
        String header = req.getHeader(AUTHORIZATION_HEADER);
        if (header != null && header.startsWith(BEARER_PREFIX)) {
            return header.substring(BEARER_PREFIX.length());
        }
        return null;
    }
}
