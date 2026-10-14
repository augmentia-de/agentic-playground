package de.augmentia.agentplatform.orchestrator.security;

import de.augmentia.agentplatform.common.tenant.TenantContext;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * WP02 / B10 — the JWT filter must not mask downstream errors as 401 and must respect
 * {@code app.security.enabled}.
 */
class JwtAuthenticationFilterTest {

    private static final byte[] KEY_BYTES =
            "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private static final String SECRET_B64 = Base64.getEncoder().encodeToString(KEY_BYTES);

    private final JwtAuthenticationFilter filter = new JwtAuthenticationFilter();

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
        TenantContext.clear();
    }

    private JwtAuthenticationFilter configured(boolean securityEnabled) {
        ReflectionTestUtils.setField(filter, "secretBase64", SECRET_B64);
        ReflectionTestUtils.setField(filter, "securityEnabled", securityEnabled);
        return filter;
    }

    private String validToken() {
        return Jwts.builder()
                .subject("alice")
                .claim("tenant", "tenant-1")
                .signWith(Keys.hmacShaKeyFor(KEY_BYTES))
                .compact();
    }

    private MockHttpServletRequest request(String uri, String token) {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", uri);
        if (token != null) {
            req.addHeader("Authorization", "Bearer " + token);
        }
        return req;
    }

    @Test
    @DisplayName("A downstream exception propagates instead of being rewritten to 401")
    void downstreamExceptionIsNotConvertedTo401() {
        MockHttpServletRequest req = request("/api/v1/workflows", validToken());
        MockHttpServletResponse resp = new MockHttpServletResponse();
        FilterChain boom = (rq, rs) -> {
            throw new ServletException("controller exploded");
        };

        assertThatThrownBy(() -> configured(true).doFilter(req, resp, boom))
                .isInstanceOf(ServletException.class)
                .hasMessageContaining("controller exploded");
        assertThat(resp.getStatus()).isNotEqualTo(401);
    }

    @Test
    @DisplayName("With security disabled an invalid token is ignored (no 401)")
    void disabledSecurityAllowsRequestWithBadToken() throws Exception {
        AtomicBoolean invoked = new AtomicBoolean(false);
        MockHttpServletResponse resp = new MockHttpServletResponse();

        configured(false).doFilter(
                request("/api/v1/workflows", "not-a-jwt"),
                resp,
                (rq, rs) -> invoked.set(true));

        assertThat(invoked).isTrue();
        assertThat(resp.getStatus()).isNotEqualTo(401);
    }

    @Test
    @DisplayName("With security enabled an invalid token is rejected with 401")
    void enabledSecurityRejectsBadToken() throws Exception {
        AtomicBoolean invoked = new AtomicBoolean(false);
        MockHttpServletResponse resp = new MockHttpServletResponse();

        configured(true).doFilter(
                request("/api/v1/workflows", "not-a-jwt"),
                resp,
                (rq, rs) -> invoked.set(true));

        assertThat(resp.getStatus()).isEqualTo(401);
        assertThat(invoked).isFalse();
    }

    @Test
    @DisplayName("A valid token populates the security context and tenant")
    void validTokenPopulatesContext() throws Exception {
        MockHttpServletResponse resp = new MockHttpServletResponse();
        String[] subject = new String[1];
        String[] tenant = new String[1];

        configured(true).doFilter(
                request("/api/v1/workflows", validToken()),
                resp,
                (rq, rs) -> {
                    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
                    subject[0] = auth != null ? auth.getName() : null;
                    tenant[0] = TenantContext.getCurrentTenant();
                });

        assertThat(subject[0]).isEqualTo("alice");
        assertThat(tenant[0]).isEqualTo("tenant-1");
    }

    @Test
    @DisplayName("Public paths bypass validation even with a bad token")
    void publicPathSkipsValidation() throws Exception {
        AtomicBoolean invoked = new AtomicBoolean(false);
        MockHttpServletResponse resp = new MockHttpServletResponse();

        configured(true).doFilter(
                request("/actuator/health", "garbage"),
                resp,
                (rq, rs) -> invoked.set(true));

        assertThat(invoked).isTrue();
        assertThat(resp.getStatus()).isNotEqualTo(401);
    }
}
