package de.augmentia.agentplatform.orchestrator.config;

import de.augmentia.agentplatform.common.tenant.TenantContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Sets TenantContext from X-Tenant-ID request header before routing,
 * clears it after processing to prevent thread-local leakage.
 */
@Component
public class TenantInterceptor implements WebMvcConfigurer {

    private static final Logger log = LoggerFactory.getLogger(TenantInterceptor.class);

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new HandlerInterceptor() {
            @Override
            public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
                String tenantId = request.getHeader("X-Tenant-ID");
                if (tenantId != null && !tenantId.isBlank()) {
                    setCurrentTenant(tenantId);
                    log.debug("Set tenant context to {} for request {}", tenantId, request.getRequestURI());
                } else {
                    resetToDefault();
                }
                return true;
            }

            @Override
            public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
                clear();
            }
        });
    }

    private static void setCurrentTenant(String tenantId) {
        TenantContext.setCurrentTenant(tenantId);
    }

    private static void resetToDefault() {
        TenantContext.resetToDefault();
    }

    private static void clear() {
        TenantContext.clear();
    }
}
