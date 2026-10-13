package de.augmentia.agentflow.app.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Two mutually exclusive security modes:
 *
 * <ul>
 *   <li><b>default (open)</b> — {@code agentflow.security.oidc.enabled=false}: a permissive chain
 *       ({@code permitAll}) so behaviour is unchanged. This bean also replaces Spring Boot's
 *       default "authenticate everything" chain whenever the security starter is on the classpath.</li>
 *   <li><b>OIDC</b> — {@code agentflow.security.oidc.enabled=true}: JWT resource server. Requires
 *       {@code spring.security.oauth2.resourceserver.jwt.issuer-uri} or {@code jwk-set-uri}
 *       (both intentionally unset by default, so nothing is fetched while disabled).</li>
 * </ul>
 *
 * The independent, opt-in {@link ApiKeyFilter} ({@code agentflow.security.enabled}) still applies.
 */
@Configuration
public class WebSecurityConfig {

    @Bean
    @ConditionalOnProperty(name = "agentflow.security.oidc.enabled", havingValue = "false", matchIfMissing = true)
    public SecurityFilterChain openFilterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
        return http.build();
    }

    @Bean
    @ConditionalOnProperty(name = "agentflow.security.oidc.enabled", havingValue = "true")
    public SecurityFilterChain oidcFilterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(rs -> rs.jwt(Customizer.withDefaults()));
        return http.build();
    }
}
