package de.augmentia.agentflow.app.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the Jackson <b>2</b> {@code com.fasterxml.jackson.databind.ObjectMapper} bean.
 *
 * <p>Spring Boot 4 auto-configures only Jackson 3 ({@code tools.jackson}) via
 * {@code spring-boot-starter-jackson}. AgentFlow's own code, as well as the reused
 * quad-core / Temporal / LangChain4j dependencies, are built against Jackson 2
 * ({@code com.fasterxml.jackson.databind}), which is present on the classpath but no longer
 * auto-configured. Without this bean the application context fails to start
 * (e.g. {@code CapabilityIndexConfig} requires an {@code ObjectMapper}).</p>
 *
 * <p>REST serialization keeps using Boot's Jackson 3 stack; this bean only serves the
 * application-internal components that inject the Jackson 2 mapper.</p>
 */
@Configuration
public class JacksonConfig {

    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper jackson2ObjectMapper() {
        // findAndRegisterModules() picks up jackson-datatype-jsr310/jdk8 (e.g. Instant) when present.
        return new ObjectMapper().findAndRegisterModules();
    }
}
