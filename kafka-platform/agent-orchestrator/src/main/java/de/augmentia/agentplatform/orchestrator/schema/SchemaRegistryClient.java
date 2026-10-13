package de.augmentia.agentplatform.orchestrator.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Confluent Schema Registry client for managing JSON Schemas for Kafka topics.
 */
@Component
@ConditionalOnProperty(name = "app.schema-registry.enabled", havingValue = "true")
public class SchemaRegistryClient {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryClient.class);

    @Value("${app.schema-registry.url:http://schema-registry:8081}")
    private String schemaRegistryUrl;
    @Value("${app.schema-registry.compatibility-mode:BACKWARD}")
    private String compatibilityMode;
    @Value("${app.schema-registry.auto-register:true}")
    private boolean autoRegister;

    private final WebClient webClient;

    public SchemaRegistryClient(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.baseUrl(schemaRegistryUrl).build();
    }

    public void registerAll() {
        log.info("Schema registration — configure subjects in subclass");
    }

    /** Check if schema list exists on registry. */
    public Set<String> listSubjects(String topicPrefix) {
        try {
            List<?> raw = webClient.get().uri("/subjects").retrieve().bodyToMono(List.class).block();
            if (raw == null) return Set.of();
            return raw.stream()
                    .filter(s -> topicPrefix == null || s.toString().contains(topicPrefix))
                    .map(Object::toString)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            log.error("Failed to list subjects: {}", e.getMessage());
            return Set.of();
        }
    }
}
