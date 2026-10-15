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
 * Confluent Schema Registry client — read-only operations.
 *
 * <p>Configuration is injected via the constructor (the previous {@code @Value} fields were read
 * before injection, so {@code baseUrl} was always {@code null}).
 *
 * <p>Schema <em>registration</em> is deliberately not offered: the former
 * {@code registerAll()} only logged a message and registered nothing, which is worse than no method
 * at all. Only {@link #listSubjects(String)} (a real HTTP call) remains.
 */
@Component
@ConditionalOnProperty(name = "app.schema-registry.enabled", havingValue = "true")
public class SchemaRegistryClient {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryClient.class);

    private final WebClient webClient;

    public SchemaRegistryClient(WebClient.Builder webClientBuilder,
                                @Value("${app.schema-registry.url:http://schema-registry:8081}") String schemaRegistryUrl) {
        this.webClient = webClientBuilder.baseUrl(schemaRegistryUrl).build();
    }

    /** List the subjects known to the registry, optionally filtered by {@code topicPrefix}. */
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
