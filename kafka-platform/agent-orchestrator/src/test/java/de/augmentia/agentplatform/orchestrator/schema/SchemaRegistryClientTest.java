package de.augmentia.agentplatform.orchestrator.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP02 / B8 — {@link SchemaRegistryClient} is wired through its constructor and can actually talk
 * to the registry (the previous {@code baseUrl} came from a not-yet-injected field).
 */
class SchemaRegistryClientTest {

    @Test
    @DisplayName("listSubjects uses the configured base URL and filters by prefix")
    void listSubjectsUsesConfiguredBaseUrl() {
        AtomicReference<ClientRequest> captured = new AtomicReference<>();
        ExchangeFunction fn = req -> {
            captured.set(req);
            return Mono.just(ClientResponse.create(HttpStatus.OK)
                    .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                    .body("[\"agent.execution.results-value\",\"agent.execution.tasks-value\",\"other\"]")
                    .build());
        };
        SchemaRegistryClient client = new SchemaRegistryClient(
                WebClient.builder().exchangeFunction(fn), "http://registry.test:8081");

        Set<String> subjects = client.listSubjects("agent.execution");

        assertThat(subjects).containsExactlyInAnyOrder(
                "agent.execution.results-value", "agent.execution.tasks-value");
        assertThat(captured.get().url().toString()).isEqualTo("http://registry.test:8081/subjects");
    }

    @Test
    @DisplayName("listSubjects returns an empty set when the registry is unreachable")
    void listSubjectsHandlesError() {
        ExchangeFunction failing = req -> Mono.error(new java.net.ConnectException("refused"));
        SchemaRegistryClient client = new SchemaRegistryClient(
                WebClient.builder().exchangeFunction(failing), "http://registry.test:8081");

        assertThat(client.listSubjects(null)).isEmpty();
    }
}
