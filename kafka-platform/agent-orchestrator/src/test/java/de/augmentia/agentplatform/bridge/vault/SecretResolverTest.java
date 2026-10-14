package de.augmentia.agentplatform.bridge.vault;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP02 / B7 + B8 — {@link SecretResolver} health logic and constructor wiring.
 *
 * <p>Uses a stubbed {@link ExchangeFunction} instead of a real Vault: the {@link WebClient} built
 * from the injected builder is exercised end-to-end (base URL, default header, body parsing).
 */
class SecretResolverTest {

    private static WebClient.Builder builder(String json, AtomicReference<ClientRequest> capture,
                                             AtomicInteger calls) {
        ExchangeFunction fn = req -> {
            if (capture != null) capture.set(req);
            if (calls != null) calls.incrementAndGet();
            return Mono.just(ClientResponse.create(HttpStatus.OK)
                    .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                    .body(json)
                    .build());
        };
        return WebClient.builder().exchangeFunction(fn);
    }

    private static SecretResolver resolver(WebClient.Builder builder) {
        return new SecretResolver(builder, "http://vault.test:8200", "tok-123",
                "secret/data", 60, 10);
    }

    @Test
    @DisplayName("WebClient uses the configured base URL and Vault token header")
    void webClientUsesConfiguredUrlAndToken() {
        AtomicReference<ClientRequest> captured = new AtomicReference<>();
        SecretResolver r = resolver(builder("{\"data\":{\"password\":\"s3cret\"}}", captured, null));

        assertThat(r.resolve("agent-platform/db", "password")).isEqualTo("s3cret");

        ClientRequest req = captured.get();
        assertThat(req).isNotNull();
        assertThat(req.url().toString())
                .isEqualTo("http://vault.test:8200/secret/data/agent-platform/db");
        assertThat(req.headers().getFirst("X-Vault-Token")).isEqualTo("tok-123");
    }

    @Test
    @DisplayName("resolve returns null for a key that is not present")
    void missingKeyReturnsNull() {
        SecretResolver r = resolver(builder("{\"data\":{\"other\":\"x\"}}", null, null));

        assertThat(r.resolve("agent-platform/db", "password")).isNull();
    }

    @Test
    @DisplayName("resolveAll parses every key of the data object")
    void resolveAllParsesKeys() {
        SecretResolver r = resolver(builder("{\"data\":{\"user\":\"u\",\"pass\":\"p\"}}", null, null));

        assertThat(r.resolveAll("agent-platform/db"))
                .containsAllEntriesOf(Map.of("user", "u", "pass", "p"));
    }

    @Test
    @DisplayName("isConnected is false when Vault is sealed")
    void isConnectedFalseWhenSealed() {
        SecretResolver r = resolver(builder("{\"initialized\":true,\"sealed\":true}", null, null));

        assertThat(r.isConnected()).isFalse();
    }

    @Test
    @DisplayName("isConnected is true when Vault is initialized and unsealed")
    void isConnectedTrueWhenInitializedAndUnsealed() {
        SecretResolver r = resolver(builder("{\"initialized\":true,\"sealed\":false}", null, null));

        assertThat(r.isConnected()).isTrue();
    }

    @Test
    @DisplayName("isConnected is false when Vault is not initialized")
    void isConnectedFalseWhenNotInitialized() {
        SecretResolver r = resolver(builder("{\"initialized\":false,\"sealed\":false}", null, null));

        assertThat(r.isConnected()).isFalse();
    }

    @Test
    @DisplayName("isConnected is false when the health call fails")
    void isConnectedFalseOnError() {
        ExchangeFunction failing = req -> Mono.error(new java.net.ConnectException("refused"));
        SecretResolver r = resolver(WebClient.builder().exchangeFunction(failing));

        assertThat(r.isConnected()).isFalse();
    }

    @Test
    @DisplayName("refreshAll clears the cache and forces a new fetch")
    void refreshAllClearsTheCache() {
        AtomicInteger calls = new AtomicInteger();
        SecretResolver r = resolver(builder("{\"data\":{\"password\":\"s3cret\"}}", null, calls));

        r.resolve("agent-platform/db", "password");
        r.resolve("agent-platform/db", "password"); // served from cache
        assertThat(calls.get()).isEqualTo(1);

        r.refreshAll();
        r.resolve("agent-platform/db", "password");
        assertThat(calls.get()).isEqualTo(2);
    }
}
