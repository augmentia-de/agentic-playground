package de.augmentia.quad.core.security;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The token cache must key on audience + subject + scopes — never on audience alone. */
class TokenExchangeClientTest {

    private static TokenExchangeConfig config() {
        return new TokenExchangeConfig(
                "https://issuer.example/realms/x", "cid", "secret", "default-aud",
                new String[]{"openid"}, 300,
                "urn:ietf:params:oauth:token-type:access_token",
                "urn:ietf:params:oauth:token-type:access_token");
    }

    private record Fixture(TokenExchangeClient client, AtomicInteger calls) {}

    @SuppressWarnings("unchecked")
    private static Fixture fixture() throws Exception {
        HttpClient http = mock(HttpClient.class);
        HttpResponse<String> response = mock(HttpResponse.class);
        AtomicInteger calls = new AtomicInteger();

        when(response.statusCode()).thenReturn(200);
        when(response.body()).thenAnswer(inv ->
                "{\"access_token\":\"tok-" + calls.incrementAndGet() + "\",\"expires_in\":3600}");
        when(http.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);

        return new Fixture(new TokenExchangeClient(config(), http), calls);
    }

    @Test
    void differentSubject_differentToken() throws Exception {
        Fixture f = fixture();

        String first = f.client().exchange("subject-A", "mcp", "s1");
        String second = f.client().exchange("subject-B", "mcp", "s1");

        assertThat(first).isEqualTo("tok-1");
        assertThat(second).as("a token issued for subject-A must never be reused for subject-B")
                .isEqualTo("tok-2");
        assertThat(f.calls().get()).isEqualTo(2);
    }

    @Test
    void differentScopes_differentToken() throws Exception {
        Fixture f = fixture();

        String read = f.client().exchange("subject-A", "mcp", "read");
        String write = f.client().exchange("subject-A", "mcp", "write");

        assertThat(read).isEqualTo("tok-1");
        assertThat(write).as("different scopes must not share a cache entry").isEqualTo("tok-2");
    }

    @Test
    void sameKey_hitsCache() throws Exception {
        Fixture f = fixture();

        String first = f.client().exchange("subject-A", "mcp", "read");
        String second = f.client().exchange("subject-A", "mcp", "read");

        assertThat(second).isEqualTo(first);
        assertThat(f.calls().get()).as("identical exchange must be served from cache").isEqualTo(1);
    }
}
