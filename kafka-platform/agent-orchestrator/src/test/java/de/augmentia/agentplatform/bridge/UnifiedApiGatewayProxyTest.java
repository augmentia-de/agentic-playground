package de.augmentia.agentplatform.bridge;

import de.augmentia.agentplatform.bridge.web.UnifiedApiGateway;
import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import de.augmentia.agentplatform.orchestrator.service.WorkflowDefinitionService;
import de.augmentia.agentplatform.orchestrator.service.WorkflowStartService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The proxy half of the gateway — the path the UI takes when it sets
 * {@code X-Backend: quarkus}.
 *
 * <p>Previously only the Kafka side was tested. This half is the more error-prone one: it
 * hangs off an address that points somewhere in operation, and its error handling had only
 * ever been asserted, never executed.
 *
 * <p>The counterpart is the JDK's {@code HttpServer}. No mock web server, no new dependency
 * — and it is real HTTP, so the {@code WebClient} sees here exactly what it sees in
 * operation.
 */
@DisplayName("Gateway: proxy to quad and agent conversion")
class UnifiedApiGatewayProxyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer quad;
    private String quadBaseUrl;
    private final AtomicReference<String> lastMethod = new AtomicReference<>();
    private final AtomicReference<String> lastPath = new AtomicReference<>();
    private final AtomicReference<String> lastBody = new AtomicReference<>();

    private WorkflowDefinitionService definitions;
    private WorkflowStartService starts;
    private JdbcAgentDefinitionStore agentStore;

    @BeforeEach
    void startQuad() throws Exception {
        quad = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // All paths are answered by the same handler — the test checks the routing,
        // not the domain logic of quad.
        quad.createContext("/", exchange -> {
            lastMethod.set(exchange.getRequestMethod());
            lastPath.set(exchange.getRequestURI().getPath());
            lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] out = "{\"quadSays\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, out.length);
            try (var body = exchange.getResponseBody()) {
                body.write(out);
            }
        });
        quad.start();
        quadBaseUrl = "http://127.0.0.1:" + quad.getAddress().getPort();

        definitions = mock(WorkflowDefinitionService.class);
        starts = mock(WorkflowStartService.class);
        agentStore = mock(JdbcAgentDefinitionStore.class);
    }

    @AfterEach
    void stopQuad() {
        if (quad != null) {
            quad.stop(0);
        }
    }

    private MockMvc gateway() {
        return MockMvcBuilders.standaloneSetup(
                new UnifiedApiGateway(quadBaseUrl, definitions, starts, agentStore,
                        AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS, "simple")).build();
    }

    /** The emitter pass-through runs asynchronously — wait briefly for the routing. */
    private void awaitPath(String expected) throws InterruptedException {
        for (int i = 0; i < 100 && !expected.equals(lastPath.get()); i++) {
            Thread.sleep(50);
        }
    }

    private static String agentPayload() throws Exception {
        // The field names are those of the QuarkusAgent record, not those of the
        // WorkerAgentConfig: id, name, modelId, maxTokens.
        return MAPPER.writeValueAsString(Map.ofEntries(
                Map.entry("id", "coder-1"),
                Map.entry("name", "Coder"),
                Map.entry("category", "code"),
                Map.entry("systemPrompt", "Du schreibst Code"),
                Map.entry("modelId", "gpt-4o-mini"),
                Map.entry("temperature", 0.2),
                Map.entry("maxTokens", 12),
                Map.entry("timeoutSeconds", 120),
                Map.entry("maxRetryAttempts", 2),
                Map.entry("version", 1),
                Map.entry("toolDefinitions", List.of(Map.of(
                        "name", "readFile", "description", "reads a file")))));
    }

    @Nested
    @DisplayName("Backend-Header quarkus")
    class BackendRoutesToQuarkus {

        @Test
        @DisplayName("POST /from-quad is forwarded to /api/ui/workflows")
        void fromQuadIsProxied() throws Exception {
            var result = gateway()
                    .perform(post("/api/v1/unified/workflows/from-quad")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-Backend", "quarkus")
                            .content("{\"name\":\"w\"}"))
                    .andExpect(status().isOk())
                    .andReturn();

            assertThat(lastMethod.get()).isEqualTo("POST");
            assertThat(lastPath.get()).isEqualTo("/api/ui/workflows");
            assertThat(result.getResponse().getContentAsString()).contains("proxied", "quadSays");
        }

        @Test
        @DisplayName("no kafka run is created in the process")
        void nothingIsWrittenToKafka() throws Exception {
            gateway()
                    .perform(post("/api/v1/unified/workflows/from-quad")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-Backend", "quarkus")
                            .content("{\"name\":\"w\"}"))
                    .andExpect(status().isOk());

            verify(definitions, never()).create(any());
            verify(starts, never()).start(anyString(), org.mockito.ArgumentMatchers.anyInt(),
                    any(), anyString());
        }

        @Test
        @DisplayName("GET /quarkus/{path} geht als GET raus")
        void getProxy() throws Exception {
            gateway()
                    .perform(get("/api/v1/unified/quarkus/workflows"))
                    .andExpect(status().isOk());

            assertThat(lastMethod.get()).isEqualTo("GET");
            assertThat(lastPath.get()).isEqualTo("/api/ui/workflows");
        }

        @Test
        @DisplayName("POST /quarkus/{path} geht als POST mit Body raus")
        void postProxy() throws Exception {
            gateway()
                    .perform(post("/api/v1/unified/quarkus/workflows/start")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"run\":true}"))
                    .andExpect(status().isOk());

            assertThat(lastMethod.get()).isEqualTo("POST");
            assertThat(lastPath.get()).isEqualTo("/api/ui/workflows/start");
            assertThat(lastBody.get()).contains("run");
        }

        @Test
        @DisplayName("a sub-path is passed through — /quarkus/workflows/w1")
        void nestedPathIsProxied() throws Exception {
            // With {path:.+} the pattern matched only one segment; everything with a slash
            // in it ended in a 404. The frontend does not use the proxy so far,
            // so the gap was invisible.
            gateway()
                    .perform(get("/api/v1/unified/quarkus/workflows/w1"))
                    .andExpect(status().isOk());

            assertThat(lastPath.get()).isEqualTo("/api/ui/workflows/w1");
        }

        @Test
        @DisplayName("if quad is unreachable, the gateway answers with 502")
        void unreachableQuadYieldsBadGateway() throws Exception {
            quad.stop(0);

            gateway()
                    .perform(get("/api/v1/unified/quarkus/workflows"))
                    .andExpect(status().isBadGateway());
        }
    }

    @Nested
    @DisplayName("POST /quarkus/{path} mit 4xx von quad")
    class QuarkusRejects {

        @BeforeEach
        void quadAnswersWithError() throws Exception {
            quad.stop(0);
            quad = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            quad.createContext("/", exchange -> {
                byte[] out = "{\"error\":\"kaputt\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(422, out.length);
                try (var body = exchange.getResponseBody()) {
                    body.write(out);
                }
            });
            quad.start();
            quadBaseUrl = "http://127.0.0.1:" + quad.getAddress().getPort();
        }

        @Test
        @DisplayName("the status from quad is passed through, not lifted to 200")
        void quarkusStatusIsPreserved() throws Exception {
            // 200 would be a false report here: the caller would consider a rejected
            // workflow saved.
            gateway()
                    .perform(post("/api/v1/unified/quarkus/workflows")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"name\":\"w\"}"))
                    .andExpect(status().isUnprocessableEntity());
        }
    }

    @Nested
    @DisplayName("GET /metrics")
    class Metrics {

        @Test
        @DisplayName("both sources are reported")
        void bothSourcesAreReported() throws Exception {
            var result = gateway()
                    .perform(get("/api/v1/unified/metrics"))
                    .andExpect(status().isOk())
                    .andReturn();

            var body = result.getResponse().getContentAsString();
            assertThat(body).contains("kafkaOrchestrator", "quadQuarkus");
        }

        @Test
        @DisplayName("if a source fails, that is reported as unavailable, not as 500")
        void deadSourceIsReportedNotFatal() throws Exception {
            quad.stop(0);

            var result = gateway()
                    .perform(get("/api/v1/unified/metrics"))
                    .andExpect(status().isOk())
                    .andReturn();

            assertThat(result.getResponse().getContentAsString())
                    .contains("\"quadQuarkus\"", "\"available\":false");
        }
    }

    @Nested
    @DisplayName("POST /agents/convert")
    class AgentConversion {

        @Test
        @DisplayName("the agent is registered and reported back")
        void agentIsRegistered() throws Exception {
            var result = gateway()
                    .perform(post("/api/v1/unified/agents/convert")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(agentPayload()))
                    .andExpect(status().isOk())
                    .andReturn();

            var body = result.getResponse().getContentAsString();
            assertThat(body).contains("\"agentId\":\"coder-1\"", "\"registered\":true");
            assertThat(body).contains("Skills registry integration lost in conversion");

            // maxTokens 12 travels unchanged into the max_tokens column. Until
            // recently a conversion stood here (/4 in the bridge, *4 in the gateway),
            // which cancelled itself out.
            verify(agentStore).upsert(eq("coder-1"), eq("Coder"), eq("Du schreibst Code"),
                    eq("gpt-4o-mini"), eq(0.2), eq(12), eq(120), eq(2), eq(1), eq(10));
        }

        @Test
        @DisplayName("without agentName the agentId is used as name")
        void fallsBackToAgentIdAsName() throws Exception {
            gateway()
                    .perform(post("/api/v1/unified/agents/convert")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(MAPPER.writeValueAsString(Map.of(
                                    "id", "nur-id",
                                    "modelId", "gpt-4o-mini"))))
                    .andExpect(status().isOk());

            verify(agentStore).upsert(eq("nur-id"), eq("nur-id"), anyString(), anyString(),
                    org.mockito.ArgumentMatchers.anyDouble(),
                    org.mockito.ArgumentMatchers.anyInt(),
                    org.mockito.ArgumentMatchers.anyInt(),
                    org.mockito.ArgumentMatchers.anyInt(),
                    org.mockito.ArgumentMatchers.anyInt(),
                    eq(10));
        }

        @Test
        @DisplayName("an unreadable payload is 400")
        void malformedPayloadIsClientError() throws Exception {
            // "id" expects a string. An object there makes Jackson fail —
            // unlike an unknown field, which Jackson silently ignores.
            gateway()
                    .perform(post("/api/v1/unified/agents/convert")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"id\": {\"nope\": 1}}"))
                    .andExpect(status().isBadRequest());

            verify(agentStore, never()).upsert(anyString(), anyString(), anyString(), anyString(),
                    org.mockito.ArgumentMatchers.anyDouble(), org.mockito.ArgumentMatchers.anyInt(),
                    org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt(),
                    org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());
        }

        @Test
        @DisplayName("if the registration fails, that is 500 and not 400")
        void registrationFailureIsServerError() throws Exception {
            doThrow(new RuntimeException("database unavailable"))
                    .when(agentStore).upsert(anyString(), anyString(), anyString(), anyString(),
                            org.mockito.ArgumentMatchers.anyDouble(), org.mockito.ArgumentMatchers.anyInt(),
                            org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt(),
                            org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());

            // 400 would name the caller's problem. The error is in the DB.
            gateway()
                    .perform(post("/api/v1/unified/agents/convert")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(agentPayload()))
                    .andExpect(status().isInternalServerError());
        }
    }

    @Test
    @DisplayName("the SSE path passes through the data from quad")
    void sseStreamIsProxied() throws Exception {
        MvcResult result = gateway()
                .perform(get("/api/v1/unified/quarkus/stream/workflows"))
                .andReturn();

        // The SSE path does not answer with a body but starts an emitter.
        // What is testable is therefore the routing; the forwarding of the events
        // (emitter.send) runs asynchronously and needs a real SSE client.
        awaitPath("/api/ui/workflows");
        assertThat(lastMethod.get()).isEqualTo("GET");
    }
}