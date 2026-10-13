package de.augmentia.agentplatform.bridge;

import de.augmentia.agentplatform.bridge.web.UnifiedApiGateway;
import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import de.augmentia.agentplatform.orchestrator.service.WorkflowDefinitionService;
import de.augmentia.agentplatform.orchestrator.service.WorkflowStartService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The HTTP level of the path the UI takes.
 *
 * <p>Previously there was not a single test for
 * {@code POST /api/v1/unified/workflows/from-quad}. The live call returned HTTP 500 with
 * {@code "Type definition error: [simple type, class com.fasterxml.jackson.databind.JsonNode]"}.
 * That is an error message from {@code ObjectMapper.convertValue} respectively
 * {@code treeToValue} — it arises when the mapper gets a {@link
 * com.fasterxml.jackson.databind.JsonNode} as the <em>target type</em> that it
 * cannot materialise.
 *
 * <p>This test sends exactly the payload that {@code convertQuadToKafka} sends in
 * {@code kafkaApi.ts}.
 */
@DisplayName("Gateway: POST /unified/workflows/from-quad")
class UnifiedApiGatewayFromQuadTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MockMvc mockMvc(WorkflowDefinitionService definitions,
                            WorkflowStartService starts) {
        return MockMvcBuilders.standaloneSetup(
                new UnifiedApiGateway("http://localhost:1", definitions, starts,
                        mock(JdbcAgentDefinitionStore.class),
                        AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS, "simple"))
                .build();
    }

    private static String uiPayload() throws Exception {
        return MAPPER.writeValueAsString(Map.of(
                "name", "aus-der-ui",
                "nodes", java.util.List.of(
                        Map.of("id", "research", "type", "agent", "agentType", "researcher"),
                        Map.of("id", "gate", "type", "conditional"),
                        Map.of("id", "left", "type", "agent", "agentType", "coder"),
                        Map.of("id", "right", "type", "agent", "agentType", "tester")),
                "edges", java.util.List.of(
                        Map.of("source", "research", "target", "gate"),
                        Map.of("source", "gate", "target", "left",
                               "condition", "context['research'].confidence < 0.8"),
                        Map.of("source", "gate", "target", "right",
                               "condition", "context['research'].confidence >= 0.8")),
                "task", "Live-Beweis"));
    }

    @Test
    @DisplayName("The UI payload is accepted instead of being rejected with HTTP 500")
    void uiPayloadIsAccepted() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        when(definitions.create(any())).thenReturn(java.util.UUID.randomUUID());
        when(starts.start(anyString(), eq(1), any(), anyString()))
                .thenReturn(new WorkflowStartService.StartResult(
                        java.util.UUID.randomUUID(), false));

        mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Backend", "kafka")
                        .content(uiPayload()))
                .andDo(print())
                .andExpect(status().isAccepted());
    }

    private static String payloadWithNodeType(String nodeType) throws Exception {
        return MAPPER.writeValueAsString(Map.of(
                "name", "abweisbar",
                "nodes", java.util.List.of(
                        Map.of("id", "a", "type", "agent", "agentType", "researcher"),
                        Map.of("id", "b", "type", nodeType, "agentType", "coder")),
                "edges", java.util.List.of(Map.of("source", "a", "target", "b")),
                "task", "n/a"));
    }

    @Test
    @DisplayName("a node type that cannot be executed is 400 and stays before every write access")
    void unsupportedNodeTypeIsClientError() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);

        mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payloadWithNodeType("nested-workflow")))
                .andDo(print())
                .andExpect(status().isBadRequest());

        // Nothing created, nothing started: the rejection happens before storing.
        verify(definitions, never()).create(any());
        verify(starts, never()).start(anyString(), anyInt(), any(), anyString());
    }

    @Test
    @DisplayName("a backend failure while saving is 500, not 400")
    void backendFailureIsServerError() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        when(definitions.create(any())).thenThrow(new RuntimeException("database unavailable"));

        mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(uiPayload()))
                .andDo(print())
                .andExpect(status().isInternalServerError());

        verify(starts, never()).start(anyString(), anyInt(), any(), anyString());
    }

    /**
     * The payload that quad's own {@code WorkflowGeneratorService} produces. Its nodes carry
     * {@code title}/{@code status}/{@code description} and put the agent binding into
     * {@code config} — none of which the bridge records declare.
     */
    private static String generatedWorkflowPayload() throws Exception {
        return MAPPER.writeValueAsString(Map.of(
                "name", "aus-dem-generator",
                "nodes", java.util.List.of(
                        Map.of("id", "node-Researcher", "type", "agent",
                                "title", "Researcher", "description", "recherche",
                                "status", "pending",
                                "config", Map.of("agentId", "agent-1a2b3c4d",
                                        "tools", java.util.List.of("grep_search"),
                                        "timeoutSeconds", 30)),
                        Map.of("id", "node-Report", "type", "agent",
                                "title", "Report", "status", "pending",
                                "config", Map.of("agentId", "agent-9f8e7d6c"))),
                "edges", java.util.List.of(Map.of("source", "node-Researcher", "target", "node-Report")),
                "task", "Recherche"));
    }

    @Test
    @DisplayName("the payload of quad's workflow generator is accepted, not rejected as malformed")
    void generatedWorkflowPayloadIsAccepted() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        when(definitions.create(any())).thenReturn(java.util.UUID.randomUUID());
        when(starts.start(anyString(), anyInt(), any(), anyString()))
                .thenReturn(new WorkflowStartService.StartResult(java.util.UUID.randomUUID(), false));

        mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Backend", "kafka")
                        .content(generatedWorkflowPayload()))
                .andDo(print())
                // The extra node fields must not turn a valid workflow into a 400. They used
                // to: the gateway mapper is a bare JsonMapper and therefore rejects unknown
                // properties, while the quad format legitimately carries title/status/config.
                .andExpect(status().isAccepted());

        verify(definitions).create(any());
    }

    @Test
    @DisplayName("config.agentId is used as agentType instead of silently falling back to 'generic'")
    void agentIdFromConfigBecomesAgentType() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        when(definitions.create(any())).thenReturn(java.util.UUID.randomUUID());
        when(starts.start(anyString(), anyInt(), any(), anyString()))
                .thenReturn(new WorkflowStartService.StartResult(java.util.UUID.randomUUID(), false));

        var captor = org.mockito.ArgumentCaptor.forClass(
                de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload.class);

        mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Backend", "kafka")
                        .content(generatedWorkflowPayload()))
                .andExpect(status().isAccepted());

        verify(definitions).create(captor.capture());
        var types = captor.getValue().dagStructure().nodes().stream()
                .filter(n -> "STEP".equals(n.nodeType()))   // ignore the synthetic END node
                .map(de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload.DagNodeUpload::agentType)
                .toList();
        // Every step ran under agent type "generic" before, which silently routed all of them
        // to the stub agent on gpt-4o-mini with a generic prompt.
        assertThat(types).containsExactly("agent-1a2b3c4d", "agent-9f8e7d6c");
    }

    @Test
    @DisplayName("timeoutSeconds inside config survives the conversion")
    void timeoutFromConfigIsUsed() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        when(definitions.create(any())).thenReturn(java.util.UUID.randomUUID());
        when(starts.start(anyString(), anyInt(), any(), anyString()))
                .thenReturn(new WorkflowStartService.StartResult(java.util.UUID.randomUUID(), false));

        var captor = org.mockito.ArgumentCaptor.forClass(
                de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload.class);

        mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Backend", "kafka")
                        .content(generatedWorkflowPayload()))
                .andExpect(status().isAccepted());

        verify(definitions).create(captor.capture());
        // 30 s in the quad config used to vanish: timeoutFrom only looked at the record fields.
        assertThat(captor.getValue().dagStructure().nodes().get(0).timeoutMs()).isEqualTo(30_000);
    }

    @Test
    @DisplayName("a supplied Idempotency-Key reaches the start call, so a retry is deduplicated")
    void suppliedIdempotencyKeyIsUsed() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        when(definitions.create(any())).thenReturn(java.util.UUID.randomUUID());
        when(starts.start(anyString(), anyInt(), any(), anyString()))
                .thenReturn(new WorkflowStartService.StartResult(java.util.UUID.randomUUID(), false));

        var captor = org.mockito.ArgumentCaptor.forClass(String.class);

        mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Backend", "kafka")
                        .header("Idempotency-Key", "quad-retry-me")
                        .content(uiPayload()))
                .andExpect(status().isAccepted());

        verify(starts).start(anyString(), anyInt(), any(), captor.capture());
        assertThat(captor.getValue()).isEqualTo("quad-retry-me");
    }

    @Test
    @DisplayName("the stub agent gets the configured model key, not a hardcoded one")
    void stubAgentUsesConfiguredModelKey() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        var agents = mock(JdbcAgentDefinitionStore.class);
        when(definitions.create(any())).thenReturn(java.util.UUID.randomUUID());
        when(starts.start(anyString(), anyInt(), any(), anyString()))
                .thenReturn(new WorkflowStartService.StartResult(java.util.UUID.randomUUID(), false));

        // The worker resolves agents with models.get(modelKey) and throws on anything it does
        // not know. A stub carrying "gpt-4o-mini" therefore failed every task with
        // "Unknown model key" while the worker only had "simple" registered.
        MockMvcBuilders.standaloneSetup(new UnifiedApiGateway("http://localhost:1", definitions,
                    starts, agents, AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS, "simple"))
                .build()
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Backend", "kafka")
                        .content(uiPayload()))
                .andExpect(status().isAccepted());

        verify(agents).ensureStub(eq("researcher"), eq("simple"));
        verify(agents).ensureStub(eq("coder"), eq("simple"));
    }

    @Test
    @DisplayName("without a key a fresh one is generated and returned, so the client can retry with it")
    void generatedIdempotencyKeyIsReturned() throws Exception {
        var definitions = mock(WorkflowDefinitionService.class);
        var starts = mock(WorkflowStartService.class);
        when(definitions.create(any())).thenReturn(java.util.UUID.randomUUID());
        when(starts.start(anyString(), anyInt(), any(), anyString()))
                .thenReturn(new WorkflowStartService.StartResult(java.util.UUID.randomUUID(), false));

        var captor = org.mockito.ArgumentCaptor.forClass(String.class);

        var body = mockMvc(definitions, starts)
                .perform(post("/api/v1/unified/workflows/from-quad")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Backend", "kafka")
                        .content(uiPayload()))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();

        verify(starts).start(anyString(), anyInt(), any(), captor.capture());
        // The key used must be the key handed to the client — otherwise the retry cannot match.
        assertThat(body).contains("\"idempotencyKey\":\"" + captor.getValue() + "\"");
    }
}
