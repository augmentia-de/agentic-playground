package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import de.augmentia.agentplatform.orchestrator.service.WorkflowQueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * WP02 / B14 — the Slack callback must be signature-verified and must only then act.
 */
class HitlPortalControllerTest {

    private static final String SECRET = "test-signing-secret";

    private String sign(String timestamp, String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            String base = "v0:" + timestamp + ":" + body;
            return "v0=" + HexFormat.of().formatHex(mac.doFinal(base.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private HitlPortalController controller(WorkflowQueryService queryService,
                                            OrchestratorEventPublisher publisher) {
        HitlPortalController c = new HitlPortalController(queryService, publisher);
        ReflectionTestUtils.setField(c, "slackSigningSecret", SECRET);
        return c;
    }

    private HitlTicketRow ticket(UUID id, UUID workflowId) {
        return new HitlTicketRow(id, workflowId, UUID.randomUUID(), "step-1", 0, "review",
                HitlTicketStatus.OPEN, "{}", 0, null, Instant.now(), null, null, null);
    }

    @Test
    @DisplayName("A callback without a signature is rejected with 401 and nothing happens")
    void missingSignatureRejected() {
        WorkflowQueryService query = mock(WorkflowQueryService.class);
        OrchestratorEventPublisher publisher = mock(OrchestratorEventPublisher.class);
        String body = "{\"value\":\"action=approve&ticketId=" + UUID.randomUUID() + "\"}";

        ResponseEntity<Map<String, String>> resp =
                controller(query, publisher).slackCallback(body, null, null);

        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        verifyNoInteractions(query, publisher);
    }

    @Test
    @DisplayName("A callback with an invalid signature is rejected with 401")
    void invalidSignatureRejected() {
        WorkflowQueryService query = mock(WorkflowQueryService.class);
        OrchestratorEventPublisher publisher = mock(OrchestratorEventPublisher.class);
        String body = "{\"value\":\"action=approve&ticketId=" + UUID.randomUUID() + "\"}";

        ResponseEntity<Map<String, String>> resp = controller(query, publisher)
                .slackCallback(body, "v0=deadbeef", String.valueOf(Instant.now().getEpochSecond()));

        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        verifyNoInteractions(query, publisher);
    }

    @Test
    @DisplayName("A correctly signed callback processes the action")
    void validSignatureProcessesAction() {
        UUID ticketId = UUID.randomUUID();
        UUID workflowId = UUID.randomUUID();
        WorkflowQueryService query = mock(WorkflowQueryService.class);
        OrchestratorEventPublisher publisher = mock(OrchestratorEventPublisher.class);
        when(query.requireTicket(ticketId)).thenReturn(ticket(ticketId, workflowId));

        String body = "{\"actions\":[{\"value\":\"action=approve&ticketId=" + ticketId
                + "&workflowId=" + workflowId + "\"}],\"user\":{\"username\":\"bob\"}}";
        String ts = String.valueOf(Instant.now().getEpochSecond());

        ResponseEntity<Map<String, String>> resp = controller(query, publisher)
                .slackCallback(body, sign(ts, body), ts);

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        verify(publisher).publish(eq(de.augmentia.agentplatform.common.kafka.TopicNames.HITL),
                eq(workflowId.toString()), anyString());
        verify(query).requireTicket(ticketId);
    }

    @Test
    @DisplayName("A signed but empty payload is a 400")
    void signedEmptyPayloadIsBadRequest() {
        WorkflowQueryService query = mock(WorkflowQueryService.class);
        OrchestratorEventPublisher publisher = mock(OrchestratorEventPublisher.class);
        String ts = String.valueOf(Instant.now().getEpochSecond());

        ResponseEntity<Map<String, String>> resp = controller(query, publisher)
                .slackCallback("", sign(ts, ""), ts);

        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        verifyNoInteractions(query, publisher);
    }
}
