package de.augmentia.agentplatform.bridge.email;

import de.augmentia.agentplatform.bridge.redis.EmailInitiatorStore;
import de.augmentia.agentplatform.orchestrator.service.WorkflowStartService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * WP04 / B1 — the email intake must start a workflow through the canonical
 * {@link WorkflowStartService} and must no longer publish a shape the orchestrator cannot read.
 */
class EmailInputTransformerTest {

    private static final String ACCEPTED = "tasks@quad.local";

    private EmailInputTransformer transformer(WorkflowStartService starts, EmailInitiatorStore store) {
        EmailInputTransformer t = new EmailInputTransformer(mock(JavaMailSender.class), starts, store);
        ReflectionTestUtils.setField(t, "acceptedAddress", ACCEPTED);
        return t;
    }

    @Test
    @DisplayName("An accepted email starts the workflow via WorkflowStartService and stores the initiator")
    void acceptedEmailStartsWorkflow() {
        WorkflowStartService starts = mock(WorkflowStartService.class);
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);
        UUID executionId = UUID.randomUUID();
        when(starts.start(eq("dev-feature-x"), eq(1), anyMap(), any()))
                .thenReturn(new WorkflowStartService.StartResult(executionId, false));

        var response = transformer(starts, store).receiveEmail(new EmailInputTransformer.WebhookPayload(
                ACCEPTED, "alice@corp.example", "[QUAD] Dev Feature X",
                "Please review the payment flow", List.of("a.pdf")));

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        verify(starts).start(eq("dev-feature-x"), eq(1), anyMap(), isNull());
        // The email payload must travel as workflow input parameters, not as a bespoke event.
        verify(starts).start(eq("dev-feature-x"), eq(1), org.mockito.ArgumentMatchers.argThat(input ->
                "Please review the payment flow".equals(input.get("task"))
                        && "Please review the payment flow".equals(input.get("emailBody"))
                        && "[QUAD] Dev Feature X".equals(input.get("subject"))
                        && "alice@corp.example".equals(input.get("initiatorEmail"))
                        && List.of("a.pdf").equals(input.get("attachments"))
                        && "dev-feature-x".equals(input.get("triggerKey"))), isNull());
        // The initiator is keyed by the REAL execution id, which is what the result handler looks up.
        verify(store).storeInitiator(executionId.toString(), "alice@corp.example");
    }

    @Test
    @DisplayName("A duplicate start still acknowledges and does not re-store under a new id")
    void duplicateStartIsAcknowledged() {
        WorkflowStartService starts = mock(WorkflowStartService.class);
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);
        UUID executionId = UUID.randomUUID();
        when(starts.start(any(), any(), anyMap(), any()))
                .thenReturn(new WorkflowStartService.StartResult(executionId, true));

        var response = transformer(starts, store).receiveEmail(new EmailInputTransformer.WebhookPayload(
                ACCEPTED, "alice@corp.example", "[QUAD] dev-feature-x", "body", null));

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getBody()).containsEntry("duplicate", true);
        verify(store).storeInitiator(executionId.toString(), "alice@corp.example");
    }

    @Test
    @DisplayName("A mail for a different recipient is rejected and never started")
    void wrongRecipientIsRejected() {
        WorkflowStartService starts = mock(WorkflowStartService.class);
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);

        var response = transformer(starts, store).receiveEmail(new EmailInputTransformer.WebhookPayload(
                "someone-else@quad.local", "alice@corp.example", "[QUAD] dev-feature-x", "body", null));

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        verifyNoInteractions(starts, store);
    }

    @Test
    @DisplayName("A start failure is reported as 500 and no initiator is stored")
    void startFailureReturns500() {
        WorkflowStartService starts = mock(WorkflowStartService.class);
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);
        when(starts.start(any(), any(), anyMap(), any()))
                .thenThrow(new IllegalStateException("no workflow definition 'dev-feature-x'"));

        var response = transformer(starts, store).receiveEmail(new EmailInputTransformer.WebhookPayload(
                ACCEPTED, "alice@corp.example", "[QUAD] dev-feature-x", "body", null));

        assertThat(response.getStatusCode().value()).isEqualTo(500);
        verify(store, never()).storeInitiator(any(), any());
    }

    @Test
    @DisplayName("Trigger key is derived from the subject and normalised")
    void triggerKeyIsNormalised() {
        var t = transformer(mock(WorkflowStartService.class), mock(EmailInitiatorStore.class));

        assertThat(t.extractTriggerKey("[QUAD] My Task")).isEqualTo("my-task");
        assertThat(t.extractTriggerKey("[QUAD] dev-feature-x")).isEqualTo("dev-feature-x");
    }

    @Test
    @DisplayName("A subject without a [QUAD] tag falls back to the default trigger")
    void missingTriggerFallsBackToDefault() {
        var t = transformer(mock(WorkflowStartService.class), mock(EmailInitiatorStore.class));

        assertThat(t.extractTriggerKey("just a normal subject"))
                .isEqualTo(EmailInputTransformer.DEFAULT_TRIGGER_KEY);
        assertThat(t.extractTriggerKey(null)).isEqualTo(EmailInputTransformer.DEFAULT_TRIGGER_KEY);
    }

    @Test
    @DisplayName("Email input params never contain nulls (JSON-safe)")
    void inputParamsAreJsonSafe() {
        WorkflowStartService starts = mock(WorkflowStartService.class);
        when(starts.start(any(), any(), anyMap(), any()))
                .thenReturn(new WorkflowStartService.StartResult(UUID.randomUUID(), false));

        transformer(starts, mock(EmailInitiatorStore.class)).receiveEmail(
                new EmailInputTransformer.WebhookPayload(ACCEPTED, null, null, null, null));

        verify(starts).start(eq(EmailInputTransformer.DEFAULT_TRIGGER_KEY), eq(1),
                org.mockito.ArgumentMatchers.argThat((Map<String, Object> input) -> input.values().stream()
                        .allMatch(v -> v != null)), isNull());
    }
}
