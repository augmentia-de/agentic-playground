package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.orchestrator.service.WorkflowQueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * WP02 / B13 — GDPR delete/anonymize must not report a false success.
 */
class GdprControllerTest {

    private final GdprController controller = new GdprController(mock(WorkflowQueryService.class));

    @Test
    @DisplayName("Delete without confirm is rejected")
    void deleteWithoutConfirmRejected() {
        assertThat(controller.deletePersonalData("session-1", null, false).getStatusCode().value())
                .isEqualTo(400);
    }

    @Test
    @DisplayName("Delete without an identifier is rejected")
    void deleteWithoutIdentifierRejected() {
        assertThat(controller.deletePersonalData(null, null, true).getStatusCode().value())
                .isEqualTo(400);
    }

    @Test
    @DisplayName("Delete of a session returns 501 — no false 200")
    void deleteSessionIsNotImplemented() {
        var resp = controller.deletePersonalData("session-1", null, true);

        assertThat(resp.getStatusCode().value()).isEqualTo(501);
        assertThat(resp.getBody()).containsEntry("status", "not_implemented");
    }

    @Test
    @DisplayName("Delete of a workflow returns 501 — no false 200")
    void deleteWorkflowIsNotImplemented() {
        var resp = controller.deletePersonalData(null, "11111111-1111-1111-1111-111111111111", true);

        assertThat(resp.getStatusCode().value()).isEqualTo(501);
    }

    @Test
    @DisplayName("Anonymize without a session is rejected")
    void anonymizeWithoutSessionRejected() {
        assertThat(controller.anonymizePersonalData(null).getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("Anonymize returns 501 — no false 200")
    void anonymizeIsNotImplemented() {
        var resp = controller.anonymizePersonalData("session-1");

        assertThat(resp.getStatusCode().value()).isEqualTo(501);
        assertThat(resp.getBody()).containsEntry("status", "not_implemented");
    }

    @Test
    @DisplayName("Export with an invalid workflow id is a 400")
    void exportInvalidWorkflowIdRejected() {
        var resp = controller.exportPersonalData(null, "not-a-uuid");

        assertThat(resp.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("Export without any identifier is a 400")
    void exportWithoutIdentifierRejected() {
        var resp = controller.exportPersonalData(null, null);

        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        assertThat(resp.getBody()).isInstanceOf(Map.class);
    }
}
