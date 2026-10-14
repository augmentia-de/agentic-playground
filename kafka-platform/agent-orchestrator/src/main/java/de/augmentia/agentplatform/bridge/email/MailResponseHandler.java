package de.augmentia.agentplatform.bridge.email;

import de.augmentia.agentplatform.bridge.redis.EmailInitiatorStore;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.model.ResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import jakarta.mail.MessagingException;
import java.time.Instant;
import java.util.Map;

/**
 * Sends result/summary emails back to the original email initiator when
 * a workflow execution completes, fails or needs HITL approval.
 *
 * <p>Pipeline:
 * <pre>
 *     ↓ Orchestrator processes like normal REST-Input
 *     │     
 *     │     ↓ MailResponder (optional)
 *     │     Sends the result email back to the initiator
 *     │     - Completed → "Workflow finished: summary"
 *     │     - NeedsHITL → "Bitte billigen: link zu approval"
 *     │     - Error/DLQ   → "Fehleraufzeichnung: details"
 * </pre>
 *
 * <p>Active via {@code app.email.responder.enabled=true}.
 */
@Component
@ConditionalOnProperty(name = "app.email.responder.enabled", havingValue = "true", matchIfMissing = false)
public class MailResponseHandler {

    private static final Logger log = LoggerFactory.getLogger(MailResponseHandler.class);

    private final JavaMailSender mailSender;
    private final EmailInitiatorStore initiatorStore;
    @Value("${app.email.address:tasks@quad.local}")
    private String senderAddress;
    @Value("${app.email.approval-url:http://localhost:8080/hitl/approve}")
    private String approvalUrl;

    public MailResponseHandler(JavaMailSender mailSender, EmailInitiatorStore initiatorStore) {
        this.mailSender = mailSender;
        this.initiatorStore = initiatorStore;
    }

    /**
     * Listen for result events and send appropriate response emails.
     */
    @KafkaListener(
            topics = "${app.kafka.result-topic:agent.execution.results}",
            groupId = "mail-responder-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleResult(String jsonPayload) {
        try {
            var header = parseHeader(jsonPayload);
            if (header == null) return;

            // Check if this workflow was initiated by email (stored in Redis during intake)
            String initiatorEmail = lookupInitiatorEmail(header.workflowId());
            if (initiatorEmail == null) return; // Not an email-initiated workflow

            switch (header.status()) {
                case COMPLETED -> sendCompletedEmail(initiatorEmail, header);
                case NEEDS_HITL -> sendHitlRequestEmail(initiatorEmail, header);
                case FAILED_RETRYABLE, FAILED_FATAL -> sendErrorEmail(initiatorEmail, header);
            }

        } catch (Exception e) {
            log.error("Failed to process result event for email responder", e);
        }
    }

    // ── Email senders ────────────────────────────────────────────────

    private void sendCompletedEmail(String to, ResultMeta header) {
        String subject = "Workflow Complete: " + header.taskId();
        String body = """
                Workflow completed successfully.
                
                Workflow ID: %s
                Task ID: %s
                Agent Type: %s
                Duration: %d ms
                Completed At: %s
                """.formatted(
                header.workflowId(), header.taskId(), header.agentType(),
                header.executionDurationMs(), header.completedAt()
        );

        sendEmail(to, subject, body);
        log.info("Sent completion email to {}", to);
    }

    private void sendHitlRequestEmail(String to, ResultMeta header) {
        String subject = "Action Required: HITL Approval for " + header.taskId();
        String approvalLink = approvalUrl + "?ticketId=" + header.taskId();
        String body = """
                Workflow requires your manual approval.
                
                Status: NEEDS_HITL
                Workflow ID: %s
                Task ID: %s
                
                Please review and approve here: %s
                
                This request expires in 24 hours.
                """.formatted(header.workflowId(), header.taskId(), approvalLink);

        sendEmail(to, subject, body);
        log.info("Sent HITL approval request to {}", to);
    }

    private void sendErrorEmail(String to, ResultMeta header) {
        boolean retryable = header.status() == ResultStatus.FAILED_RETRYABLE;
        String status = retryable ? "Retryable failure" : "Fatal error";
        String subject = "Workflow Error: " + header.taskId();
        
        String body = """
                %s
                
                Workflow ID: %s
                Task ID: %s
                Agent Type: %s
                Duration: %d ms
                
                The workflow will %s automatically.
                """.formatted(
                status,
                header.workflowId(), header.taskId(), header.agentType(),
                header.executionDurationMs(),
                retryable ? "be retried" : "not be retried"
        );

        sendEmail(to, subject, body);
        log.info("Sent error email to {} for {}", to, header.taskId());
    }

    private void sendEmail(String to, String subject, String body) {
        try {
            jakarta.mail.internet.MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true);
            helper.setFrom(senderAddress);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(body, true);
            mailSender.send(message);
        } catch (MessagingException e) {
            log.error("Failed to send email to {}: {}", to, e.getMessage());
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private String lookupInitiatorEmail(String workflowId) {
        // The mapping was written by EmailInputTransformer at intake, keyed by the workflow
        // execution id that the orchestrator also reports on the result topic. Redis failures are
        // non-fatal: a missing mapping simply means "not an email-initiated workflow".
        try {
            return initiatorStore.lookupInitiator(workflowId);
        } catch (RuntimeException e) {
            log.warn("Could not look up email initiator for workflow {}: {}", workflowId, e.getMessage());
            return null;
        }
    }

    private ResultMeta parseHeader(String jsonPayload) {
        try {
            ResultEvent result = EventJson.read(jsonPayload, ResultEvent.class);
            return toResultMeta(result);
        } catch (Exception e) {
            log.warn("Failed to parse result event from Kafka message", e);
            return null;
        }
    }

    private ResultMeta toResultMeta(ResultEvent event) {
        Map<String, Object> data = event.resultData() == null ? Map.of() : event.resultData();
        ResultStatus status = ResultStatus.COMPLETED;
        if (data.get("status") instanceof String s) {
            try {
                status = ResultStatus.valueOf(s);
            } catch (IllegalArgumentException ignored) {
                // fallback COMPLETED
            }
        }
        String agentType = data.get("agentType") instanceof String s ? s : "";
        long duration = data.get("durationMs") instanceof Number n ? n.longValue() : 0;
        Instant completedAt = event.finishedAtMs() > 0
                ? Instant.ofEpochMilli(event.finishedAtMs()) : Instant.now();
        return new ResultMeta(event.workflowId(), event.taskId(), status, agentType, duration, completedAt);
    }

    private record ResultMeta(
            String workflowId,
            String taskId,
            ResultStatus status,
            String agentType,
            long executionDurationMs,
            Instant completedAt
    ) {}
}
