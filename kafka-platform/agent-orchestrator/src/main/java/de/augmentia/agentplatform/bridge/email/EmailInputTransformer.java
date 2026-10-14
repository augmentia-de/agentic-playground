package de.augmentia.agentplatform.bridge.email;

import de.augmentia.agentplatform.bridge.redis.EmailInitiatorStore;
import de.augmentia.agentplatform.orchestrator.service.WorkflowStartService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.mail.MessagingException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Receives inbound emails (via SMTP webhook or {@link ImapMailListener}) and starts a
 * workflow through the canonical {@link WorkflowStartService}.
 *
 * <h2>Intake contract</h2>
 *
 * <p>Previously this class published a bespoke {@code ControlEvents.EmailIntakeEvent} directly
 * onto {@code agent.execution.intake}. The orchestrator reads that topic as a
 * {@link de.augmentia.agentplatform.common.event.WorkflowStartEvent}, so the two shapes never
 * matched: the start event had a {@code null} {@code workflowExecutionId}, the engine threw on
 * {@code UUID.fromString(null)} and the message ended up in the DLQ. The feature was dead.
 *
 * <p>Now there is exactly <em>one</em> intake contract: the trigger key from the subject
 * ({@code [QUAD] <name>}, sanitised) is the workflow name, and the email is started through the
 * same {@link WorkflowStartService#start} path the REST API uses. The email payload travels as
 * workflow input parameters ({@code task}, {@code emailBody}, {@code subject},
 * {@code initiatorEmail}, {@code attachments}, {@code triggerKey}). The initiator mapping is
 * stored keyed by the <em>real</em> workflow execution id, which is what
 * {@code MailResponseHandler} later looks up.
 */
@Component
@ConditionalOnProperty(name = "app.email.transformer.enabled", havingValue = "true")
@RestController
@RequestMapping("/api/v1/email")
@EnableAsync
public class EmailInputTransformer {

    private static final Logger log = LoggerFactory.getLogger(EmailInputTransformer.class);
    private static final Pattern TRIGGER_KEY_PATTERN = Pattern.compile("\\[QUAD\\]\\s*(.+)");

    /** Workflow name used when the subject carries no {@code [QUAD] …} trigger. */
    static final String DEFAULT_TRIGGER_KEY = "default";

    @Value("${app.email.address:tasks@quad.local}")
    private String acceptedAddress;

    private final JavaMailSender mailSender;
    private final WorkflowStartService startService;
    private final EmailInitiatorStore initiatorStore;

    public EmailInputTransformer(JavaMailSender mailSender,
                                 WorkflowStartService startService,
                                 EmailInitiatorStore initiatorStore) {
        this.mailSender = mailSender;
        this.startService = startService;
        this.initiatorStore = initiatorStore;
    }

    /** REST endpoint for external mailers (Mailgun/SendGrid webhooks). */
    @PostMapping("/receive")
    public ResponseEntity<Map<String, Object>> receiveEmail(@RequestBody WebhookPayload payload) {
        if (!payload.to().equalsIgnoreCase(acceptedAddress)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Not accepting addresses other than " + acceptedAddress));
        }
        String triggerKey = extractTriggerKey(payload.subject());
        try {
            WorkflowStartService.StartResult started = startService.start(
                    triggerKey, 1, emailInputParams(triggerKey, payload), null);

            // Remember who to answer, keyed by the actual execution id — the same value the
            // orchestrator later reports back on the result topic.
            initiatorStore.storeInitiator(started.executionId().toString(), payload.from());

            log.info("Started workflow '{}' from email (execution {}, duplicate={})",
                    triggerKey, started.executionId(), started.duplicate());
            return ResponseEntity.accepted().body(Map.of(
                    "received", true,
                    "workflowName", triggerKey,
                    "workflowExecutionId", started.executionId().toString(),
                    "duplicate", started.duplicate()));
        } catch (Exception e) {
            // A missing workflow definition, DB or broker outage — our error, not the caller's.
            log.error("Could not start workflow '{}' from email: {}", triggerKey, e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private Map<String, Object> emailInputParams(String triggerKey, WebhookPayload payload) {
        Map<String, Object> input = new LinkedHashMap<>();
        // "task" mirrors the REST/quad convention so a simple single-step workflow can consume it
        // directly; the structured fields are available for richer DAGs.
        input.put("task", payload.body());
        input.put("emailBody", payload.body());
        input.put("subject", payload.subject());
        input.put("initiatorEmail", payload.from());
        input.put("attachments", payload.attachments());
        input.put("triggerKey", triggerKey);
        return input;
    }

    /**
     * Derives the workflow name from the subject: {@code [QUAD] Foo Bar} → {@code foo-bar}.
     * Falls back to {@link #DEFAULT_TRIGGER_KEY}.
     */
    String extractTriggerKey(String subject) {
        if (subject == null) {
            return DEFAULT_TRIGGER_KEY;
        }
        Matcher m = TRIGGER_KEY_PATTERN.matcher(subject);
        return m.find() ? m.group(1).toLowerCase().replaceAll("[^a-z0-9-]", "-") : DEFAULT_TRIGGER_KEY;
    }

    /** Send response email to initiator. */
    public void sendResponseEmail(String to, String subject, String body) {
        try {
            jakarta.mail.internet.MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true);
            helper.setFrom(acceptedAddress);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(body, true);
            mailSender.send(message);
        } catch (MessagingException e) {
            log.error("Failed to send response email to {}: {}", to, e.getMessage());
        }
    }

    public record WebhookPayload(String to, String from, String subject, String body, List<String> attachments) {
        public WebhookPayload {
            to = to != null ? to : "";
            from = from != null ? from : "";
            subject = subject != null ? subject : "";
            body = body != null ? body : "";
            attachments = attachments != null ? attachments : List.of();
        }
    }
}
