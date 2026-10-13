package de.augmentia.agentplatform.bridge.email;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Receives inbound emails (via SMTP) and transforms them into Kafka intake events.
 */
@Component
@ConditionalOnProperty(name = "app.email.transformer.enabled", havingValue = "true")
@RestController
@RequestMapping("/api/v1/email")
@EnableAsync
public class EmailInputTransformer {

    private static final Logger log = LoggerFactory.getLogger(EmailInputTransformer.class);
    private static final Pattern TRIGGER_KEY_PATTERN = Pattern.compile("\\[QUAD\\]\\s*(.+)");

    @Value("${app.email.address:tasks@quad.local}")
    private String acceptedAddress;
    private final JavaMailSender mailSender;
    private final KafkaProducer kafkaProducer;

    public EmailInputTransformer(JavaMailSender mailSender, KafkaProducer kafkaProducer) {
        this.mailSender = mailSender;
        this.kafkaProducer = kafkaProducer;
    }

    /** REST endpoint for external mailers (Mailgun/SendGrid webhooks). */
    @PostMapping("/receive")
    public ResponseEntity<Map<String, Object>> receiveEmail(@RequestBody WebhookPayload payload) {
        if (!payload.to().equalsIgnoreCase(acceptedAddress)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Not accepting addresses other than " + acceptedAddress));
        }
        try {
            ControlEvents.EmailIntakeEvent event = extractWorkflowEvent(payload);
            kafkaProducer.publish(TopicNames.INTAKE, event.workflowId(), EventJson.write(event));
            return ResponseEntity.accepted().body(Map.of("received", true, "workflowId", event.workflowId()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private String extractTriggerKey(String subject) {
        Matcher m = TRIGGER_KEY_PATTERN.matcher(subject);
        return m.find() ? m.group(1).toLowerCase().replaceAll("[^a-z0-9-]", "-") : "default";
    }

    private ControlEvents.EmailIntakeEvent extractWorkflowEvent(WebhookPayload payload) {
        String triggerKey = extractTriggerKey(payload.subject());
        UUID workflowId = UUID.randomUUID();
        return new ControlEvents.EmailIntakeEvent(workflowId.toString(), triggerKey, payload.body(), 
                payload.attachments(), payload.from(), Instant.now());
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
