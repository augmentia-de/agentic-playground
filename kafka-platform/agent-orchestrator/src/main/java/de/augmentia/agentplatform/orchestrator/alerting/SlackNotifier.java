package de.augmentia.agentplatform.orchestrator.alerting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Slack notification dispatcher for HITL escalations and workflow alerts.
 */
@Component
@ConditionalOnProperty(name = "app.slack.enabled", havingValue = "true")
public class SlackNotifier {
    private static final Logger log = LoggerFactory.getLogger(SlackNotifier.class);
    
    @Value("${app.slack.webhook-url:#{null}}")
    private String webhookUrl;
    @Value("${app.slack.channel:}")
    private String channel;
    @Value("${app.slack.username:Agent Platform}")
    private String username;

    public void sendHitlEscalation(String workflowId, String stepId, String reason, 
                                    String assignedRole, String approvalEndpoint) {
        if (webhookUrl == null || webhookUrl.isBlank()) return;

        Map<String, Object> payload = Map.of(
                "channel", channel,
                "username", username,
                "text", "*🚨 Action Required*\nWorkflow: `" + workflowId + "`\nStep: `" + stepId + "`\nReason: " + reason,
                "blocks", List.of(
                        Map.of("type", "header", "text", Map.of("type", "mrkdwn", "text", "⚠️ Action Required")),
                        Map.of("type", "section", "text", Map.of("type", "mrkdwn", "text", 
                                "*Workflow:* `" + workflowId + "`\n*Step:* `" + stepId + "`\n*Reason:* " + reason)),
                        Map.of("type", "actions", "elements", List.of(
                                Map.of("type", "button", "text", Map.of("type", "plain_text", "text", "✅ Approve"),
                                        "value", "action=approve&workflowId=" + workflowId),
                                Map.of("type", "button", "text", Map.of("type", "plain_text", "text", "❌ Reject"),
                                        "style", "danger", "value", "action=reject&workflowId=" + workflowId),
                                Map.of("type", "button", "text", Map.of("type", "plain_text", "text", "🔁 Retry"),
                                        "value", "action=retry&workflowId=" + workflowId)
                        ))
                )
        );

        try {
            var response = WebClient.create().post()
                    .uri(webhookUrl)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(payload)
                    .retrieve()
                    .toEntity(String.class)
                    .block();

            if (response != null && response.getStatusCode().is2xxSuccessful()) {
                log.info("Slack HITL escalation sent: workflow={}", workflowId);
            } else {
                log.warn("Slack API returned {}: {}", response != null ? response.getStatusCode() : "no response", "");
            }
        } catch (Exception e) {
            log.error("Failed to send Slack message: {}", e.getMessage());
        }
    }

    public void sendNotification(String title, String text, String color) {
        if (webhookUrl == null || webhookUrl.isBlank()) return;
        Map<String, Object> payload = Map.of(
                "channel", channel,
                "username", username,
                "attachments", List.of(Map.of(
                        "color", color != null ? color : "#36a64f",
                        "title", title,
                        "text", text,
                        "ts", Instant.now().getEpochSecond()
                ))
        );
        try {
            var response = WebClient.create().post()
                    .uri(webhookUrl)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(payload)
                    .retrieve()
                    .toEntity(String.class)
                    .block();
            if (response != null && response.getStatusCode().is2xxSuccessful()) {
                log.info("Slack notification sent: {}", title);
            }
        } catch (Exception e) {
            log.error("Failed to send Slack notification: {}", e.getMessage());
        }
    }
}

