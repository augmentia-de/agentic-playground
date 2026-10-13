package de.augmentia.agentplatform.orchestrator.alerting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * PagerDuty alert sender for high-priority escalations.
 */
@Component
@ConditionalOnProperty(name = "app.pagerduty.enabled", havingValue = "true")
public class PagerDutyAlertSender {

    private static final Logger log = LoggerFactory.getLogger(PagerDutyAlertSender.class);

    @Value("${app.pagerduty.integration-key:#{null}}")
    private String integrationKey;
    @Value("${app.pagerduty.source:agent-platform}")
    private String source;
    private final WebClient webClient;

    public PagerDutyAlertSender(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    public void sendCritical(String summary, String workflowId, String stepId) {
        sendEvent("critical", summary, workflowId, stepId);
    }

    public void sendMajor(String summary, String workflowId, String stepId) {
        sendEvent("major", summary, workflowId, stepId);
    }

    public void sendMinor(String summary, String workflowId, String stepId) {
        sendEvent("minor", summary, workflowId, stepId);
    }

    public void resolve(String incidentKey) {
        if (integrationKey == null || integrationKey.isBlank()) return;
        try {
            Map<String, Object> payload = Map.of(
                    "routing_key", integrationKey,
                    "event_action", "resolve",
                    "payload", Map.of("summary", "Resolved: " + incidentKey, "severity", "info", "source", source),
                    "incident_key", incidentKey
            );
            Mono<Void> mono = webClient.post()
                    .uri("https://events.pagerduty.com/v2/enqueue")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(payload)
                    .retrieve().bodyToMono(Void.class);
            mono.block();
            log.info("PagerDuty resolve sent for {}", incidentKey);
        } catch (Exception e) {
            log.error("Failed to send PagerDuty resolve: {}", e.getMessage());
        }
    }

    private void sendEvent(String severity, String summary, String workflowId, String stepId) {
        if (integrationKey == null || integrationKey.isBlank()) return;
        try {
            Map<String, Object> payload = Map.of(
                    "routing_key", integrationKey,
                    "event_action", "trigger",
                    "payload", Map.of("summary", summary, "severity", severity, "source", source,
                            "component", "workflow-engine"),
                    "details", Map.of("workflow_id", workflowId, "step_id", stepId)
            );
            Mono<Void> mono = webClient.post()
                    .uri("https://events.pagerduty.com/v2/enqueue")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(payload)
                    .retrieve().bodyToMono(Void.class);
            mono.block();
            log.info("PagerDuty {} event sent: {}", severity, summary);
        } catch (Exception e) {
            log.error("Failed to send PagerDuty event: {}", e.getMessage());
        }
    }
}
