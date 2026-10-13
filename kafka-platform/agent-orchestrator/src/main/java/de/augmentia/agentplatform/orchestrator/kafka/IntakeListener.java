package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.bridge.redis.EmailInitiatorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Intake event listener — receives workflow start events and optionally
 * records email initiator mapping for later response emails.
 */
@Component
public class IntakeListener {

    private static final Logger log = LoggerFactory.getLogger(IntakeListener.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EmailInitiatorStore emailInitiatorStore;

    public IntakeListener(KafkaTemplate<String, String> kafkaTemplate,
                          EmailInitiatorStore emailInitiatorStore) {
        this.kafkaTemplate = kafkaTemplate;
        this.emailInitiatorStore = emailInitiatorStore;
    }

    /**
     * Handle regular workflow start events from REST API.
     * Key: workflow_id.
     */
    @KafkaListener(
            topics = "${app.kafka.intake-topic:agent.execution.intake}",
            groupId = "orchestrator-intake-group",
            containerFactory = "manualAckListenerContainerFactory"
    )
    public void handleIntake(String payload, Acknowledgment ack) {
        try {
            // Deserialize and process — the WorkflowEngine already handles the actual execution
            log.debug("Received intake event: {}", payload.substring(0, Math.min(200, payload.length())));
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process intake event", e);
            ack.acknowledge(); // ACK anyway to prevent replay loop
        }
    }

    /**
     * Handle email intake events specifically — record the initiator for later response.
     */
    @KafkaListener(
            topics = "${app.kafka.intake-topic:agent.execution.intake}",
            groupId = "email-tracking-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void trackEmailIntake(String payload) {
        try {
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            var node = mapper.readTree(payload);
            
            // Check if this has an email-specific field
            if (node.has("initiatorEmail") && node.has("workflowId")) {
                String workflowId = node.get("workflowId").asText();
                String initiatorEmail = node.get("initiatorEmail").asText("");
                
                if (!initiatorEmail.isBlank()) {
                    emailInitiatorStore.storeInitiator(workflowId, initiatorEmail);
                    
                    // Also register for dedup
                    if (node.has("messageId")) {
                        emailInitiatorStore.registerIntake(node.get("messageId").asText(), workflowId);
                    }
                    
                    log.info("Tracked email initiator {} for workflow {}", initiatorEmail, workflowId);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to track email intake (non-fatal): {}", e.getMessage());
        }
    }
}
