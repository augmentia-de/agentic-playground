package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.orchestrator.service.DlqService;
import de.augmentia.agentplatform.orchestrator.service.WorkflowEngine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class HitlListener {

    private static final Logger log = LoggerFactory.getLogger(HitlListener.class);

    private final WorkflowEngine engine;
    private final DlqService dlqService;

    public HitlListener(WorkflowEngine engine, DlqService dlqService) {
        this.engine = engine;
        this.dlqService = dlqService;
    }

    @KafkaListener(topics = TopicNames.HITL, groupId = "alerting-hitl-group")
    public void onHitl(ConsumerRecord<String, String> record) {
        try {
            engine.onHitl(EventJson.read(record.value(), ControlEvents.HitlEvent.class));
        } catch (RuntimeException ex) {
            log.error("Failed processing HITL event key={}", record.key(), ex);
            dlqService.send(record.topic(), record.key(), record.value(), ex);
        }
    }
}
