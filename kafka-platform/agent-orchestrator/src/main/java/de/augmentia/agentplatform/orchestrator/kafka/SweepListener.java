package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.orchestrator.service.DlqService;
import de.augmentia.agentplatform.orchestrator.service.SweepReaperService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SweepListener {

    private static final Logger log = LoggerFactory.getLogger(SweepListener.class);

    private final SweepReaperService sweepReaperService;
    private final DlqService dlqService;

    public SweepListener(SweepReaperService sweepReaperService, DlqService dlqService) {
        this.sweepReaperService = sweepReaperService;
        this.dlqService = dlqService;
    }

    @KafkaListener(topics = TopicNames.SWEEP, groupId = "orchestrator-reaper-group")
    public void onSweep(ConsumerRecord<String, String> record) {
        try {
            sweepReaperService.runSweep(EventJson.read(record.value(), ControlEvents.SweepEvent.class));
        } catch (RuntimeException ex) {
            log.error("Failed processing sweep event key={}", record.key(), ex);
            dlqService.send(record.topic(), record.key(), record.value(), ex);
        }
    }
}
