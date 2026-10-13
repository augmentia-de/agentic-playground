package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.WorkflowStartEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.orchestrator.service.DlqService;
import de.augmentia.agentplatform.orchestrator.service.WorkflowEngine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DagTopicListener {

    private static final Logger log = LoggerFactory.getLogger(DagTopicListener.class);

    private final WorkflowEngine engine;
    private final DlqService dlqService;

    public DagTopicListener(WorkflowEngine engine, DlqService dlqService) {
        this.engine = engine;
        this.dlqService = dlqService;
    }

    @KafkaListener(
            topics = {TopicNames.INTAKE, TopicNames.RESULTS, TopicNames.LOOP, TopicNames.CANCELLATION},
            groupId = "orchestrator-dag-group"
    )
    public void onMessage(ConsumerRecord<String, String> record) {
        try {
            switch (record.topic()) {
                case TopicNames.INTAKE -> engine.onIntake(EventJson.read(record.value(), WorkflowStartEvent.class));
                case TopicNames.RESULTS -> engine.onResult(EventJson.read(record.value(), ResultEvent.class));
                case TopicNames.LOOP -> engine.onLoop(EventJson.read(record.value(), ControlEvents.LoopEvent.class));
                case TopicNames.CANCELLATION ->
                        engine.onCancellation(EventJson.read(record.value(), ControlEvents.CancellationEvent.class));
                default -> log.warn("Ignoring unexpected topic {}", record.topic());
            }
        } catch (RuntimeException ex) {
            log.error("Failed processing DAG event topic={} key={}", record.topic(), record.key(), ex);
            dlqService.send(record.topic(), record.key(), record.value(), ex);
        }
    }
}
