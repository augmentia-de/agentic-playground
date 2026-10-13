package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.orchestrator.db.JdbcDlqStore;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;

@Service
public class DlqService {

    private final JdbcDlqStore dlqStore;
    private final OrchestratorEventPublisher publisher;
    private final Clock clock;

    public DlqService(JdbcDlqStore dlqStore, OrchestratorEventPublisher publisher, Clock clock) {
        this.dlqStore = dlqStore;
        this.publisher = publisher;
        this.clock = clock;
    }

    public void send(String originalTopic, String key, Object payload, Throwable error) {
        String payloadJson = payload instanceof String raw ? raw : EventJson.write(payload);
        DlqEnvelope envelope = new DlqEnvelope(
                originalTopic,
                key,
                payloadJson,
                error == null ? null : error.getClass().getName(),
                error == null ? null : error.getMessage(),
                clock.instant()
        );
        String envelopeJson = EventJson.write(envelope);
        dlqStore.insert(originalTopic, key, envelopeJson,
                error == null ? null : error.getClass().getName(),
                error == null ? null : error.getMessage());
        publisher.publishAfterCommit(TopicNames.DLQ, key == null ? "unknown" : key, envelopeJson);
    }

    public record DlqEnvelope(
            String originalTopic,
            String originalKey,
            String payload,
            String errorClass,
            String errorMessage,
            Instant occurredAt
    ) {}
}
