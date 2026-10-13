package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.UUID;

@Component
public class SweepScheduler {

    private final OrchestratorEventPublisher publisher;
    private final Clock clock;

    public SweepScheduler(OrchestratorEventPublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${app.orchestration.sweep-interval-ms:5000}")
    public void publishSweep() {
        ControlEvents.SweepEvent event = new ControlEvents.SweepEvent(UUID.randomUUID().toString(), clock.instant(), "ALL");
        publisher.publish(TopicNames.SWEEP, event.sweepId(), EventJson.write(event));
    }
}
