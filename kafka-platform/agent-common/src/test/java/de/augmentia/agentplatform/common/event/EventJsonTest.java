package de.augmentia.agentplatform.common.event;

import de.augmentia.agentplatform.common.model.ResultStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EventJsonTest {

    @Test
    void roundTripsTaskEvent() {
        AgentTaskEvent event = new AgentTaskEvent(
                new AgentTaskEvent.TaskHeader("wf-1", "task-1", null, "trace-1", 1, 3,
                        Instant.parse("2026-01-01T10:00:00Z"), 60_000L),
                new AgentTaskEvent.AgentConfig("researcher", 1, "mock-model", 0.0d),
                new AgentTaskEvent.TaskPayload("do work", List.of("research_step"), Map.of("stepId", "research_step"))
        );

        String json = EventJson.write(event);
        AgentTaskEvent parsed = EventJson.read(json, AgentTaskEvent.class);

        assertThat(parsed).isEqualTo(event);
    }

    @Test
    void roundTripsResultEventAndIgnoresUnknownFields() {
        AgentResultEvent event = new AgentResultEvent(
                new AgentResultEvent.ResultHeader("wf-1", "task-1", "researcher", ResultStatus.COMPLETED,
                        Instant.parse("2026-01-01T10:05:00Z"), 1200L),
                new AgentResultEvent.ResultOutput(Map.of("confidence", 0.83), null, null),
                new AgentResultEvent.Usage(10, 20, 0.0d)
        );

        String json = EventJson.write(event).replaceFirst("\\{", "{\"unknownField\":\"x\",");
        AgentResultEvent parsed = EventJson.read(json, AgentResultEvent.class);

        assertThat(parsed.header()).isEqualTo(event.header());
        assertThat(parsed.output().resultData()).containsEntry("confidence", 0.83);
    }

    @Test
    void roundTripsControlEvents() {
        ControlEvents.LoopEvent event = new ControlEvents.LoopEvent("wf-1", "refinement_loop", 2, true, 3,
                Map.of("confidence", 0.9d));

        ControlEvents.LoopEvent parsed = EventJson.read(EventJson.write(event), ControlEvents.LoopEvent.class);

        assertThat(parsed).isEqualTo(event);
    }
}
