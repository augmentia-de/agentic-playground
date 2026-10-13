package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.event.WorkflowStartEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.HitlAction;
import de.augmentia.agentplatform.orchestrator.service.DlqService;
import de.augmentia.agentplatform.orchestrator.service.SweepReaperService;
import de.augmentia.agentplatform.orchestrator.service.WorkflowEngine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Behaviour of the Kafka-to-engine adapter.
 *
 * <p>This layer was completely untested — it is the boundary between
 * deserialisation and orchestration. Exactly at this boundary lay one of the
 * blockers found during the end-to-end run: {@code interceptor.classes} pointed
 * at a non-existent class, causing <em>every</em> publish to fail.
 * No test of the engine below would have seen that.
 *
 * <p>What is checked is the routing (which topic lands in which engine method)
 * and the error behaviour (deserialisable, but the engine throws → DLQ).
 */
@DisplayName("Kafka-Listener routen auf die Engine")
class KafkaListenerRoutingTest {

    private static ConsumerRecord<String, String> record(String topic, String key, String value) {
        return new ConsumerRecord<>(topic, 0, 0L, key, value);
    }

    // ── DagTopicListener ─────────────────────────────────────────────

    @Test
    @DisplayName("INTAKE landet in engine.onIntake")
    void intakeGoesToOnIntake() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new DagTopicListener(engine, dlq);

        var event = new WorkflowStartEvent("exec-1", "def-1", "wf", 1, Map.of(), "idem-1", Instant.EPOCH);
        listener.onMessage(record(TopicNames.INTAKE, "exec-1", EventJson.write(event)));

        verify(engine).onIntake(any(WorkflowStartEvent.class));
        verifyNoInteractions(dlq);
    }

    @Test
    @DisplayName("RESULTS landet in engine.onResult")
    void resultsGoToOnResult() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new DagTopicListener(engine, dlq);

        var task = new TaskEvent(
                new TaskEvent.Header("wf-1", UUID.randomUUID().toString(), null, 1, 1L),
                "researcher", "step-1", 1, "prompt", Map.of(), java.util.List.of(), 60_000L);
        var result = new ResultEvent(
                new ResultEvent.Header("tp", 1L), "wf-1", task.header().taskId(),
                "step-1", 1, 1, 1L, 2L, Map.of("status", "COMPLETED"));

        listener.onMessage(record(TopicNames.RESULTS, "wf-1", EventJson.write(result)));

        verify(engine).onResult(any(ResultEvent.class));
        verifyNoInteractions(dlq);
    }

    @Test
    @DisplayName("LOOP and CANCELLATION land in the respective methods")
    void loopAndCancellationRouting() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new DagTopicListener(engine, dlq);

        var loop = new ControlEvents.LoopEvent("exec-1", "loop-step", 1, false, 3, Map.of());
        listener.onMessage(record(TopicNames.LOOP, "exec-1", EventJson.write(loop)));
        verify(engine).onLoop(any(ControlEvents.LoopEvent.class));

        var cancel = new ControlEvents.CancellationEvent("exec-1", "abort", "operator");
        listener.onMessage(record(TopicNames.CANCELLATION, "exec-1", EventJson.write(cancel)));
        verify(engine).onCancellation(any(ControlEvents.CancellationEvent.class));
    }

    @Test
    @DisplayName("An unknown topic does not reach the engine")
    void unknownTopicIsIgnored() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new DagTopicListener(engine, dlq);

        listener.onMessage(record("agent.unbekannt", "k", "{}"));

        verifyNoInteractions(engine);
        verifyNoInteractions(dlq);
    }

    @Test
    @DisplayName("A non-deserialisable payload goes to the DLQ, not into the engine")
    void unparseablePayloadGoesToDlq() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new DagTopicListener(engine, dlq);

        listener.onMessage(record(TopicNames.INTAKE, "exec-1", "{ kaputt ,,, }"));

        verify(engine, never()).onIntake(any());
        verify(dlq).send(eq(TopicNames.INTAKE), eq("exec-1"), eq("{ kaputt ,,, }"), any(RuntimeException.class));
    }

    @Test
    @DisplayName("If the engine throws, the result lands in the DLQ")
    void engineFailureGoesToDlq() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        doThrow(new IllegalStateException("kaputt")).when(engine).onIntake(any());
        var listener = new DagTopicListener(engine, dlq);

        var event = new WorkflowStartEvent("exec-1", "def-1", "wf", 1, Map.of(), "idem-1", Instant.EPOCH);
        listener.onMessage(record(TopicNames.INTAKE, "exec-1", EventJson.write(event)));

        verify(dlq).send(eq(TopicNames.INTAKE), eq("exec-1"), anyString(), any(RuntimeException.class));
    }

    // ── HitlListener ─────────────────────────────────────────────────

    @Test
    @DisplayName("HITL-Event landet in engine.onHitl")
    void hitlGoesToEngine() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new HitlListener(engine, dlq);

        var event = new ControlEvents.HitlEvent("exec-1", "ticket-1", "step-1", 1,
                HitlAction.APPROVED, "operator");
        listener.onHitl(record(TopicNames.HITL, "exec-1", EventJson.write(event)));

        var captor = ArgumentCaptor.forClass(ControlEvents.HitlEvent.class);
        verify(engine).onHitl(captor.capture());
        assertThat(captor.getValue().action()).isEqualTo(HitlAction.APPROVED);
        assertThat(captor.getValue().decidedBy()).isEqualTo("operator");
    }

    @Test
    @DisplayName("Fehlerhafter HITL-Event geht in die DLQ")
    void hitlFailureGoesToDlq() {
        WorkflowEngine engine = mock(WorkflowEngine.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new HitlListener(engine, dlq);

        listener.onHitl(record(TopicNames.HITL, "exec-1", "kein-json"));

        verify(engine, never()).onHitl(any());
        verify(dlq).send(eq(TopicNames.HITL), eq("exec-1"), eq("kein-json"), any(RuntimeException.class));
    }

    // ── SweepListener ────────────────────────────────────────────────

    @Test
    @DisplayName("Sweep-Event landet im SweepReaper")
    void sweepGoesToReaper() {
        SweepReaperService reaper = mock(SweepReaperService.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new SweepListener(reaper, dlq);

        var event = new ControlEvents.SweepEvent("sweep-1", Instant.EPOCH, "ALL");
        listener.onSweep(record(TopicNames.SWEEP, "sweep-1", EventJson.write(event)));

        verify(reaper).runSweep(any(ControlEvents.SweepEvent.class));
        verifyNoInteractions(dlq);
    }

    @Test
    @DisplayName("Fehlerhafter Sweep-Event geht in die DLQ")
    void sweepFailureGoesToDlq() {
        SweepReaperService reaper = mock(SweepReaperService.class);
        DlqService dlq = mock(DlqService.class);
        var listener = new SweepListener(reaper, dlq);

        listener.onSweep(record(TopicNames.SWEEP, "sweep-1", "{{{"));

        verify(reaper, never()).runSweep(any());
        verify(dlq).send(eq(TopicNames.SWEEP), eq("sweep-1"), anyString(), any(RuntimeException.class));
    }

    // ── SweepScheduler ───────────────────────────────────────────────

    @Test
    @DisplayName("SweepScheduler publiziert einen SweepEvent mit eigener sweepId")
    void schedulerPublishesSweepEvent() {
        OrchestratorEventPublisher publisher = mock(OrchestratorEventPublisher.class);
        var scheduler = new SweepScheduler(publisher,
                java.time.Clock.fixed(Instant.parse("2026-09-25T12:00:00Z"), java.time.ZoneOffset.UTC));

        scheduler.publishSweep();

        var topic = ArgumentCaptor.forClass(String.class);
        var key = ArgumentCaptor.forClass(String.class);
        var payload = ArgumentCaptor.forClass(String.class);
        verify(publisher).publish(topic.capture(), key.capture(), payload.capture());

        assertThat(topic.getValue()).isEqualTo(TopicNames.SWEEP);

        ControlEvents.SweepEvent published =
                EventJson.read(payload.getValue(), ControlEvents.SweepEvent.class);
        assertThat(published.sweepId()).isNotBlank();
        assertThat(published.generatedAt())
                .as("Der Zeitstempel kommt aus der injizierten Uhr")
                .isEqualTo(Instant.parse("2026-09-25T12:00:00Z"));
        assertThat(key.getValue())
                .as("the key is the sweepId — that makes the sweep idempotently assignable")
                .isEqualTo(published.sweepId());
    }
}
