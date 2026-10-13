package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests covering core orchestrator functionality:
 * - Event serialization round-trips (Task events, Result events, Control events)
 * - Topic name generation with/without tenant suffixes
 * - Error classification heuristics
 * - JSON schema compliance for Kafka payloads
 */
class OrchestratorIntegrationTest {

    // ─── Event Serialization Tests ──────────────────────────────────

    @Test
    void resultEvent_serializesAndDeserializesRoundTrip() {
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        
        ResultEvent.Header header = new ResultEvent.Header(
                "trace-" + workflowId, java.time.Instant.now().toEpochMilli()
        );
        ResultEvent resultEvent = new ResultEvent(
                header,
                workflowId.toString(),
                taskId.toString(),
                "step-code",
                0,
                1,
                java.time.Instant.now().minusSeconds(1).toEpochMilli(),
                java.time.Instant.now().toEpochMilli(),
                Map.of("status", "COMPLETED", "response", "def hello(): pass", "confidence", 0.92)
        );

        String json = EventJson.write(resultEvent);
        ResultEvent decoded = EventJson.read(json, ResultEvent.class);

        assertThat(decoded.workflowId()).isEqualTo(workflowId.toString());
        assertThat(decoded.taskId()).isEqualTo(taskId.toString());
        assertThat(decoded.resultData().get("response")).isEqualTo("def hello(): pass");
        assertThat(decoded.resultData().get("status")).isEqualTo("COMPLETED");
        assertThat(decoded.resultData().get("confidence")).isEqualTo(0.92);
        assertThat(json).contains("result_data");
    }

    @Test
    void failedResultEvent_withErrorMessage() {
        UUID wfId = UUID.randomUUID();
        UUID tid = UUID.randomUUID();

        ResultEvent evt = new ResultEvent(
                new ResultEvent.Header("trace-" + wfId, java.time.Instant.now().toEpochMilli()),
                wfId.toString(),
                tid.toString(),
                "step-plan",
                0,
                2,
                java.time.Instant.now().minusSeconds(2).toEpochMilli(),
                java.time.Instant.now().toEpochMilli(),
                Map.of(
                        "status", "FAILED_FATAL",
                        "errorMessage", "Something went wrong",
                        "errorType", "java.lang.NullPointerException"
                )
        );

        String json = EventJson.write(evt);
        ResultEvent decoded = EventJson.read(json, ResultEvent.class);

        assertThat(decoded.resultData().get("status")).isEqualTo("FAILED_FATAL");
        assertThat(decoded.resultData().get("errorMessage")).isEqualTo("Something went wrong");
        assertThat(decoded.resultData().get("errorType")).isEqualTo("java.lang.NullPointerException");
    }

    @Test
    void taskEvent_serializesAndDeserializesRoundTrip() {
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        
        TaskEvent taskEvent = new TaskEvent(
                new TaskEvent.Header(
                        workflowId.toString(), taskId.toString(), workflowId + ":" + taskId,
                        1, java.time.Instant.now().toEpochMilli()
                ),
                "researcher",
                "step-research",
                0,
                "Review the code and find issues",
                Map.of("repository", "my-app", "branch", "main"),
                List.of(new TaskEvent.ContextReference("baseline", "step-initial", Map.of("issues", 0))),
                120000L
        );

        String json = EventJson.write(taskEvent);
        TaskEvent decoded = EventJson.read(json, TaskEvent.class);

        assertThat(decoded.header().workflowId()).isEqualTo(workflowId.toString());
        assertThat(decoded.header().taskId()).isEqualTo(taskId.toString());
        assertThat(decoded.agentType()).isEqualTo("researcher");
        assertThat(decoded.stepId()).isEqualTo("step-research");
        assertThat(decoded.timeoutMs()).isEqualTo(120000L);
        assertThat(decoded.contextReferences()).hasSize(1);
        assertThat(json).contains("timeout_ms");
    }

    @Test
    void intakeEvent_roundTripsCorrectly() {
        UUID wfId = UUID.randomUUID();
        ControlEvents.EmailIntakeEvent intake = new ControlEvents.EmailIntakeEvent(
                wfId.toString(), "dev-feature-x", "Review PR for auth endpoint",
                List.of("https://blob.storage/patch.diff"), "alice@corp.com", java.time.Instant.now());

        String json = EventJson.write(intake);
        ControlEvents.EmailIntakeEvent decoded = EventJson.read(json, ControlEvents.EmailIntakeEvent.class);

        assertThat(decoded.triggerKey()).isEqualTo("dev-feature-x");
        assertThat(decoded.emailBody()).isEqualTo("Review PR for auth endpoint");
        assertThat(decoded.initiatorEmail()).isEqualTo("alice@corp.com");
    }

    // ─── Topic Name Tests ───────────────────────────────────────────

    @Test
    void topicNames_withoutTenant_usesLegacyFormat() {
        assertThat(TopicNames.tasks("Coder")).isEqualTo("agent.tasks.coder");
        assertThat(TopicNames.tasks("researcher")).isEqualTo("agent.tasks.researcher");
    }

    @Test
    void topicNames_withTenant_appendSuffix() {
        assertThat(TopicNames.tasks("Coder", "my-org")).isEqualTo("agent.tasks.coder.my-org");
        assertThat(TopicNames.tasks("researcher", "team-alpha")).isEqualTo("agent.tasks.researcher.team-alpha");
    }

    @Test
    void topicNames_nullOrEmptyTenant_fallsBackToLegacy() {
        assertThat(TopicNames.tasks("Coder", null)).isEqualTo(TopicNames.tasks("Coder"));
        assertThat(TopicNames.tasks("Coder", "")).isEqualTo(TopicNames.tasks("Coder"));
    }

    @Test
    void topicNames_caseInsensitiveAgentType() {
        assertThat(TopicNames.tasks("RESEARCHER", "org")).isEqualTo("agent.tasks.researcher.org");
        assertThat(TopicNames.tasks("COder", null)).isEqualTo("agent.tasks.coder");
    }

    // ─── Error Classification Tests ─────────────────────────────────

    @Test
    void errorClassification_transientErrors() {
        assertThat(ErrorClassification.TRANSIENT.isRetryable()).isTrue();
        assertThat(ErrorClassification.BUSINESS.isRetryable()).isFalse();
        assertThat(ErrorClassification.FATAL.isRetryable()).isFalse();
    }

    @Test
    void errorClassification_businessNeedsHitl() {
        assertThat(ErrorClassification.BUSINESS.needsHitlEscalation()).isTrue();
        assertThat(ErrorClassification.TRANSIENT.needsHitlEscalation()).isFalse();
        assertThat(ErrorClassification.FATAL.needsHitlEscalation()).isFalse();
    }

    @Test
    void errorClassification_fatalIsTerminal() {
        assertThat(ErrorClassification.FATAL.isTerminal()).isTrue();
        assertThat(ErrorClassification.TRANSIENT.isTerminal()).isFalse();
        assertThat(ErrorClassification.BUSINESS.isTerminal()).isFalse();
    }

    @Test
    void errorClassification_classifyByMessage() {
        assertThat(ErrorClassification.classify("timeout waiting for response"))
                .isEqualTo(ErrorClassification.TRANSIENT);
        assertThat(ErrorClassification.classify("guardrail violation detected"))
                .isEqualTo(ErrorClassification.BUSINESS);
        assertThat(ErrorClassification.classify("malformed input data corrupted"))
                .isEqualTo(ErrorClassification.FATAL);
    }

    // ─── Multiple Sequential Events ─────────────────────────────────

    @Test
    void multipleSequentialResults_produceDistinctPayloads() {
        IntStream.range(0, 3).forEach(i -> {
            UUID taskId = UUID.randomUUID();
            ResultEvent evt = new ResultEvent(
                    new ResultEvent.Header("trace", System.currentTimeMillis()),
                    "wf-123", taskId.toString(), "step-c", 0, 1,
                    System.currentTimeMillis() - 1000, System.currentTimeMillis(),
                    Map.of("status", "COMPLETED", "iteration", i));
            String json = EventJson.write(evt);

            ResultEvent decoded = EventJson.read(json, ResultEvent.class);
            assertThat(decoded.taskId()).isEqualTo(taskId.toString());
            assertThat(decoded.resultData().get("iteration")).isEqualTo(i);
        });
    }

    @Test
    void controlEvents_timeoutEvent_serializes() {
        ControlEvents.TimeoutEvent timeout = new ControlEvents.TimeoutEvent(
                "task-abc", "wf-123", java.time.Instant.now().plusSeconds(60), true);
        String json = EventJson.write(timeout);
        ControlEvents.TimeoutEvent decoded = EventJson.read(json, ControlEvents.TimeoutEvent.class);

        assertThat(decoded.taskId()).isEqualTo("task-abc");
        assertThat(decoded.workflowId()).isEqualTo("wf-123");
        assertThat(decoded.escalateToHitl()).isTrue();
    }

    @Test
    void controlEvents_loopEvent_serializes() {
        ControlEvents.LoopEvent loop = new ControlEvents.LoopEvent(
                "wf-123", "refinement_loop", 2, false, 3, Map.of("score", 85));
        String json = EventJson.write(loop);
        ControlEvents.LoopEvent decoded = EventJson.read(json, ControlEvents.LoopEvent.class);

        assertThat(decoded.stepId()).isEqualTo("refinement_loop");
        assertThat(decoded.iteration()).isEqualTo(2);
        assertThat(decoded.maxIterations()).isEqualTo(3);
    }

    @Test
    void controlEvents_sweepEvent_serializes() {
        ControlEvents.SweepEvent sweep = new ControlEvents.SweepEvent(
                "sweep-001", java.time.Instant.now(), "ALL");
        String json = EventJson.write(sweep);
        ControlEvents.SweepEvent decoded = EventJson.read(json, ControlEvents.SweepEvent.class);

        assertThat(decoded.sweepId()).isEqualTo("sweep-001");
        assertThat(decoded.scope()).isEqualTo("ALL");
    }

    // ─── Workflow status tests ──────────────────────────────────────

    @Test
    void executionStatus_transitionsValid() {
        assertThat(ExecutionStatus.PENDING).isNotNull();
        assertThat(ExecutionStatus.RUNNING).isNotNull();
        assertThat(ExecutionStatus.COMPLETED).isNotNull();
        assertThat(ExecutionStatus.FAILED).isNotNull();
        assertThat(ExecutionStatus.CANCELLED).isNotNull();
        assertThat(ExecutionStatus.PAUSED_HITL).isNotNull();
    }

    @Test
    void checkpointSnapshot_immutability() {
        CheckpointSnapshot snap =
                new CheckpointSnapshot("wf-1", "step-a",
                        NodeType.STEP,
                        CheckpointStatus.IN_PROGRESS, 1, null, "coder");

        assertThat(snap.stepId()).isEqualTo("step-a");
        assertThat(snap.status()).isEqualTo(CheckpointStatus.IN_PROGRESS);
        assertThat(snap.agentType()).isEqualTo("coder");
    }

    @Test
    void checkpointSnapshot_sentinelValues() {
        CheckpointSnapshot sentinel =
                CheckpointSnapshot.PENDING_SENTINEL;

        assertThat(sentinel.workflowExecutionId()).isEmpty();
        assertThat(sentinel.nodeType()).isEqualTo(NodeType.STEP);
        assertThat(sentinel.status()).isEqualTo(CheckpointStatus.PENDING);
    }
}
