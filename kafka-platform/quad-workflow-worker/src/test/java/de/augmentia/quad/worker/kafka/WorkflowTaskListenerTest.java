package de.augmentia.quad.worker.kafka;

import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.worker.agent.TaskExecutor;
import de.augmentia.quad.worker.agent.TaskPromptBuilder;
import de.augmentia.quad.worker.agent.WorkerTestFixtures;
import de.augmentia.quad.worker.config.ChatModelRegistry;
import de.augmentia.quad.worker.config.WorkerProperties;
import de.augmentia.quad.worker.metrics.WorkerMetrics;
import de.augmentia.quad.worker.result.ResultMapper;
import de.augmentia.quad.worker.session.TaskSessionContext;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Level 2 — Kafka listener behaviour of the quad worker.
 *
 * <p>This class covers the gap the other E2E tests do not close:
 * they call {@code TaskExecutor.safeExecute} directly and thereby bypass exactly
 * the path that in production lies between Kafka and the agents — the
 * session binding across threads and the acknowledgement semantics of the batch.
 *
 * <p>Regression tests that originated here:
 * <ul>
 *   <li><b>K1</b> — {@code CurrentSession} was a ThreadLocal but was bound on the
 *       bound to the calling thread, while the ReAct loop ran on another
 *       thread. The tool therefore always received {@code null}.</li>
 *   <li><b>K4</b> — the batch was acknowledged completely with {@code acknowledge()},
 *       which meant a failed record was lost for good. What must be acknowledged
 *       is the contiguous success prefix.</li>
 * </ul>
 */
class WorkflowTaskListenerTest {

    private static final String TOOL_NAME = "probeSession";

    // ── Instrumentation: observes thread and session from within the tool execution ──

    private static final AtomicReference<String> OBSERVED_SESSION = new AtomicReference<>("not-run");
    private static final AtomicReference<String> OBSERVED_THREAD_KIND = new AtomicReference<>("not-run");
    private static final AtomicReference<String> CALLER_THREAD_KIND = new AtomicReference<>("not-run");

    /**
     * Tool that checks exactly what the agent needs in production: a
     * bound {@code CurrentSession} on the thread that runs the ReAct loop.
     * If this tool returns {@code NULL}, the session is not bound.
     */
    public static class SessionProbeTool {
        @Tool("Returns the id of the current agent session, or NULL if none is bound.")
        public String probeSession() {
            AgentSessionState state = CurrentSession.getCurrent();
            OBSERVED_SESSION.set(state == null ? "NULL" : state.getSessionId());
            OBSERVED_THREAD_KIND.set(threadKind(Thread.currentThread()));
            return "session=" + OBSERVED_SESSION.get();
        }
    }

    private static String threadKind(Thread t) {
        if (t.isVirtual()) return "virtual";
        return t.getName().startsWith("ForkJoinPool") ? "forkjoin" : "other";
    }

    // ── 1) K1: the session is bound on the executing thread ──

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void sessionIsVisibleInsideToolExecutionOnTheAsyncThread() throws Exception {
        OBSERVED_SESSION.set("not-run");
        OBSERVED_THREAD_KIND.set("not-run");

        var registry = new ChatModelRegistry();
        registry.register("simple", new ToolAwareMock());

        var toolFactory = WorkerTestFixtures.toolFactoryWith(Set.of(new SessionProbeTool()));
        var props = WorkerTestFixtures.baseProperties(java.util.Map.of("researcher",
                new WorkerProperties.AgentConfig(
                        "researcher", "simple", 0.2, 4, null,
                        Set.of(TOOL_NAME), false,
                        "You are a researcher. Always call " + TOOL_NAME + " first, then report its result "
                                + "as JSON with response, confidence and status.",
                        null, null, false)));

        var factory = WorkerTestFixtures.factoryWith(registry, toolFactory, props);
        var results = mock(WorkflowResultPublisher.class);

        var executor = new TaskExecutor(
                factory,
                new TaskPromptBuilder(),
                new TaskSessionContext("/tmp/quad-worker/workspace-listener"),
                new ResultMapper(),
                results,
                mock(WorkflowDlqPublisher.class),
                new WorkerMetrics(new SimpleMeterRegistry()),
                8,
                600_000L,
                WorkerTestFixtures.virtualExecutor());

        var listener = new WorkflowTaskListener(executor);
        RecordingAck ack = new RecordingAck();

        CALLER_THREAD_KIND.set(threadKind(Thread.currentThread()));
        TaskEvent task = WorkerTestFixtures.createTask("session-probe", "Probe the session.");
        listener.onTasks(List.of(recordFor(task, "session-probe-0")), ack);

        // the core of the regression statement: the tool saw a real session.
        assertThat(OBSERVED_SESSION.get())
                .as("CurrentSession must be bound on the executing thread")
                .isNotEqualTo("NULL")
                .isNotEqualTo("not-run");

        // and it did not run on the caller's thread, but on the executor thread.
        assertThat(OBSERVED_THREAD_KIND.get())
                .as("Tool execution must run on the executor thread")
                .isEqualTo("virtual");

        // delivered successfully -> acknowledge.
        assertThat(ack.fullAcks.get()).isEqualTo(1);
        assertThat(ack.indexAcks).isEmpty();
    }

    // ── 2) K4: only the success prefix is acknowledged ──

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void onlyTheSuccessfulPrefixIsAcknowledgedWhenARecordFails() {
        // result publish and DLQ publish fail -> safeExecute yields false.
        var results = mock(WorkflowResultPublisher.class);
        var dlq = mock(WorkflowDlqPublisher.class);
        doThrow(new RuntimeException("kafka down")).when(results).send(anyString(), any());
        doThrow(new RuntimeException("kafka down")).when(dlq).send(any(), any(Exception.class));

        var executor = new TaskExecutor(
                WorkerTestFixtures.factoryWith(new ChatModelRegistry()),
                new TaskPromptBuilder(),
                new TaskSessionContext("/tmp/quad-worker/workspace-listener"),
                new ResultMapper(),
                results,
                dlq,
                new WorkerMetrics(new SimpleMeterRegistry()),
                8,
                600_000L,
                WorkerTestFixtures.virtualExecutor());

        var listener = new WorkflowTaskListener(executor);
        RecordingAck ack = new RecordingAck();

        List<ConsumerRecord<String, String>> batch = List.of(
                recordFor(WorkerTestFixtures.createTask("t-0", "a"), "t-0"),
                recordFor(WorkerTestFixtures.createTask("t-1", "b"), "t-1"),
                recordFor(WorkerTestFixtures.createTask("t-2", "c"), "t-2"));

        listener.onTasks(batch, ack);

        // the first record does not fail on the unknown agentType "simple" —
        // instead it fails on the broken producer. Nothing is acknowledged
        // completely, and no index beyond the error is acknowledged.
        assertThat(ack.fullAcks.get())
                .as("With a failed publish the batch must not be acknowledged completely")
                .isZero();

        for (int index : ack.indexAcks) {
            assertThat(index)
                    .as("The acknowledged index must not include a failed record")
                    .isLessThan(0);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void malformedRecordIsAcknowledgedAfterReachingTheDlq() {
        var results = mock(WorkflowResultPublisher.class);
        var dlq = mock(WorkflowDlqPublisher.class);
        var metrics = mock(WorkerMetrics.class);

        var executor = new TaskExecutor(
                WorkerTestFixtures.factoryWith(new ChatModelRegistry()),
                new TaskPromptBuilder(),
                new TaskSessionContext("/tmp/quad-worker/workspace-listener"),
                new ResultMapper(),
                results,
                dlq,
                metrics,
                8,
                600_000L,
                WorkerTestFixtures.virtualExecutor());

        var listener = new WorkflowTaskListener(executor);
        RecordingAck ack = new RecordingAck();

        listener.onTasks(List.of(
                new ConsumerRecord<>("agent.tasks.researcher", 0, 0L, "k", "{ kaputt ,,, }")),
                ack);

        // The DLQ publish was successful -> the record is delivered -> acknowledging is correct.
        assertThat(ack.fullAcks.get()).isEqualTo(1);
    }

    @Test
    void emptyBatchAcknowledgesNothing() {
        var executor = mock(TaskExecutor.class);
        var listener = new WorkflowTaskListener(executor);
        RecordingAck ack = new RecordingAck();

        listener.onTasks(List.of(), ack);

        assertThat(ack.fullAcks.get()).isZero();
        assertThat(ack.indexAcks).isEmpty();
    }

    // ── Hilfen ──

    private static ConsumerRecord<String, String> recordFor(TaskEvent task, String key) {
        return new ConsumerRecord<>("agent.tasks.researcher", 0, 0L, key,
                de.augmentia.quad.worker.contract.WorkerJson.write(task));
    }

    /** Counts acknowledgements instead of binding them to a real consumer. */
    private static final class RecordingAck implements Acknowledgment {
        final AtomicInteger fullAcks = new AtomicInteger();
        final List<Integer> indexAcks = new CopyOnWriteArrayList<>();

        @Override
        public void acknowledge() {
            fullAcks.incrementAndGet();
        }

        @Override
        public void acknowledge(int index) {
            indexAcks.add(index);
        }
    }

    /** Mock LLM: the first pass requests the probe tool, the second uses its result. */
    private static final class ToolAwareMock implements ChatModel {
        @Override
        public ChatResponse chat(ChatRequest request) {
            boolean hasToolResult = request.messages().stream()
                    .anyMatch(m -> m instanceof ToolExecutionResultMessage);

            if (!hasToolResult) {
                ToolExecutionRequest req = ToolExecutionRequest.builder()
                        .id("probe-1")
                        .name(TOOL_NAME)
                        .arguments("{}")
                        .build();
                return ChatResponse.builder()
                        .aiMessage(AiMessage.from("Probing.", List.of(req)))
                        .build();
            }

            String toolResult = request.messages().stream()
                    .filter(m -> m instanceof ToolExecutionResultMessage)
                    .map(m -> ((ToolExecutionResultMessage) m).text())
                    .findFirst().orElse("(no tool result)");

            return ChatResponse.builder()
                    .aiMessage(AiMessage.from(
                            "{\"response\": \"" + toolResult + "\", \"confidence\": 0.9, \"status\": \"COMPLETED\"}"))
                    .build();
        }
    }
}
