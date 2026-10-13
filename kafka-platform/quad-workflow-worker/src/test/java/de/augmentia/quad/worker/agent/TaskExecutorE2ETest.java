package de.augmentia.quad.worker.agent;

import de.augmentia.quad.worker.config.ChatModelRegistry;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.worker.contract.WorkerJson;
import de.augmentia.quad.worker.kafka.WorkflowDlqPublisher;
import de.augmentia.quad.worker.kafka.WorkflowResultPublisher;
import de.augmentia.quad.worker.metrics.WorkerMetrics;
import de.augmentia.quad.worker.result.ResultMapper;
import de.augmentia.quad.worker.session.TaskSessionContext;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Level 1 E2E test: {@link TaskExecutor} — the full processing path of a
 * task without Kafka/DB: Kafka message → parser/validation → agent with a real
 * LLM call → result mapping → publishing + timeout/FAILED classification.
 *
 * Publisher and metrics are Mockito mocks; the actual task pipeline
 * (TaskPromptBuilder, TaskSessionContext, WorkerAgentFactory, ResultMapper)
 * is run through for real. The LLM is key-gated as in {@link WorkerTestFixtures}.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class TaskExecutorE2ETest {

    @Test
    void happyPathPublishesCompletedResult() {
        var publisher = mock(WorkflowResultPublisher.class);
        var dlq = mock(WorkflowDlqPublisher.class);
        var metrics = mock(WorkerMetrics.class);

        TaskExecutor executor = newExecutor(publisher, dlq, metrics,
            WorkerTestFixtures.registry());

        TaskEvent task = WorkerTestFixtures.createTask("tx-happy", "Summarize the meeting notes");
        ConsumerRecord<String, String> record = recordFor(task);

        executor.safeExecute(record);

        // the result was published, the DLQ was NOT.
        ArgumentCaptor<ResultEvent> resultCaptor = ArgumentCaptor.forClass(ResultEvent.class);
        verify(publisher).send(eq("wf-e2e"), resultCaptor.capture());
        verify(dlq, never()).send(any(), any());

        ResultEvent result = resultCaptor.getValue();
        assertThat(result.taskId()).isEqualTo("tx-happy");
        assertThat(result.resultData().get("agentType")).isEqualTo("researcher");
        assertThat(result.resultData()).containsKey("response");
        assertThat(result.resultData().get("status")).isEqualTo("COMPLETED");

        verify(metrics).recordTaskReceived();
        verify(metrics).recordTaskCompleted(eq("researcher"), eq("completed"));
    }

    @Test
    void taskTimeoutMapsToRetryableFailure() {
        var publisher = mock(WorkflowResultPublisher.class);
        var dlq = mock(WorkflowDlqPublisher.class);
        var metrics = mock(WorkerMetrics.class);

        // Slow LLM: sleeps so that the task timeout takes effect (deterministically).
        ChatModel slowModel = new ChatModel() {
            @Override
            public ChatResponse chat(ChatRequest request) {
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return null;
            }
        };

        TaskExecutor executor = newExecutor(publisher, dlq, metrics,
            WorkerTestFixtures.registryWith(slowModel));

        // timeout very short (10ms) → the slow LLM runs into the timeout.
        TaskEvent task = WorkerTestFixtures.createTask("tx-timeout", "Slow task", 10L);
        ConsumerRecord<String, String> record = recordFor(task);

        executor.safeExecute(record);

        ArgumentCaptor<ResultEvent> resultCaptor = ArgumentCaptor.forClass(ResultEvent.class);
        verify(publisher).send(eq("wf-e2e"), resultCaptor.capture());

        ResultEvent result = resultCaptor.getValue();
        assertThat(result.resultData().get("status")).isEqualTo("FAILED_RETRYABLE");
        verify(metrics).recordTaskFailed(eq("researcher"), eq("FAILED_RETRYABLE"));
    }

    @Test
    void unknownAgentTypePublishesFailureWithoutDlq() throws Exception {
        var publisher = mock(WorkflowResultPublisher.class);
        var dlq = mock(WorkflowDlqPublisher.class);
        var metrics = mock(WorkerMetrics.class);

        TaskExecutor executor = newExecutor(publisher, dlq, metrics,
            WorkerTestFixtures.registry());

        TaskEvent task = new TaskEvent(
            new TaskEvent.Header("wf-e2e", "tx-unknown", null, 0, System.currentTimeMillis()),
            "nonexistent-agent",
            "step-1",
            0,
            "Do something",
            null,
            java.util.List.of(),
            120_000L
        );
        ConsumerRecord<String, String> record = recordFor(task);

        executor.safeExecute(record);

        ArgumentCaptor<ResultEvent> resultCaptor = ArgumentCaptor.forClass(ResultEvent.class);
        verify(publisher).send(eq("wf-e2e"), resultCaptor.capture());

        ResultEvent result = resultCaptor.getValue();
        assertThat(result.resultData().get("status")).isIn("FAILED_RETRYABLE", "FAILED_FATAL");
        assertThat(result.resultData()).containsKey("errorMessage");
        verify(dlq, never()).send(any(), any());
    }

    @Test
    void malformedJsonGoesToDlq() {
        var publisher = mock(WorkflowResultPublisher.class);
        var dlq = mock(WorkflowDlqPublisher.class);
        var metrics = mock(WorkerMetrics.class);

        TaskExecutor executor = newExecutor(publisher, dlq, metrics,
            WorkerTestFixtures.registry());

        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("agent.tasks.researcher", 0, 0L, "wf-e2e",
                "{ not valid json ,,, }");

        executor.safeExecute(record);

        verify(dlq).send(eq(record), any());
        verify(publisher, never()).send(any(), any());
        verify(metrics).recordDlqSent();
    }

    @Test
    void taskTimeoutFromTheWorkflowIsCappedAtTheWorkerLimit() {
        var publisher = mock(WorkflowResultPublisher.class);
        var dlq = mock(WorkflowDlqPublisher.class);
        var metrics = mock(WorkerMetrics.class);

        ChatModel slowModel = new ChatModel() {
            @Override
            public ChatResponse chat(ChatRequest request) {
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return null;
            }
        };

        // The workflow asks for an hour; the worker allows 50 ms.
        TaskExecutor executor = newExecutor(publisher, dlq, metrics,
            WorkerTestFixtures.registryWith(slowModel), 50L);

        long began = System.currentTimeMillis();
        executor.safeExecute(recordFor(WorkerTestFixtures.createTask("tx-long", "Long task", 3_600_000L)));
        long elapsed = System.currentTimeMillis() - began;

        ArgumentCaptor<ResultEvent> resultCaptor = ArgumentCaptor.forClass(ResultEvent.class);
        verify(publisher).send(eq("wf-e2e"), resultCaptor.capture());

        // Honoured verbatim, the record would have occupied the poll thread for five seconds
        // — with a realistic timeout far beyond max.poll.interval.ms, which evicts the
        // consumer and replays the whole partition, paying for the LLM call twice.
        assertThat(resultCaptor.getValue().resultData().get("status")).isEqualTo("FAILED_RETRYABLE");
        assertThat(elapsed).isLessThan(3_000L);
    }

    // ── Helpers ──

    private TaskExecutor newExecutor(WorkflowResultPublisher publisher, WorkflowDlqPublisher dlq,
                                     WorkerMetrics metrics, ChatModelRegistry registry) {
        return newExecutor(publisher, dlq, metrics, registry, 600_000L);
    }

    private TaskExecutor newExecutor(WorkflowResultPublisher publisher, WorkflowDlqPublisher dlq,
                                     WorkerMetrics metrics, ChatModelRegistry registry,
                                     long maxTaskTimeoutMs) {
        var factory = WorkerTestFixtures.factoryWith(registry);
        var sessionCtx = new TaskSessionContext("/tmp/quad-worker/workspace-e2e");
        var prompt = new TaskPromptBuilder();
        var resultMapper = new ResultMapper();
        return new TaskExecutor(factory, prompt, sessionCtx, resultMapper, publisher, dlq, metrics, 8,
            maxTaskTimeoutMs, WorkerTestFixtures.virtualExecutor());
    }

    private static ConsumerRecord<String, String> recordFor(TaskEvent task) {
        return new ConsumerRecord<>("agent.tasks.researcher", 0, 0L, "wf-e2e", WorkerJson.write(task));
    }
}