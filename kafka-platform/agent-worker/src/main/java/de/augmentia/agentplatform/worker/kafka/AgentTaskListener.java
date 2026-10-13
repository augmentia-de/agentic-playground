package de.augmentia.agentplatform.worker.kafka;

import de.augmentia.agentplatform.common.agent.AgentExecutionResult;
import de.augmentia.agentplatform.common.agent.AgentExecutor;
import de.augmentia.agentplatform.common.event.AgentResultEvent;
import de.augmentia.agentplatform.common.event.AgentTaskEvent;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.ResultStatus;
import de.augmentia.agentplatform.worker.agent.AgentExecutorRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;

@Component
public class AgentTaskListener {

    private static final Logger log = LoggerFactory.getLogger(AgentTaskListener.class);

    private final AgentExecutorRegistry registry;
    private final WorkerEventPublisher publisher;
    private final Clock clock;

    public AgentTaskListener(AgentExecutorRegistry registry, WorkerEventPublisher publisher, Clock clock) {
        this.registry = registry;
        this.publisher = publisher;
        this.clock = clock;
    }

    @KafkaListener(
            topicPattern = "${app.worker.task-topic-pattern:agent\\.tasks\\..*}",
            groupId = "${app.worker.group-id:agent-worker-group}"
    )
    public void onTask(ConsumerRecord<String, String> record) {
        AgentTaskEvent task;
        try {
            task = EventJson.read(record.value(), AgentTaskEvent.class);
        } catch (RuntimeException ex) {
            log.error("Unable to parse agent task on topic {}", record.topic(), ex);
            publishDlq(record, ex);
            return;
        }

        AgentExecutionResult result;
        try {
            AgentExecutor executor = registry.executorFor(task.agentConfig().agentType());
            result = executor.execute(task);
        } catch (RuntimeException ex) {
            log.error("Agent execution failed workflowId={} taskId={}", task.header().workflowId(), task.header().taskId(), ex);
            result = new AgentExecutionResult(
                    ResultStatus.FAILED_FATAL,
                    Map.of(),
                    ex.getMessage(),
                    stackTrace(ex),
                    0,
                    0,
                    0.0d
            );
        }

        AgentResultEvent event = toResultEvent(task, result);
        publisher.publish(TopicNames.RESULTS, task.header().workflowId(), EventJson.write(event));
    }

    private AgentResultEvent toResultEvent(AgentTaskEvent task, AgentExecutionResult result) {
        return new AgentResultEvent(
                new AgentResultEvent.ResultHeader(
                        task.header().workflowId(),
                        task.header().taskId(),
                        task.agentConfig().agentType(),
                        result.status(),
                        clock.instant(),
                        0L
                ),
                new AgentResultEvent.ResultOutput(
                        result.resultData() == null ? Map.of() : result.resultData(),
                        result.errorMessage(),
                        result.stackTrace()
                ),
                new AgentResultEvent.Usage(
                        result.promptTokens(),
                        result.completionTokens(),
                        result.totalCostUsd()
                )
        );
    }

    private void publishDlq(ConsumerRecord<String, String> record, RuntimeException ex) {
        Map<String, Object> payload = Map.of(
                "originalTopic", record.topic(),
                "originalKey", record.key() == null ? "" : record.key(),
                "payload", record.value() == null ? "" : record.value(),
                "errorClass", ex.getClass().getName(),
                "errorMessage", ex.getMessage() == null ? "" : ex.getMessage(),
                "occurredAt", Instant.now(clock).toString()
        );
        publisher.publish(TopicNames.DLQ, record.key(), EventJson.write(payload));
    }

    private static String stackTrace(Throwable throwable) {
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }
}
