package de.augmentia.quad.worker.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import de.augmentia.quad.worker.agent.TaskExecutor;
import de.augmentia.quad.worker.agent.TaskPromptBuilder;
import de.augmentia.quad.worker.agent.WorkerTestFixtures;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.worker.contract.WorkerJson;
import de.augmentia.quad.worker.metrics.WorkerMetrics;
import de.augmentia.quad.worker.result.ResultMapper;
import de.augmentia.quad.worker.session.TaskSessionContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Level 4 — Kafka topic deep-dive (real broker, no Spring context):
 *
 *   Orchestrator-Seite (Test-Producer)
 *     → agent.tasks.researcher
 *     → WorkflowTaskListener.onTasks (real Kafka MessageListenerContainer,
 *       batch + MANUAL ack like KafkaConfig.quadBatchListenerFactory)
 *     → TaskExecutor (volle Pipeline: Prompt, Session, AgentFactory+MockChatModel,
 *       ResultMapper)
 *     → WorkflowResultPublisher auf agent.execution.results
 *
 * A second scenario verifies the DLQ path: malformed JSON on the task topic
 * lands via WorkflowDlqPublisher on agent.execution.dlq — exactly the
 * item 2 topic chain (agent.tasks.* → agent.execution.results / *.dlq).
 *
 * Broker: spring-kafka-test EmbeddedKafkaBroker (in-JVM, KRaft) — offline-capable,
 * no Docker dependency. Producer/consumer are raw Kafka clients that
 * run through serialisation + topic names exactly like production.
 */
class KafkaTopicE2ETest {

    static final String TASK_TOPIC = "agent.tasks.researcher";
    static final String RESULT_TOPIC = "agent.execution.results";
    static final String DLQ_TOPIC = "agent.execution.dlq";

    static EmbeddedKafkaBroker broker;

    private KafkaProducer<String, String> workerProducer;
    private KafkaMessageListenerContainer<String, String> workerContainer;

    @BeforeAll
    static void startBroker() {
        broker = new EmbeddedKafkaKraftBroker(1, 1, TASK_TOPIC, RESULT_TOPIC, DLQ_TOPIC);
        broker.afterPropertiesSet();
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.destroy();
        }
    }

    @AfterEach
    void stopWorker() {
        if (workerContainer != null) {
            workerContainer.stop();
        }
        if (workerProducer != null) {
            workerProducer.close(Duration.ofSeconds(2));
        }
    }

    // ── Szenario 1: Task auf agent.tasks.researcher → Result auf agent.execution.results ──

    @Test
    void taskOnTopicYieldsResultOnResultTopic() throws Exception {
        startWorkerListener();

        // Orchestrator side: send the TaskEvent contract via a real Kafka topic.
        try (var taskProducer = new KafkaProducer<>(producerProps())) {
            TaskEvent task = WorkerTestFixtures.createTask("tx-kafka-1", "Analyze the quarterly report");
            taskProducer.send(new ProducerRecord<>(
                TASK_TOPIC, task.header().workflowId(), WorkerJson.write(task)));
            taskProducer.flush();
        }

        // worker side: consume the result of the task on the result topic (test consumer).
        ResultEvent result = awaitResult("tx-kafka-1");
        assertThat(result.taskId()).isEqualTo("tx-kafka-1");
        assertThat(result.resultData().get("agentType")).isEqualTo("researcher");
        assertThat(result.resultData()).containsKey("response");
        assertThat(result.resultData().get("status")).isEqualTo("COMPLETED");
    }

    // ── Szenario 2: malformed JSON → DLQ (agent.execution.dlq) statt Result ──

    @Test
    void malformedTaskGoesToDlqTopic() throws Exception {
        startWorkerListener();

        try (var taskProducer = new KafkaProducer<>(producerProps())) {
            taskProducer.send(new ProducerRecord<>(
                TASK_TOPIC, "wf-e2e", "{ not valid json ,,, }"));
            taskProducer.flush();
        }

        JsonNode dlqEntry = awaitDlqEntry();
        assertThat(dlqEntry.get("original_topic").asText()).isEqualTo(TASK_TOPIC);
        assertThat(dlqEntry.get("error_type").asText()).contains("IllegalArgument");
    }

    // ── Worker-Pipeline: echter Listener + echte TaskExecutor-Kette ──

    /** Starts the real WorkflowTaskListener via a real Kafka container
     *  (batch + MANUAL ack, identisch zu KafkaConfig.quadBatchListenerFactory). */
    private void startWorkerListener() throws Exception {
        workerProducer = new KafkaProducer<>(producerProps());
        TaskExecutor executor = new TaskExecutor(
            WorkerTestFixtures.factoryWith(WorkerTestFixtures.registry()),
            new TaskPromptBuilder(),
            new TaskSessionContext("/tmp/quad-worker/workspace-e2e"),
            new ResultMapper(),
            new WorkflowResultPublisher(workerProducer, RESULT_TOPIC),
            new WorkflowDlqPublisher(workerProducer, DLQ_TOPIC),
            new WorkerMetrics(new SimpleMeterRegistry()),
            8,
            600_000L,
            WorkerTestFixtures.virtualExecutor()
        );
        WorkflowTaskListener listener = new WorkflowTaskListener(executor);

        var consumerFactory = consumerFactory();
        var containerProps = new ContainerProperties(TASK_TOPIC);
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProps.setMessageListener(
            (BatchAcknowledgingMessageListener<String, String>) listener::onTasks);

        workerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        workerContainer.start();
        // wait until the partition assignment is in place so the task is not missed.
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline
            && workerContainer.getAssignedPartitions().isEmpty()) {
            Thread.sleep(50);
        }
        assertThat(workerContainer.getAssignedPartitions()).isNotEmpty();
    }

    private org.springframework.kafka.core.ConsumerFactory<String, String> consumerFactory() {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "quad-workflow-worker");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new org.springframework.kafka.core.DefaultKafkaConsumerFactory<>(props);
    }

    private Map<String, Object> producerProps() {
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    /** Polls the result topic until a ResultEvent with a matching taskId is present. */
    private ResultEvent awaitResult(String expectedTaskId) throws Exception {
        AtomicReference<ResultEvent> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicBoolean done = new AtomicBoolean();
        consumeEvents(RESULT_TOPIC, record -> {
            if (done.get()) return;
            try {
                ResultEvent event = WorkerJson.mapper().readValue(record.value(), ResultEvent.class);
                if (expectedTaskId.equals(event.taskId())) {
                    result.set(event);
                    done.set(true);
                }
            } catch (Exception ex) {
                failure.set(ex);
            }
        }, done);
        if (result.get() == null) {
            fail("Timeout waiting for result " + expectedTaskId + " on " + RESULT_TOPIC
                + (failure.get() != null ? " (last parse error: " + failure.get().getMessage() + ")" : ""));
        }
        return result.get();
    }

    /** Polls the DLQ topic until a DLQ entry is present. */
    private JsonNode awaitDlqEntry() throws Exception {
        AtomicReference<JsonNode> result = new AtomicReference<>();
        AtomicBoolean done = new AtomicBoolean();
        consumeEvents(DLQ_TOPIC, record -> {
            if (done.get()) return;
            try {
                result.set(WorkerJson.mapper().readValue(record.value(), JsonNode.class));
                done.set(true);
            } catch (Exception ignored) {
            }
        }, done);
        if (result.get() == null) {
            fail("Timeout waiting for DLQ entry on " + DLQ_TOPIC);
        }
        return result.get();
    }

    private void consumeEvents(String topic, java.util.function.Consumer<ConsumerRecord<String, String>> handler,
                               AtomicBoolean done) throws Exception {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
            "test-reader-" + topic + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline && !done.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                records.forEach(handler);
            }
        }
    }
}