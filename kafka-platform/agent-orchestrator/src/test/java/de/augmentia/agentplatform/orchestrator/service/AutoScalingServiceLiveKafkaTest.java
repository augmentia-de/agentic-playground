package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.metrics.OrchestratorMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The auto-scaler against a real broker.
 *
 * <p>The other tests check the computation, the property preparation and a replicated
 * broker interaction. None of them would have found the production bug: it consisted in
 * building a {@code KafkaConsumer} from the factory properties — and those carry no group.
 * That only shows up when group administration actually takes place, that is, only against
 * a real broker.
 *
 * <p>The own topic with its own name is deliberate: the test must touch neither the
 * platform's topics nor the {@code orchestrator-dag-group} group. A test that writes into
 * foreign state is the worst thing in a parallel CI.
 *
 * <p>Without a broker the class is skipped, not failed — the same rule as for
 * {@code LlmDrivenWorkflowE2ETest} and the integration suite.
 */
@DisplayName("AutoScalingService — against real Kafka")
class AutoScalingServiceLiveKafkaTest {

    private static final String BOOTSTRAP =
            System.getProperty("it.kafka.bootstrap", "localhost:9092");
    private static final int PARTITIONS = 3;

    private static String topic;
    private static String group;
    private static final List<String> createdTopics = new java.util.ArrayList<>();

    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private OrchestratorMetrics metrics;
    private AutoScalingService service;

    private static boolean brokerUp() {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.listTopics().names().get();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static OrchestratorProperties.AutoScaling cfg() {
        return new OrchestratorProperties.AutoScaling(true, 30, 500, 2, 20, topic, group);
    }

    /** ConsumerFactory properties as in production: without group.id. */
    private static Map<String, Object> consumerFactoryProps() {
        var p = new HashMap<String, Object>();
        p.put("bootstrap.servers", List.of(BOOTSTRAP));
        p.put("key.deserializer", StringDeserializer.class);
        p.put("value.deserializer", StringDeserializer.class);
        return p;
    }

    private static Properties producerProps() {
        var p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", StringSerializer.class);
        p.put("value.serializer", StringSerializer.class);
        return p;
    }

    /** Exactly {@code perPartition} messages per partition, so that the expectation is exact. */
    private static void produce(int perPartition) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            for (int partition = 0; partition < PARTITIONS; partition++) {
                for (int i = 0; i < perPartition; i++) {
                    producer.send(new ProducerRecord<>(topic, partition, "k" + i, "v" + i)).get();
                }
            }
        }
    }

    private static void commitOffsets(long offsetPerPartition) throws Exception {
        var props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("group.id", group);
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        try (var raw = new KafkaConsumer<String, String>(props)) {
            var tps = new HashMap<TopicPartition, OffsetAndMetadata>();
            for (int p = 0; p < PARTITIONS; p++) {
                tps.put(new TopicPartition(topic, p), new OffsetAndMetadata(offsetPerPartition));
            }
            ((Consumer<String, String>) raw).commitSync(tps);
        }
    }

    @BeforeAll
    static void requireBroker() {
        assumeThat(brokerUp())
                .as("Broker auf %s noetig — uebersprungen", BOOTSTRAP)
                .isTrue();
    }

    /**
     * Own topic and own group per test, not once for the class: otherwise
     * the messages accumulate across the tests, and the order is not
     * guaranteed. The expectations in the tests start from an empty base.
     */
    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        var suffix = UUID.randomUUID().toString().substring(0, 8);
        topic = "it-autoscaling-" + suffix;
        group = "it-autoscaling-" + suffix;
        createdTopics.add(topic);

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
        }

        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenReturn(Boolean.TRUE);
        metrics = mock(OrchestratorMetrics.class);

        service = new AutoScalingService(
                new OrchestratorProperties(new OrchestratorProperties.Kafka(List.of()),
                        null, null, null, cfg(), null, null,
                null),
                new DefaultKafkaConsumerFactory<String, String>(consumerFactoryProps()), redis, metrics);
    }

    @AfterAll
    static void removeTopics() {
        if (createdTopics.isEmpty() || !brokerUp()) {
            return;
        }
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.deleteTopics(createdTopics).all().get();
        } catch (Exception e) {
            // Cleanup must not make the run fail.
        }
    }

    @Test
    @DisplayName("without commits by the group everything in the topic counts as backlog")
    void uncommittedGroupSeesWholeTopic() throws Exception {
        produce(2);

        var lag = service.computeTotalLag(
                AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP)), topic, group);

        assertThat(lag)
                .as("the group never committed — everything is backlog")
                .hasSize(PARTITIONS)
                .allSatisfy((partition, value) ->
                        assertThat(value).as("Partition %s", partition).isEqualTo(2L));
    }

    @Test
    @DisplayName("committed messages are subtracted")
    void committedOffsetsAreSubtracted() throws Exception {
        produce(3);
        commitOffsets(2);

        var lag = service.computeTotalLag(
                AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP)), topic, group);

        assertThat(lag)
                .as("3 je Partition, 2 committet — 1 bleibt")
                .allSatisfy((partition, value) ->
                        assertThat(value).as("Partition %s", partition).isEqualTo(1L));
    }

    @Test
    @DisplayName("a fully consumed topic yields 0 and no scale-up")
    void fullyConsumedTopicYieldsZeroLag() throws Exception {
        produce(2);
        commitOffsets(2);
        when(values.get("autoscaling:target")).thenReturn("2");

        service.evaluateScale();

        verify(metrics).gaugeWorkerLoadScore(0.0);
        verify(metrics).gaugeWorkerScaleTarget(2);
        verify(values, never()).set(eq("autoscaling:target"), anyString(), any(Duration.class));
    }

    @Test
    @DisplayName("a real backlog above the threshold scales the workers up")
    void realLagScalesUp() throws Exception {
        // 3 partitions times 200 messages = 600 in total, cut 200 per partition.
        // The threshold is 500 — with 3 times 300 messages the cut is 300,
        // which is not enough, so the cut has to be above that. 4 partitions would
        // be needed, so here the threshold is set via the lagThreshold.
        produce(200);
        var cfg = new OrchestratorProperties.AutoScaling(
                true, 30, 100, 2, 20, topic, group);
        var tuned = new AutoScalingService(
                new OrchestratorProperties(new OrchestratorProperties.Kafka(List.of()),
                        null, null, null, cfg, null, null,
                null),
                new DefaultKafkaConsumerFactory<String, String>(consumerFactoryProps()),
                redis, metrics);
        when(values.get("autoscaling:target")).thenReturn("2");

        tuned.evaluateScale();

        // Schnitt 200 > Schwelle 100 -> hochfahren auf 3
        verify(metrics).gaugeWorkerScaleTarget(3);
        verify(values).set(eq("autoscaling:target"), eq("3"), any(Duration.class));
    }

    @Test
    @DisplayName("the evaluation does not throw when the topic exists")
    void evaluationNeverThrows() {
        assertThatCode(() -> service.evaluateScale()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("autoscaling switched off touches neither broker nor Redis")
    void disabledAutoscalingTouchesNothing() {
        var disabled = new AutoScalingService(
                new OrchestratorProperties(new OrchestratorProperties.Kafka(List.of()),
                        null, null, null,
                        new OrchestratorProperties.AutoScaling(
                                false, 30, 500, 2, 20, topic, group), null, null,
                null),
                new DefaultKafkaConsumerFactory<String, String>(consumerFactoryProps()),
                redis, metrics);

        disabled.evaluateScale();

        verify(redis, never()).opsForValue();
        verify(metrics, never()).gaugeWorkerScaleTarget(org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    @DisplayName("no target value in Redis means min-workers")
    void missingTargetInRedisFallsBackToMinWorkers() throws Exception {
        produce(2);
        commitOffsets(0);
        when(values.get("autoscaling:target")).thenReturn(null);

        service.evaluateScale();

        // Without a stored value the scaling starts at min-workers (2).
        verify(metrics).gaugeWorkerScaleTarget(2);
    }

    @Test
    @DisplayName("a broken value in Redis does not mean anything — min-workers is used")
    void unparsableTargetInRedisFallsBackToMinWorkers() throws Exception {
        produce(2);
        commitOffsets(0);
        when(values.get("autoscaling:target")).thenReturn("kein-zahl-wert");

        service.evaluateScale();

        verify(metrics).gaugeWorkerScaleTarget(2);
    }

    @Test
    @DisplayName("a topic that does not exist skips the scaling")
    void unknownTopicSkipsScaling() {
        var missing = new AutoScalingService(
                new OrchestratorProperties(new OrchestratorProperties.Kafka(List.of()),
                        null, null, null,
                        new OrchestratorProperties.AutoScaling(
                                true, 30, 500, 2, 20, "gibt-es-nicht-" + UUID.randomUUID(), group),
                        null, null,
                null),
                new DefaultKafkaConsumerFactory<String, String>(consumerFactoryProps()),
                redis, metrics);

        // 0 partitions means "nothing to do" in evaluateScale — no error, no scale-up.
        missing.evaluateScale();

        verify(metrics, never()).gaugeWorkerScaleTarget(org.mockito.ArgumentMatchers.anyInt());
        verify(values, never()).set(eq("autoscaling:target"), anyString(), any(Duration.class));
    }

    @Test
    @DisplayName("Redis fails in the middle of the evaluation without the scheduler dying")
    void redisFailureDuringEvaluationIsCaught() throws Exception {
        produce(2);
        commitOffsets(0);
        when(values.get("autoscaling:target")).thenThrow(new IllegalStateException("redis gone"));

        // The scheduler must not die on a Redis outage — the evaluation
        // must swallow the error and run again on the next pass.
        assertThatCode(() -> service.evaluateScale()).doesNotThrowAnyException();

        // The lag measurement still ran, afterwards the target value query broke off.
        verify(metrics).gaugeWorkerLoadScore(2.0);
    }

    @Test
    @DisplayName("whoever fails before the lock also does not release a foreign lock")
    void failureBeforeLockDoesNotReleaseIt() throws Exception {
        produce(2);
        commitOffsets(0);
        when(values.get("autoscaling:target")).thenThrow(new IllegalStateException("redis gone"));

        service.evaluateScale();

        // getCurrentScaleTarget() is evaluated as the argument to evaluateWithLock and
        // throws before the lock was even acquired. Releasing here would be exactly the
        // original bug: an instance releases another's lock without ever having owned it.
        verify(redis, never()).delete("autoscaling:semaphore");
        verify(values, never()).setIfAbsent(anyString(), anyString(), any(Duration.class));
    }
}