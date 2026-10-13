package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.metrics.OrchestratorMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The broker interaction of the lag measurement — the part in which the production bug lived.
 *
 * <p>{@code AutoScalingServiceLagTest} checks the computation,
 * {@code AutoScalingServiceBootstrapTest} the property preparation. Between the two sits
 * {@code computeTotalLag}, which talks to the broker — and that was untested even though
 * that is where the bug happened: it built a {@code KafkaConsumer} without
 * {@code group.id}, was never happy with {@code InvalidGroupIdException}, and the evaluation
 * therefore never ran.
 *
 * <p>Here the AdminClient is replicated. The setup is a bit laborious because Kafka returns
 * result objects rather than plain values — in return you see exactly which request goes
 * out with which {@code OffsetSpec}, and what happens for a group that has never been
 * active.
 */
@DisplayName("AutoScalingService — lag at the broker")
class AutoScalingServiceBrokerTest {

    private static final String TOPIC = "agent.control.loop";
    private static final String GROUP = "orchestrator-dag-group";

    private AdminClient admin;
    private AutoScalingService service;

    private static Node node() {
        return new Node(0, "broker", 9092);
    }

    private static TopicPartitionInfo partition(int p) {
        return new TopicPartitionInfo(p, node(), List.of(node()), List.of(node()));
    }

    private static ListOffsetsResultInfo offset(long value) {
        return new ListOffsetsResultInfo(value, 0L, java.util.Optional.empty());
    }

    /**
     * KafkaFuture is abstract and has no failedFuture(). The get() is instead
     * forced to throw — that way the future behaves as in operation.
     */
    @SuppressWarnings("unchecked")
    private static <T> KafkaFuture<T> failedWith(Throwable t) {
        try {
            KafkaFuture<T> future = mock(KafkaFuture.class);
            when(future.get()).thenThrow(t);
            return future;
        } catch (InterruptedException | java.util.concurrent.ExecutionException e) {
            // get() declares both exceptions; Mockito demands the handling,
            // even though the test path neither waits nor is interrupted.
            throw new IllegalStateException(e);
        }
    }

    /** Broker delivers {@code count} partitions with the given end offsets. */
    @SuppressWarnings("unchecked")
    private void givenTopic(long... endOffsets) {
        var partitions = new ArrayList<TopicPartitionInfo>();
        var offsets = new HashMap<TopicPartition, ListOffsetsResultInfo>();
        for (int i = 0; i < endOffsets.length; i++) {
            partitions.add(partition(i));
            offsets.put(new TopicPartition(TOPIC, i), offset(endOffsets[i]));
        }

        var description = new org.apache.kafka.clients.admin.TopicDescription(
                TOPIC, false, partitions, java.util.Set.of());
        var describe = mock(DescribeTopicsResult.class);
        when(describe.allTopicNames())
                .thenReturn(KafkaFuture.completedFuture(Map.of(TOPIC, description)));
        when(admin.describeTopics(eq(List.of(TOPIC)))).thenReturn(describe);

        var listOffsets = mock(ListOffsetsResult.class);
        when(listOffsets.all()).thenReturn(KafkaFuture.completedFuture(offsets));
        when(admin.listOffsets(any())).thenAnswer(inv -> {
            // The request must be exactly "latest" per partition — "earliest" would
            // beginningOffsets and thereby measure the history again.
            Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> request = inv.getArgument(0);
            assertThat(request).hasSize(endOffsets.length);
            request.forEach((tp, spec) ->
                    assertThat(spec)
                            .as("OffsetSpec bei Partition %s", tp.partition())
                            .isInstanceOf(org.apache.kafka.clients.admin.OffsetSpec.LatestSpec.class));
            return listOffsets;
        });
    }

    private void givenTopicDoesNotExist() {
        var describe = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, org.apache.kafka.clients.admin.TopicDescription>> failure =
                failedWith(new UnknownTopicOrPartitionException(TOPIC));
        when(describe.allTopicNames()).thenReturn(failure);
        when(admin.describeTopics(eq(List.of(TOPIC)))).thenReturn(describe);
    }

    @SuppressWarnings("unchecked")
    private void givenCommitted(Map<TopicPartition, OffsetAndMetadata> committed) {
        var result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(committed));
        when(admin.listConsumerGroupOffsets(GROUP)).thenReturn(result);
    }

    private void givenGroupNeverCommitted() {
        var result = mock(ListConsumerGroupOffsetsResult.class);
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> failure =
                failedWith(new GroupIdNotFoundException(GROUP));
        when(result.partitionsToOffsetAndMetadata()).thenReturn(failure);
        when(admin.listConsumerGroupOffsets(GROUP)).thenReturn(result);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        admin = mock(AdminClient.class);
        var redis = mock(StringRedisTemplate.class);
        service = new AutoScalingService(
                new OrchestratorProperties(new OrchestratorProperties.Kafka(List.of()),
                        null, null, null,
                        new OrchestratorProperties.AutoScaling(
                                true, 30, 500, 2, 20, TOPIC, GROUP), null, null,
                null),
                mock(ConsumerFactory.class), redis, mock(OrchestratorMetrics.class));
    }

    @Nested
    @DisplayName("normaler Betrieb")
    class Working {

        @Test
        @DisplayName("returns the backlog per partition")
        void computesLagPerPartition() {
            givenTopic(1000L, 800L);
            givenCommitted(Map.of(
                    new TopicPartition(TOPIC, 0), new OffsetAndMetadata(400L),
                    new TopicPartition(TOPIC, 1), new OffsetAndMetadata(300L)));

            var lag = service.computeTotalLag(admin, TOPIC, GROUP);

            assertThat(lag).containsExactlyInAnyOrderEntriesOf(Map.of(0, 600L, 1, 500L));
        }

        @Test
        @DisplayName("asks for latest, not for earliest")
        void asksForLatestOffsets() {
            givenTopic(500L, 500L);
            givenCommitted(Map.of());

            // Die Assertion sitzt im givenTopic-Stub: er fuehrt bei listOffsets
            // out and checks the OffsetSpec.
            assertThat(service.computeTotalLag(admin, TOPIC, GROUP))
                    .containsEntry(0, 500L)
                    .containsEntry(1, 500L);
        }

        @Test
        @DisplayName("a topic without commits reports everything as backlog")
        void groupWithoutCommitsMeansEverythingPending() {
            givenTopic(120L, 80L);
            givenGroupNeverCommitted();

            var lag = service.computeTotalLag(admin, TOPIC, GROUP);

            assertThat(lag).containsExactlyInAnyOrderEntriesOf(Map.of(0, 120L, 1, 80L));
        }

        @Test
        @DisplayName("an empty consumer map does not become an error")
        void emptyCommittedMapIsNotAFailure() {
            givenTopic(64L);
            givenCommitted(Map.of());

            assertThat(service.computeTotalLag(admin, TOPIC, GROUP)).containsEntry(0, 64L);
        }
    }

    @Nested
    @DisplayName("Stoerungen")
    class Failures {

        @Test
        @DisplayName("an unknown topic returns empty instead of throwing")
        void unknownTopicYieldsEmpty() {
            givenTopicDoesNotExist();

            assertThat(service.computeTotalLag(admin, TOPIC, GROUP)).isEmpty();
        }

        @Test
        @DisplayName("the caller never sees an exception")
        void brokerFailureIsSwallowed() {
            var broken = mock(AdminClient.class);
            var describe = mock(DescribeTopicsResult.class);
            KafkaFuture<Map<String, org.apache.kafka.clients.admin.TopicDescription>> failure =
                    failedWith(new RuntimeException("broker gone"));
            when(describe.allTopicNames()).thenReturn(failure);
            when(broken.describeTopics(ArgumentMatchers.<String>anyCollection())).thenReturn(describe);

            // 0 partitions means "nothing to do" in evaluateScale, not an error.
            assertThat(service.computeTotalLag(broken, TOPIC, GROUP)).isEmpty();
        }
    }

    @Test
    @DisplayName("a broker without group permission returns 0 and does not block the evaluation")
    void authorizationErrorYieldsEmpty() {
        givenTopic(100L);
        var result = mock(ListConsumerGroupOffsetsResult.class);
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> failure = failedWith(
                new org.apache.kafka.common.errors.GroupAuthorizationException(GROUP));
        when(result.partitionsToOffsetAndMetadata()).thenReturn(failure);
        when(admin.listConsumerGroupOffsets(GROUP)).thenReturn(result);

        assertThat(service.computeTotalLag(admin, TOPIC, GROUP)).isEmpty();
    }

    @Test
    @DisplayName("the consumer properties need no group — the AdminClient manages without")
    void adminClientNeedsNoGroupId() {
        // The production props: Spring Boot without a global group-id. The AdminClient
        // gets only the bootstrap server from it and runs without a group.
        var factory = new org.springframework.kafka.core.DefaultKafkaConsumerFactory<String, String>(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, List.of("localhost:9093"),
                       ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                       "org.apache.kafka.common.serialization.StringDeserializer",
                       ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                       "org.apache.kafka.common.serialization.StringDeserializer"));
        var props = new HashMap<>(factory.getConfigurationProperties());

        assertThat(props).doesNotContainKey(ConsumerConfig.GROUP_ID_CONFIG);
        assertThat(AutoScalingService.adminProperties(props))
                .containsOnlyKeys(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
                .containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
    }
}