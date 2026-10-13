package de.augmentia.agentplatform.orchestrator.service;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the auto-scaler's lag computation.
 *
 * <p>Two bugs were embedded in this evaluation, and no test saw either:
 *
 * <ol>
 *   <li>It built a {@code KafkaConsumer} from the factory properties and called
 *       {@code subscribe()}. In production the properties carry no {@code group.id} — all
 *       groups sit on the {@code @KafkaListener} annotation and such overrides do not land
 *       in the factory properties. Result: {@code InvalidGroupIdException} every 30
 *       seconds, evaluation empty, scaling never happened. The log showed it as ERROR and
 *       was dismissed as noise.</li>
 *   <li>It computed {@code endOffset - beginningOffset}. That is not the lag but the entire
 *       retained history of the topic. From the moment retention applies you are measuring
 *       the past and driving the workers up permanently.</li>
 * </ol>
 *
 * <p>The computation itself needs no broker and is checked here directly against the real
 * Kafka offset types.
 */
@DisplayName("AutoScalingService — lag per partition")
class AutoScalingServiceLagTest {

    private static final String TOPIC = "agent.control.loop";

    private static Map<TopicPartition, Long> ends(Object... partitionAndOffset) {
        Map<TopicPartition, Long> map = new HashMap<>();
        for (int i = 0; i < partitionAndOffset.length; i += 2) {
            map.put(new TopicPartition(TOPIC, (int) partitionAndOffset[i]),
                    (long) partitionAndOffset[i + 1]);
        }
        return map;
    }

    private static Map<TopicPartition, OffsetAndMetadata> commits(Object... partitionAndOffset) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        for (int i = 0; i < partitionAndOffset.length; i += 2) {
            map.put(new TopicPartition(TOPIC, (int) partitionAndOffset[i]),
                    new OffsetAndMetadata((long) partitionAndOffset[i + 1]));
        }
        return map;
    }

    @Nested
    @DisplayName("endOffset minus committeter Offset")
    class AgainstCommittedOffsets {

        @Test
        @DisplayName("subtracts the group offset, not the beginning offset")
        void subtractsCommittedOffset() {
            var lag = AutoScalingService.lagPerPartition(
                    ends(0, 1000L, 1, 800L),
                    commits(0, 400L, 1, 300L));

            assertThat(lag).containsExactlyInAnyOrderEntriesOf(
                    Map.of(0, 600L, 1, 500L));
        }

        @Test
        @DisplayName("a full partition yields 0 and no negative result")
        void fullyConsumedIsZero() {
            var lag = AutoScalingService.lagPerPartition(
                    ends(0, 500L, 1, 42L),
                    commits(0, 500L, 1, 42L));

            assertThat(lag).containsExactlyInAnyOrderEntriesOf(
                    Map.of(0, 0L, 1, 0L));
        }

        @Test
        @DisplayName("if the group is ahead of the log, the lag is 0 instead of negative")
        void committedAheadDoesNotGoNegative() {
            // Happens after a reset: broker offset rolled back, group at the old state.
            var lag = AutoScalingService.lagPerPartition(
                    ends(0, 100L),
                    commits(0, 900L));

            assertThat(lag).containsExactlyInAnyOrderEntriesOf(Map.of(0, 0L));
        }

        @Test
        @DisplayName("an intact history yields 0 lag — that was the actual bug")
        void fullHistoryIsNoLag() {
            // 10,000 messages retained, everything consumed. The old formula
            // endOffset - beginningOffset yielded 10,000 here and drove the workers to
            // maxWorkers. The real lag is 0.
            var lag = AutoScalingService.lagPerPartition(
                    ends(0, 10_000L),
                    commits(0, 10_000L));

            assertThat(lag).containsEntry(0, 0L);
        }
    }

    @Nested
    @DisplayName("group without commits")
    class WithoutCommittedOffsets {

        @Test
        @DisplayName("everything retained counts as lag")
        void everythingIsPending() {
            var lag = AutoScalingService.lagPerPartition(ends(0, 250L, 1, 175L), Map.of());

            assertThat(lag).containsExactlyInAnyOrderEntriesOf(
                    Map.of(0, 250L, 1, 175L));
        }

        @Test
        @DisplayName("a missing partition counts as not committed")
        void missingPartitionCountsAsUnconsumed() {
            var lag = AutoScalingService.lagPerPartition(
                    ends(0, 300L, 1, 300L),
                    commits(0, 300L));

            assertThat(lag).containsEntry(0, 0L).containsEntry(1, 300L);
        }
    }

    @Test
    @DisplayName("every partition appears — the counter must not swallow them")
    void everyPartitionIsRepresented() {
        var lag = AutoScalingService.lagPerPartition(
                ends(0, 10L, 1, 10L, 2, 10L, 3, 10L, 4, 10L, 5, 10L),
                commits(0, 5L, 1, 5L, 2, 5L, 3, 5L, 4, 5L, 5, 5L));

        assertThat(lag).hasSize(6).containsOnlyKeys(0, 1, 2, 3, 4, 5);
        assertThat(lag.values()).allMatch(l -> l == 5L);
    }
}