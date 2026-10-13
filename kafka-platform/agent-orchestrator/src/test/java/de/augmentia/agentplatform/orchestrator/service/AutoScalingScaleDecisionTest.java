package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.metrics.OrchestratorMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the evaluation that actually sets the target value.
 *
 * <p>Two bugs, both in {@code evaluateScale}:
 *
 * <ol>
 *   <li>The return value of {@code acquireSemaphore()} was discarded. The lock was therefore
 *       without effect, and because the {@code finally} released unconditionally, an instance
 *       could break another's lock in the middle of its evaluation.</li>
 *   <li>{@code gaugeWorkerScaleTarget} received {@code currentTarget} — the state from
 *       before the change. The metric reported the new state one cycle too late after every
 *       scaling step. Anyone looking at the metric saw a scaling during scale-up that had
 *       not yet happened.</li>
 * </ol>
 */
@DisplayName("AutoScalingService — decision and target metric")
class AutoScalingScaleDecisionTest {

    private static final int LAG_THRESHOLD = 500;
    private static final int MIN_WORKERS = 2;
    private static final int MAX_WORKERS = 20;

    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private OrchestratorMetrics metrics;
    private AutoScalingService service;

    private static OrchestratorProperties.AutoScaling cfg() {
        return new OrchestratorProperties.AutoScaling(
                true, 30, LAG_THRESHOLD, MIN_WORKERS, MAX_WORKERS, null, null);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        metrics = mock(OrchestratorMetrics.class);
        service = new AutoScalingService(new OrchestratorProperties(
                        new OrchestratorProperties.Kafka(java.util.List.of()),
                        null, null, null, cfg(), null, null,
                null),
                mock(ConsumerFactory.class), redis, metrics);
    }

    private void givenScaleTarget(int target) {
        when(values.get("autoscaling:target")).thenReturn(String.valueOf(target));
    }

    private void givenConsecutiveDecrease(long count) {
        when(values.get("autoscaling:consecutive_decrease")).thenReturn(String.valueOf(count));
    }

    @Nested
    @DisplayName("hochfahren")
    class ScalingUp {

        @Test
        @DisplayName("reports the new target value, not the old one")
        void gaugeReportsNewTarget() {
            givenScaleTarget(4);

            service.applyScaleDecision(4, 900.0, cfg());

            // Previously currentTarget stood here — the metric would have stayed at 4,
            // although 5 was set.
            verify(metrics).gaugeWorkerScaleTarget(5);
        }

        @Test
        @DisplayName("sets the target value in Redis")
        void writesNewTarget() {
            givenScaleTarget(4);

            service.applyScaleDecision(4, 900.0, cfg());

            verify(values).set(eq("autoscaling:target"), eq("5"), any(java.time.Duration.class));
        }

        @Test
        @DisplayName("bleibt bei max-workers stehen")
        void capsAtMaxWorkers() {
            givenScaleTarget(MAX_WORKERS);

            service.applyScaleDecision(MAX_WORKERS, 900.0, cfg());

            verify(metrics).gaugeWorkerScaleTarget(MAX_WORKERS);
            verify(values, never()).set(eq("autoscaling:target"), anyString(), any());
        }

        @Test
        @DisplayName("exactly at the threshold there is no scale-up")
        void thresholdIsExclusive() {
            givenScaleTarget(4);

            service.applyScaleDecision(4, LAG_THRESHOLD, cfg());

            verify(metrics).gaugeWorkerScaleTarget(4);
        }
    }

    @Nested
    @DisplayName("runterfahren")
    class ScalingDown {

        @Test
        @DisplayName("if the counter is at 5, it scales down and reports the new value")
        void gaugeReportsNewTargetWhenCounterReachedFive() {
            givenScaleTarget(8);
            givenConsecutiveDecrease(5);

            service.applyScaleDecision(8, 10.0, cfg());

            verify(metrics).gaugeWorkerScaleTarget(7);
            verify(values).set(eq("autoscaling:target"), eq("7"), any(Duration.class));
            verify(values).set(eq("autoscaling:consecutive_decrease"), eq("0"), any(Duration.class));
        }

        @Test
        @DisplayName("below 5 the counter counts up instead of scaling down")
        void belowFiveIncrementsCounterInsteadOfScaling() {
            givenScaleTarget(8);
            givenConsecutiveDecrease(3);

            service.applyScaleDecision(8, 10.0, cfg());

            verify(metrics).gaugeWorkerScaleTarget(8);
            verify(values).set(eq("autoscaling:consecutive_decrease"), eq("4"), any(Duration.class));
            verify(values, never()).set(eq("autoscaling:target"), anyString(), any());
        }

        @Test
        @DisplayName("bleibt bei min-workers stehen")
        void capsAtMinWorkers() {
            givenScaleTarget(MIN_WORKERS);
            givenConsecutiveDecrease(9);

            service.applyScaleDecision(MIN_WORKERS, 10.0, cfg());

            verify(metrics).gaugeWorkerScaleTarget(MIN_WORKERS);
        }
    }

    @Test
    @DisplayName("im mittleren Band aendert sich nichts")
    void deadBandLeavesTargetAlone() {
        givenScaleTarget(7);

        // Between half and full threshold: neither up nor down.
        service.applyScaleDecision(7, 400.0, cfg());

        verify(metrics).gaugeWorkerScaleTarget(7);
        verify(values, never()).set(eq("autoscaling:target"), anyString(), any());
        verify(values).set(eq("autoscaling:consecutive_decrease"), eq("0"), any(java.time.Duration.class));
    }

    @Test
    @DisplayName("fehlt der Zielwert in Redis, gilt min-workers")
    void missingTargetFallsBackToMinWorkers() {
        when(values.get("autoscaling:target")).thenReturn(null);

        service.applyScaleDecision(MIN_WORKERS, 900.0, cfg());

        verify(metrics).gaugeWorkerScaleTarget(MIN_WORKERS + 1);
    }

    @Nested
    @DisplayName("Lock-Disziplin")
    class Semaphore {

        private void givenLock(Boolean result) {
            when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(result);
        }

        @Test
        @DisplayName("if another instance holds the lock, nothing is written")
        void doesNotWriteWithoutLock() {
            givenLock(Boolean.FALSE);

            service.evaluateWithLock(4, 900.0, cfg());

            verify(metrics, never()).gaugeWorkerScaleTarget(any(Integer.class));
            verify(values, never()).set(eq("autoscaling:target"), anyString(), any());
        }

        @Test
        @DisplayName("without a lock a foreign lock is not released either")
        void doesNotReleaseForeignLock() {
            givenLock(Boolean.FALSE);

            service.evaluateWithLock(4, 900.0, cfg());

            // That was the second part of the bug: the finally released unconditionally,
            // so an instance that could not even acquire it released the lock of an
            // of another in the middle of its evaluation.
            verify(redis, never()).delete(anyString());
        }

        @Test
        @DisplayName("null from Redis counts as not obtained")
        void nullFromRedisIsNotAcquired() {
            givenLock(null);

            service.evaluateWithLock(4, 900.0, cfg());

            verify(metrics, never()).gaugeWorkerScaleTarget(any(Integer.class));
            verify(redis, never()).delete(anyString());
        }

        @Test
        @DisplayName("with a lock the decision is made and the lock is released again")
        void writesAndReleasesWhenAcquired() {
            givenLock(Boolean.TRUE);
            givenScaleTarget(4);

            service.evaluateWithLock(4, 900.0, cfg());

            verify(metrics).gaugeWorkerScaleTarget(5);
            verify(values).set(eq("autoscaling:target"), eq("5"), any(Duration.class));
            verify(redis).delete("autoscaling:semaphore");
        }

        @Test
        @DisplayName("the lock is released even on an error in the decision")
        void releasesLockOnFailure() {
            givenLock(Boolean.TRUE);
            when(values.get("autoscaling:target")).thenReturn("4");
            // Writing the new target value fails — the most frequent error
            // in operation (Redis unreachable).
            org.mockito.Mockito.doThrow(new IllegalStateException("redis gone"))
                    .when(values).set(org.mockito.ArgumentMatchers.anyString(),
                            org.mockito.ArgumentMatchers.anyString(),
                            org.mockito.ArgumentMatchers.any(Duration.class));

            assertThatThrownBy(() -> service.evaluateWithLock(4, 900.0, cfg()))
                    .isInstanceOf(IllegalStateException.class);

            // Otherwise this instance holds the lock for the full minute and all
            // other instances do not evaluate at all during that time.
            verify(redis).delete("autoscaling:semaphore");
        }
    }
}