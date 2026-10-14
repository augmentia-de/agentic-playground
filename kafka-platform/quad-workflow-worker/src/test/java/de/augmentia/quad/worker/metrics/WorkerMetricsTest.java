package de.augmentia.quad.worker.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP03/B37 — task outcomes must land on the series that is actually registered/exported.
 */
class WorkerMetricsTest {

    @Test
    void failureIncrementsTheRegisteredOutcomeCounter() {
        var registry = new SimpleMeterRegistry();
        var metrics = new WorkerMetrics(registry);

        metrics.recordTaskFailed("researcher", "FAILED_RETRYABLE");

        var counter = registry.find("quad_worker_tasks_completed_total")
            .tag("status", "failed")
            .counter();
        assertThat(counter)
            .as("the failure must land on the registered outcome series")
            .isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
        assertThat(registry.find("quad_worker_tasks_completed_total")
            .tag("agentType", "researcher").counter()).isNotNull();
    }

    @Test
    void noSeparateFailedMetricIsEmitted() {
        var registry = new SimpleMeterRegistry();
        var metrics = new WorkerMetrics(registry);

        metrics.recordTaskFailed("researcher", "execution_error");

        assertThat(registry.find("quad_worker_tasks_failed_total").counter())
            .as("the duplicating failed_total metric is gone")
            .isNull();
    }

    @Test
    void successAndFailureShareTheOutcomeMetric() {
        var registry = new SimpleMeterRegistry();
        var metrics = new WorkerMetrics(registry);

        metrics.recordTaskCompleted("researcher", "completed");
        metrics.recordTaskFailed("researcher", "FAILED_RETRYABLE");

        assertThat(registry.find("quad_worker_tasks_completed_total")
            .tag("status", "completed").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("quad_worker_tasks_completed_total")
            .tag("status", "failed").counter().count()).isEqualTo(1.0);
    }

    @Test
    void receivedDlqAndToolCountersAreExported() {
        var registry = new SimpleMeterRegistry();
        var metrics = new WorkerMetrics(registry);

        metrics.recordTaskReceived();
        metrics.recordDlqSent();
        metrics.recordToolError("grep");

        assertThat(registry.find("quad_worker_tasks_received_total").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("quad_worker_dlq_total").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("quad_worker_tool_errors_total")
            .tag("tool", "grep").counter().count()).isEqualTo(1.0);
    }
}
