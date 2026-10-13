package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import de.augmentia.agentplatform.bridge.AgentDefinitionBridge;
import de.augmentia.agentplatform.orchestrator.domain.AgentDefinitionRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Clock;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guard for the timeout cap (K3).
 *
 * <p>Background: the quad worker blocks its poll thread until the whole Kafka batch is
 * done. If it runs longer than {@code max.poll.interval.ms} (default 900000), the consumer
 * is kicked out of the group, there is a rebalance, and the still-running tasks are paid
 * for again by another instance. Therefore the orchestrator must not dispatch a task whose
 * timeout exceeds {@code app.orchestration.max-task-timeout-ms}.
 *
 * <p>Deliberately without a Spring test context and without a database:
 * {@code resolveTimeoutMs} is a pure function of node, agent row and properties. The
 * publisher is not touched.
 */
@SpringJUnitConfig(TaskDispatcherTimeoutCapTest.DispatcherConfig.class)
@DisplayName("TaskDispatcher — timeout cap")
class TaskDispatcherTimeoutCapTest {

    private static final long CEILING = 600_000L;   // 10 min
    private static final long DEFAULT_TIMEOUT = 60_000L;

    @Autowired
    private TaskDispatcher dispatcher;

    // ── Fixtures ──

    private static DagNode node(Integer timeoutMs) {
        return DagNode.builder()
                .stepId("s1")
                .nodeType(NodeType.STEP)
                .agentType("researcher")
                .timeoutMs(timeoutMs)
                .build();
    }

    private static AgentDefinitionRow agent(int timeoutSeconds, int maxRetries) {
        return new AgentDefinitionRow(
                "researcher", "desc", "prompt", "mock-deterministic",
                0.0d, 1024, timeoutSeconds, maxRetries, 1,
                AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS);
    }

    private static OrchestratorProperties propsWithCeiling(long ceiling) {
        return new OrchestratorProperties(
                new OrchestratorProperties.Kafka(List.of()),
                new OrchestratorProperties.Orchestration(60_000L, 3, 5_000L, 1_000L, ceiling),
                new OrchestratorProperties.Hitl(24L, 3),
                new OrchestratorProperties.Seeding(false, ""),
                new OrchestratorProperties.AutoScaling(false, 30, 500, 2, 20, null, null),
                new OrchestratorProperties.Vault(false, "", "", "", 60L),
                new OrchestratorProperties.Lease("n", 30L),
                null);
    }

    private static TaskDispatcher dispatcherWithCeiling(long ceiling) {
        // The AgentDefinitionStore is not used in resolveTimeoutMs; null is enough.
        return new TaskDispatcher(
                (JdbcAgentDefinitionStore) null,
                new NoopPublisher(),
                propsWithCeiling(ceiling),
                Clock.systemUTC());
    }

    // ── Tests ──

    @Test
    @DisplayName("No node timeout and no agent timeout -> default")
    void fallsBackToDefault() {
        assertThat(dispatcher.resolveTimeoutMs(node(null), agent(0, 3)))
                .isEqualTo(DEFAULT_TIMEOUT);
    }

    @Test
    @DisplayName("a node timeout below the cap stays unchanged")
    void keepsTimeoutBelowCeiling() {
        assertThat(dispatcher.resolveTimeoutMs(node(120_000), agent(0, 3)))
                .isEqualTo(120_000L);
    }

    @Test
    @DisplayName("a node timeout above the cap is capped")
    void capsNodeTimeoutAboveCeiling() {
        // 45 minutes — clearly above the worker's poll interval.
        assertThat(dispatcher.resolveTimeoutMs(node(2_700_000), agent(0, 3)))
                .isEqualTo(CEILING);
    }

    @Test
    @DisplayName("an agent timeout in seconds is converted and capped as well")
    void capsAgentTimeoutAboveCeiling() {
        // 40 min = 2,400,000 ms -> above the cap
        assertThat(dispatcher.resolveTimeoutMs(node(null), agent(2_400, 3)))
                .as("the agent timeout is converted to ms first, then capped")
                .isEqualTo(CEILING);
    }

    @Test
    @DisplayName("an agent timeout below the cap is preserved")
    void keepsAgentTimeoutBelowCeiling() {
        assertThat(dispatcher.resolveTimeoutMs(node(null), agent(90, 3)))
                .isEqualTo(90_000L);
    }

    @Test
    @DisplayName("a node timeout beats the agent timeout")
    void nodeTimeoutWinsOverAgentTimeout() {
        assertThat(dispatcher.resolveTimeoutMs(node(30_000), agent(2_400, 3)))
                .as("an explicit node timeout takes precedence")
                .isEqualTo(30_000L);
    }

    @Test
    @DisplayName("the cap is configurable via maxTaskTimeoutMs")
    void ceilingIsConfigurable() {
        assertThat(dispatcherWithCeiling(1_000L).resolveTimeoutMs(node(120_000), agent(0, 3)))
                .isEqualTo(1_000L);
    }

    /** resolveTimeoutMs publishes nothing — the publisher stays silent. */
    private static final class NoopPublisher extends OrchestratorEventPublisher {
        NoopPublisher() {
            super(null, new org.springframework.beans.factory.support.DefaultListableBeanFactory()
                    .getBeanProvider(io.micrometer.tracing.Tracer.class));
        }

        @Override
        public void publish(String topic, String key, String payload) {
            throw new AssertionError("resolveTimeoutMs must not publish: " + topic);
        }

        @Override
        public void publishAfterCommit(String topic, String key, String payload) {
            throw new AssertionError("resolveTimeoutMs must not publish: " + topic);
        }
    }

    @TestConfiguration
    static class DispatcherConfig {

        @Bean
        OrchestratorProperties properties() {
            return propsWithCeiling(CEILING);
        }

        @Bean
        TaskDispatcher taskDispatcher(OrchestratorProperties properties) {
            return dispatcherWithCeiling(CEILING);
        }
    }
}
