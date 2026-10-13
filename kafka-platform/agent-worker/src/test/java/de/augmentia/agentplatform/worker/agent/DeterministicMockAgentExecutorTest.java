package de.augmentia.agentplatform.worker.agent;

import de.augmentia.agentplatform.common.agent.AgentExecutionResult;
import de.augmentia.agentplatform.common.event.AgentTaskEvent;
import de.augmentia.agentplatform.common.model.ResultStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class DeterministicMockAgentExecutorTest {

    private final DeterministicMockAgentExecutor executor = new DeterministicMockAgentExecutor();

    private static AgentTaskEvent task(String agentType, String stepId, int iteration, int attempt,
                                       Map<String, Object> input, Map<String, Object> context) {
        return new AgentTaskEvent(
                new AgentTaskEvent.TaskHeader(
                        "wf-1",
                        "task-" + stepId + "-" + iteration + "-" + attempt,
                        null,
                        "trace-1",
                        attempt,
                        3,
                        Instant.parse("2026-01-01T00:00:00Z"),
                        60_000L
                ),
                new AgentTaskEvent.AgentConfig(agentType, 1, "mock-deterministic", 0.0d),
                new AgentTaskEvent.TaskPayload(
                        "Execute " + stepId,
                        List.of(),
                        Map.of(
                                "stepId", stepId,
                                "iteration", iteration,
                                "attempt", attempt,
                                "input", input,
                                "context", context
                        )
                )
        );
    }

    @Test
    void sameInputProducesSameResult() {
        AgentTaskEvent task = task("researcher", "research_step", 1, 1, Map.of(), Map.of());

        AgentExecutionResult first = executor.execute(task);
        AgentExecutionResult second = executor.execute(task);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void usesExplicitResearchConfidence() {
        AgentTaskEvent task = task("researcher", "research_step", 1, 1, Map.of("researchConfidence", 0.72d), Map.of());

        AgentExecutionResult result = executor.execute(task);

        assertThat(result.status()).isEqualTo(ResultStatus.COMPLETED);
        assertThat(result.resultData()).containsEntry("confidence", 0.72d);
    }

    @Test
    void refinerImprovesConfidencePerIteration() {
        Map<String, Object> context = Map.of("research_step", Map.of("resultData", Map.of("confidence", 0.72d)));

        AgentExecutionResult first = executor.execute(task("refiner", "refinement_loop", 1, 1, Map.of(), context));
        AgentExecutionResult second = executor.execute(task("refiner", "refinement_loop", 2, 1, Map.of(), context));

        assertThat((Double) first.resultData().get("confidence")).isCloseTo(0.72d, within(0.001d));
        assertThat((Double) second.resultData().get("confidence")).isCloseTo(0.84d, within(0.001d));
    }

    @Test
    void failAttemptsProducesRetryThenSuccess() {
        Map<String, Object> input = Map.of("failAttempts", Map.of("b.retryable.task", 1));

        AgentExecutionResult first = executor.execute(task("fixer", "b.retryable.task", 1, 1, input, Map.of()));
        AgentExecutionResult second = executor.execute(task("fixer", "b.retryable.task", 1, 2, input, Map.of()));

        assertThat(first.status()).isEqualTo(ResultStatus.FAILED_RETRYABLE);
        assertThat(second.status()).isEqualTo(ResultStatus.COMPLETED);
    }

    @Test
    void forceStatusProducesFatalAndHitl() {
        AgentExecutionResult fatal = executor.execute(task("coder", "b.fatal.task", 1, 1,
                Map.of("forceStatus", Map.of("b.fatal.task", "FAILED_FATAL")), Map.of()));
        AgentExecutionResult hitl = executor.execute(task("reviewer", "b.hitl.task", 1, 1,
                Map.of("forceStatus", Map.of("b.hitl.task", "NEEDS_HITL")), Map.of()));

        assertThat(fatal.status()).isEqualTo(ResultStatus.FAILED_FATAL);
        assertThat(hitl.status()).isEqualTo(ResultStatus.NEEDS_HITL);
    }

    @Test
    void appliesResultDataOverrides() {
        AgentExecutionResult result = executor.execute(task("planner", "prepare", 1, 1,
                Map.of("resultDataOverrides", Map.of("prepare", Map.of("runLint", true))),
                Map.of()));

        assertThat(result.resultData()).containsEntry("runLint", true);
    }
}
