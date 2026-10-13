package de.augmentia.agentflow.core;

import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.CompileConfig;
import org.bsc.langgraph4j.GraphInput;
import org.bsc.langgraph4j.checkpoint.MemorySaver;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AgentFlowGraphTest {

    @Test
    void plannerExecutorAndReviewerRunUntilApproved() throws Exception {
        var graph = AgentFlowGraph.build(
                state -> Map.of(
                        AgentFlowState.PLAN, "plan:" + state.input(),
                        AgentFlowState.TRACE, "planner;"),
                state -> Map.of(
                        AgentFlowState.RESULT, "result(" + state.plan() + ")#" + state.attempts(),
                        AgentFlowState.TRACE, state.trace() + "executor;"),
                state -> {
                    int attempts = state.attempts() + 1;
                    return Map.of(
                            AgentFlowState.ATTEMPTS, attempts,
                            AgentFlowState.REVIEW, attempts >= 2 ? "approved" : "retry",
                            AgentFlowState.TRACE, state.trace() + "reviewer;");
                });

        AgentFlowState result = graph
                .invoke(Map.of(AgentFlowState.INPUT, "hello"), RunnableConfig.builder().build())
                .orElseThrow();

        assertThat(result.review()).isEqualTo("approved");
        assertThat(result.attempts()).isEqualTo(2);
        assertThat(result.trace()).contains("planner;").contains("executor;").contains("reviewer;");
    }

    @Test
    void reviewerCanApproveOnFirstPass() throws Exception {
        var graph = AgentFlowGraph.build(
                state -> Map.of(AgentFlowState.PLAN, "p"),
                state -> Map.of(AgentFlowState.RESULT, "r"),
                state -> Map.of(AgentFlowState.ATTEMPTS, state.attempts() + 1, AgentFlowState.REVIEW, "approved"));

        AgentFlowState result = graph
                .invoke(Map.of(AgentFlowState.INPUT, "x"), RunnableConfig.builder().build())
                .orElseThrow();

        assertThat(result.attempts()).isEqualTo(1);
        assertThat(result.result()).isEqualTo("r");
    }

    @Test
    void interruptsBeforeReviewerAndResumesOnApproval() throws Exception {
        var graph = AgentFlowGraph.build(
                state -> Map.of(AgentFlowState.PLAN, "plan"),
                state -> Map.of(AgentFlowState.RESULT, "result"),
                state -> Map.of(
                        AgentFlowState.ATTEMPTS, state.attempts() + 1,
                        AgentFlowState.REVIEW, state.approval() ? "approved" : "rejected"),
                CompileConfig.builder()
                        .checkpointSaver(new MemorySaver())
                        .interruptBefore(AgentFlowGraph.REVIEWER)
                        .build());

        RunnableConfig config = RunnableConfig.builder().threadId("t-hitl").build();

        // Runs planner + executor, then pauses before the reviewer.
        AgentFlowState interrupted = graph.invoke(Map.of(AgentFlowState.INPUT, "x"), config).orElseThrow();
        assertThat(interrupted.result()).isEqualTo("result");
        assertThat(interrupted.review()).isEmpty();

        // Human approves -> resume; reviewer applies the decision and routes to END.
        AgentFlowState approved = graph
                .invoke(GraphInput.resume(Map.of(AgentFlowState.APPROVAL, true)), config)
                .orElseThrow();

        assertThat(approved.review()).isEqualTo("approved");
        assertThat(approved.approval()).isTrue();
    }
}
