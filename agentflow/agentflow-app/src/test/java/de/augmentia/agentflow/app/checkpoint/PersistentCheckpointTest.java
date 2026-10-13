package de.augmentia.agentflow.app.checkpoint;

import de.augmentia.agentflow.core.AgentFlowGraph;
import de.augmentia.agentflow.core.AgentFlowState;
import org.bsc.langgraph4j.CompileConfig;
import org.bsc.langgraph4j.GraphInput;
import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.checkpoint.SQLiteSaverV2;
import org.bsc.langgraph4j.serializer.std.ObjectStreamStateSerializer;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * (c) Persistent checkpointing: a graph paused for HITL resumes from a fresh saver
 * instance on the same SQLite file — i.e. across worker restarts.
 */
class PersistentCheckpointTest {

    private static final String GRAPH_ID = "agentflow";

    @Test
    void resumesFromSqliteAcrossSaverInstances() throws Exception {
        Path dir = Files.createTempDirectory("agentflow-cp");
        String dbPath = dir.resolve("checkpoints.db").toString();
        String threadId = "t-persist";

        var saver1 = SQLiteSaverV2.builder()
                .databasePath(dbPath)
                .stateSerializer(new ObjectStreamStateSerializer<>(AgentFlowState::new))
                .createTables(true)
                .build();
        var graph1 = AgentFlowGraph.build(planner(), executor(), reviewer(),
                CompileConfig.builder().graphId(GRAPH_ID).checkpointSaver(saver1)
                        .interruptBefore(AgentFlowGraph.REVIEWER).build());
        AgentFlowState paused = graph1.invoke(
                Map.of(AgentFlowState.INPUT, "x"),
                RunnableConfig.builder().threadId(threadId).build()).orElseThrow();
        assertThat(paused.result()).isEqualTo("result");

        // New "worker instance": new saver, same DB file, same graph id.
        var saver2 = SQLiteSaverV2.builder()
                .databasePath(dbPath)
                .stateSerializer(new ObjectStreamStateSerializer<>(AgentFlowState::new))
                .createTables(true)
                .build();
        var graph2 = AgentFlowGraph.build(planner(), executor(), reviewer(),
                CompileConfig.builder().graphId(GRAPH_ID).checkpointSaver(saver2).build());
        AgentFlowState resumed = graph2.invoke(
                GraphInput.resume(Map.of(AgentFlowState.APPROVAL, true)),
                RunnableConfig.builder().threadId(threadId).build()).orElseThrow();

        assertThat(resumed.review()).isEqualTo("approved");
        assertThat(resumed.approval()).isTrue();
    }

    private AgentFlowGraph.NodeHandler planner() {
        return state -> Map.of(AgentFlowState.PLAN, "plan");
    }

    private AgentFlowGraph.NodeHandler executor() {
        return state -> Map.of(AgentFlowState.RESULT, "result");
    }

    private AgentFlowGraph.NodeHandler reviewer() {
        return state -> Map.of(
                AgentFlowState.ATTEMPTS, state.attempts() + 1,
                AgentFlowState.REVIEW, state.approval() ? "approved" : "retry");
    }
}
