package de.augmentia.agentflow.core;

import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.CompileConfig;
import org.bsc.langgraph4j.GraphStateException;
import org.bsc.langgraph4j.StateGraph;

import java.util.Map;

import static org.bsc.langgraph4j.StateGraph.END;
import static org.bsc.langgraph4j.StateGraph.START;
import static org.bsc.langgraph4j.action.AsyncEdgeAction.edge_async;
import static org.bsc.langgraph4j.action.AsyncNodeAction.node_async;

/**
 * The canonical agent graph: {@code planner -> executor -> reviewer -> (retry executor | END)}.
 *
 * <p>Framework-neutral: the node bodies are supplied by the caller, so the same graph runs
 * with plain Java (tests), LangChain4j or Spring AI. The durable orchestration (Temporal)
 * wraps this graph; see {@code agentflow-app}.</p>
 */
public final class AgentFlowGraph {

    public static final String PLANNER = "planner";
    public static final String EXECUTOR = "executor";
    public static final String REVIEWER = "reviewer";

    /** A node body: reads the state and returns the keys it updates. */
    @FunctionalInterface
    public interface NodeHandler {
        Map<String, Object> apply(AgentFlowState state);
    }

    private AgentFlowGraph() {
    }

    /**
     * Builds and compiles the graph.
     *
     * @param planner  produces {@code plan}
     * @param executor produces {@code result} (reads {@code plan})
     * @param reviewer sets {@code review} to {@code "approved"} or {@code "retry"} and
     *                 increments {@code attempts}
     */
    public static CompiledGraph<AgentFlowState> build(NodeHandler planner,
                                                      NodeHandler executor,
                                                      NodeHandler reviewer) throws GraphStateException {
        return build(planner, executor, reviewer, CompileConfig.builder().build());
    }

    /**
     * Variant with an explicit {@link CompileConfig} — used for checkpointing and
     * Human-in-the-Loop ({@code interruptBefore(REVIEWER)}).
     */
    public static CompiledGraph<AgentFlowState> build(NodeHandler planner,
                                                      NodeHandler executor,
                                                      NodeHandler reviewer,
                                                      CompileConfig compileConfig) throws GraphStateException {
        StateGraph<AgentFlowState> graph = new StateGraph<>(AgentFlowState::new);
        graph.addNode(PLANNER, node_async(planner::apply))
                .addNode(EXECUTOR, node_async(executor::apply))
                .addNode(REVIEWER, node_async(reviewer::apply))
                .addEdge(START, PLANNER)
                .addEdge(PLANNER, EXECUTOR)
                .addEdge(EXECUTOR, REVIEWER)
                .addConditionalEdges(REVIEWER,
                        edge_async(state -> "approved".equals(state.review()) ? "approved" : "retry"),
                        Map.of("approved", END, "retry", EXECUTOR));
        return graph.compile(compileConfig);
    }
}
