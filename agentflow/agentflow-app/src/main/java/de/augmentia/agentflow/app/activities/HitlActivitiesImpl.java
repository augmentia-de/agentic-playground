package de.augmentia.agentflow.app.activities;

import de.augmentia.agentflow.app.config.RequestContext;
import de.augmentia.agentflow.app.workflow.HitlActivities;
import de.augmentia.agentflow.core.AgentFlowGraph;
import de.augmentia.agentflow.core.AgentFlowState;
import org.bsc.langgraph4j.CompileConfig;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphInput;
import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.checkpoint.BaseCheckpointSaver;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Component;

import java.util.Map;
import io.temporal.spring.boot.ActivityImpl;

/**
 * HITL activities: start the graph until it interrupts before the reviewer; on approval,
 * resume from the persistent checkpoint and finish.
 */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class HitlActivitiesImpl implements HitlActivities {

    /** Stable graph id so checkpoints started in one worker resume in another. */
    private static final String GRAPH_ID = "agentflow";

    private final BaseCheckpointSaver checkpointSaver;
    private final ChatClient chatClient;

    public HitlActivitiesImpl(BaseCheckpointSaver checkpointSaver, ChatClient.Builder chatClientBuilder) {
        this.checkpointSaver = checkpointSaver;
        this.chatClient = chatClientBuilder.build();
    }

    @Override
    public HitlStart startGraph(String input, String threadId) {
        RequestContext.set(threadId);
        try {
            CompiledGraph<AgentFlowState> graph = buildGraph(true);
            RunnableConfig config = RunnableConfig.builder().threadId(threadId).build();
            AgentFlowState paused = graph.invoke(Map.of(AgentFlowState.INPUT, input), config).orElseThrow();
            return new HitlStart(threadId, paused.plan(), paused.result());
        } catch (Exception e) {
            throw new IllegalStateException("HITL start failed", e);
        } finally {
            RequestContext.clear();
        }
    }

    @Override
    public String resumeGraph(String threadId) {
        RequestContext.set(threadId);
        try {
            CompiledGraph<AgentFlowState> graph = buildGraph(false);
            RunnableConfig config = RunnableConfig.builder().threadId(threadId).build();
            AgentFlowState resumed = graph
                    .invoke(GraphInput.resume(Map.of(AgentFlowState.APPROVAL, true)), config)
                    .orElseThrow();
            return resumed.result();
        } catch (Exception e) {
            throw new IllegalStateException("HITL resume failed", e);
        } finally {
            RequestContext.clear();
        }
    }

    private CompiledGraph<AgentFlowState> buildGraph(boolean interruptBeforeReviewer) throws Exception {
        var builder = CompileConfig.builder().graphId(GRAPH_ID).checkpointSaver(checkpointSaver);
        if (interruptBeforeReviewer) {
            builder.interruptBefore(AgentFlowGraph.REVIEWER);
        }
        return AgentFlowGraph.build(
                state -> Map.of(AgentFlowState.PLAN, call("Create a short plan for: " + state.input())),
                state -> Map.of(AgentFlowState.RESULT, call("Execute this plan and answer: " + state.plan())),
                state -> Map.of(
                        AgentFlowState.ATTEMPTS, state.attempts() + 1,
                        AgentFlowState.REVIEW, state.approval() ? "approved" : "retry"),
                builder.build());
    }

    private String call(String prompt) {
        String content = chatClient.prompt().user(prompt).call().content();
        return content != null ? content : "";
    }
}
