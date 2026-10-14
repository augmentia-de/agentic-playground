package de.augmentia.quad.workflow.engine;

import de.augmentia.quad.core.workflow.run.WorkflowRunState;
import de.augmentia.quad.workflow.engine.context.InputBuilder;
import de.augmentia.quad.workflow.engine.internal.RunStateWriter;
import de.augmentia.quad.workflow.engine.internal.WorkflowExecutionContext;
import de.augmentia.quad.workflow.engine.internal.WorkflowSupport;
import de.augmentia.quad.workflow.engine.node.NodeRunner;
import de.augmentia.quad.workflow.engine.node.executor.AgentNodeExecutor;
import de.augmentia.quad.workflow.engine.node.executor.AsyncNodeExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Async node: bounded waiting and single child dispatch. */
class AsyncNodeExecutorTest {

    @Test
    void zeroTimeoutDoesNotBusyWaitForever() throws Exception {
        var runs = new EngineStubs.InMemoryRunPort();
        var writer = new RunStateWriter(runs, new EngineStubs.InMemoryCheckpoints());
        // default timeout 200ms: a node with timeout==0 must be hard-bounded, not infinite.
        var support = new WorkflowSupport(runs, writer, 50, 200, 1000);
        var nodeExecutor = new AsyncNodeExecutor(support, mock(AgentNodeExecutor.class), writer);

        WorkflowRunState run = runs.create("wf", List.of("a1", "child"), "");
        Map<String, Object> asyncNode = Map.of("id", "a1", "type", "async",
                "config", Map.of("refNodeId", "child", "timeoutMs", 0));
        Map<String, Object> child = Map.of("id", "child", "type", "boom", "config", Map.of());
        var ctx = new WorkflowExecutionContext(run.runId, run, "", "", support.newRunState(), false);

        NodeRunner runner = (node, allNodes, edges, c, skip) -> {
            throw new RuntimeException("child failed");
        };

        ExecutorService pool = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "async-test");
            t.setDaemon(true);
            return t;
        });
        try {
            Future<Boolean> fut = pool.submit(() ->
                    nodeExecutor.execute(asyncNode, List.of(asyncNode, child), List.of(), ctx, runner));
            assertThat(fut.get(5, TimeUnit.SECONDS))
                    .as("a timeout==0 async node must be bounded, not an infinite busy-wait")
                    .isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(ctx.output("a1")).contains("no result within");
    }

    @Test
    void childIsExecutedExactlyOnce() {
        var runs = new EngineStubs.InMemoryRunPort();
        var writer = new RunStateWriter(runs, new EngineStubs.InMemoryCheckpoints());
        var events = new EngineStubs.RecordingEvents();
        var support = new WorkflowSupport(runs, writer, 50, 2000, 1000);
        var agents = new EngineStubs.CountingAgents();
        var agentExecutor = new AgentNodeExecutor(agents,
                new InputBuilder(new EngineStubs.StaticDefinitions(null)), support, writer, events);
        var nodeExecutor = new AsyncNodeExecutor(support, agentExecutor, writer);

        WorkflowRunState run = runs.create("wf", List.of("a1", "child"), "");
        Map<String, Object> asyncNode = Map.of("id", "a1", "type", "async",
                "config", Map.of("refNodeId", "child", "timeoutMs", 5000));
        Map<String, Object> child = Map.of("id", "child", "type", "agent",
                "config", Map.of("agentId", "a1"));
        var ctx = new WorkflowExecutionContext(run.runId, run, "", "", support.newRunState(), false);

        AtomicInteger runnerCalls = new AtomicInteger();
        NodeRunner runner = (node, allNodes, edges, c, skip) -> {
            runnerCalls.incrementAndGet();
            c.putOutput(String.valueOf(node.get("id")), "via-runner");
        };

        boolean ok = nodeExecutor.execute(asyncNode, List.of(asyncNode, child), List.of(), ctx, runner);

        assertThat(ok).isTrue();
        assertThat(ctx.output("a1")).isEqualTo("stub-antwort");
        assertThat(agents.invocations.get())
                .as("the child agent must run exactly once").isEqualTo(1);
        assertThat(runnerCalls.get())
                .as("no second dispatch via the NodeRunner").isZero();
    }
}
