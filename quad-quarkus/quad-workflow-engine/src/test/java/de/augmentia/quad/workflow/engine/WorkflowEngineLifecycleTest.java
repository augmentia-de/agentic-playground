package de.augmentia.quad.workflow.engine;

import de.augmentia.quad.contract.ApiDtos;
import de.augmentia.quad.core.workflow.run.WorkflowRunState;
import de.augmentia.quad.workflow.engine.context.InputBuilder;
import de.augmentia.quad.workflow.engine.internal.RunStateWriter;
import de.augmentia.quad.workflow.engine.internal.WorkflowSupport;
import de.augmentia.quad.workflow.engine.node.NodeExecutor;
import de.augmentia.quad.workflow.engine.node.NodeExecutorRegistry;
import de.augmentia.quad.workflow.engine.node.executor.AgentNodeExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Run lifecycle: no leaked runs, no silently-dropped deferred completions. */
class WorkflowEngineLifecycleTest {

    private record Harness(WorkflowEngine engine, WorkflowSupport support,
                           EngineStubs.InMemoryRunPort runs, EngineStubs.RecordingEvents events) {
    }

    private static Harness harness(NodeExecutor... extra) {
        var runs = new EngineStubs.InMemoryRunPort();
        var checkpoints = new EngineStubs.InMemoryCheckpoints();
        var events = new EngineStubs.RecordingEvents();
        var writer = new RunStateWriter(runs, checkpoints);
        var support = new WorkflowSupport(runs, writer, 50, 200, 1000);
        var agentExecutor = new AgentNodeExecutor(new EngineStubs.CountingAgents(),
                new InputBuilder(new EngineStubs.StaticDefinitions(null)), support, writer, events);

        var executors = new ArrayList<NodeExecutor>();
        executors.add(agentExecutor);
        executors.addAll(List.of(extra));

        var engine = new WorkflowEngine(
                new EngineStubs.StaticDefinitions(null),
                support,
                new WorkflowScheduler(support, writer),
                new NodeExecutorRegistry(executors),
                agentExecutor,
                writer,
                runs,
                events,
                checkpoints);
        return new Harness(engine, support, runs, events);
    }

    private static ApiDtos.WorkflowDef workflow(String type) {
        var wf = new ApiDtos.WorkflowDef();
        wf.id = "wf-test";
        wf.name = "Lifecycle";
        wf.nodes = List.of(Map.<String, Object>of("id", "n1", "type", type, "title", "n1"));
        wf.edges = List.of();
        return wf;
    }

    @Test
    void failedRunIsDeregistered() {
        Harness h = harness(new EngineStubs.ThrowingExecutor("boom"));
        ApiDtos.WorkflowDef wf = workflow("boom");
        WorkflowRunState run = h.runs().create(wf.id, List.of("n1"), "");

        h.engine().execute(wf, run);

        assertThat(run.status).isEqualTo("failed");
        assertThat(h.support().hasRun(run.runId))
                .as("a failed run must not leak its output registry")
                .isFalse();
    }

    @Test
    void successfulRunIsDeregistered() {
        Harness h = harness();
        ApiDtos.WorkflowDef wf = workflow("start");
        WorkflowRunState run = h.runs().create(wf.id, List.of("n1"), "");

        h.engine().execute(wf, run);

        assertThat(run.status).isEqualTo("completed");
        assertThat(h.support().hasRun(run.runId)).isFalse();
    }

    @Test
    void completeDeferredForInactiveRunFailsLoudly() {
        var runs = new EngineStubs.InMemoryRunPort();
        var writer = new RunStateWriter(runs, new EngineStubs.InMemoryCheckpoints());
        var support = new WorkflowSupport(runs, writer, 50, 200, 1000);
        // The run is persisted (so the port can load it) but is not registered as active.
        WorkflowRunState persisted = runs.create("wf", List.of("n1"), "");
        assertThat(support.hasRun(persisted.runId)).isFalse();

        assertThatThrownBy(() -> support.completeDeferred(persisted.runId, "n1", "late"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no longer active");
    }

    @Test
    void completeDeferredForUnknownRunFailsLoudly() {
        var runs = new EngineStubs.InMemoryRunPort();
        var writer = new RunStateWriter(runs, new EngineStubs.InMemoryCheckpoints());
        var support = new WorkflowSupport(runs, writer, 50, 200, 1000);

        assertThatThrownBy(() -> support.completeDeferred("does-not-exist", "n1", "late"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void completeDeferredForActiveRunWritesOutput() {
        var runs = new EngineStubs.InMemoryRunPort();
        var writer = new RunStateWriter(runs, new EngineStubs.InMemoryCheckpoints());
        var support = new WorkflowSupport(runs, writer, 50, 200, 1000);
        WorkflowRunState run = runs.create("wf", List.of("n1"), "");
        support.registerRun(run.runId, new java.util.concurrent.ConcurrentHashMap<>());

        support.completeDeferred(run.runId, "n1", "value");

        assertThat(runs.load(run.runId).nodeResults)
                .anyMatch(r -> "n1".equals(r.nodeId()) && "completed".equals(r.status()));
    }
}
