package de.augmentia.quad.workflow.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.contract.ApiDtos;
import de.augmentia.quad.core.workflow.run.WorkflowRunState;
import de.augmentia.quad.workflow.engine.context.InputBuilder;
import de.augmentia.quad.workflow.engine.internal.RunStateWriter;
import de.augmentia.quad.workflow.engine.internal.WorkflowSupport;
import de.augmentia.quad.workflow.engine.node.NodeExecutorRegistry;
import de.augmentia.quad.workflow.engine.node.executor.AgentNodeExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Run-event payloads must always be valid JSON, even with user text containing quotes/newlines. */
class WorkflowEngineEventJsonTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void specialCharsProduceValidJson() throws Exception {
        var runs = new EngineStubs.InMemoryRunPort();
        var checkpoints = new EngineStubs.InMemoryCheckpoints();
        var events = new EngineStubs.RecordingEvents();
        var writer = new RunStateWriter(runs, checkpoints);
        var support = new WorkflowSupport(runs, writer, 50, 200, 1000);
        var agentExecutor = new AgentNodeExecutor(new EngineStubs.CountingAgents(),
                new InputBuilder(new EngineStubs.StaticDefinitions(null)), support, writer, events);
        var engine = new WorkflowEngine(
                new EngineStubs.StaticDefinitions(null),
                support,
                new WorkflowScheduler(support, writer),
                new NodeExecutorRegistry(List.of(agentExecutor, new EngineStubs.ThrowingExecutor("boom"))),
                agentExecutor,
                writer,
                runs,
                events,
                checkpoints);

        String nastyName = "Quote\" Backslash\\ Newline\n Tab\t";
        var wf = new ApiDtos.WorkflowDef();
        wf.id = "wf-json";
        wf.name = nastyName;
        wf.nodes = List.of(Map.<String, Object>of("id", "n1", "type", "boom"));
        wf.edges = List.of();
        WorkflowRunState run = runs.create(wf.id, List.of("n1"), "x\"y");

        engine.execute(wf, run);

        String[] started = events.events.stream()
                .filter(e -> "RUN_STARTED".equals(e[0])).findFirst().orElseThrow();
        assertThat(MAPPER.readTree(started[1]).get("workflow").asText()).isEqualTo(nastyName);

        String[] failed = events.events.stream()
                .filter(e -> "RUN_FAILED".equals(e[0])).findFirst().orElseThrow();
        assertThat(MAPPER.readTree(failed[1]).get("error").asText()).isEqualTo("boom");
    }
}
