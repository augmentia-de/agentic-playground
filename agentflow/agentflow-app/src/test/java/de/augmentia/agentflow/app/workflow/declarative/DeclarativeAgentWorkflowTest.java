package de.augmentia.agentflow.app.workflow.declarative;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class DeclarativeAgentWorkflowTest {

    private static final String QUEUE = "agentflow";

    private TestWorkflowEnvironment env;
    private Fake activities;

    @BeforeEach
    void setUp() {
        env = TestWorkflowEnvironment.newInstance();
    }

    @AfterEach
    void tearDown() {
        if (env != null) {
            env.close();
        }
    }

    private void startWorker(DeclarativePlan plan) {
        Worker worker = env.newWorker(QUEUE);
        worker.registerWorkflowImplementationTypes(DeclarativeAgentWorkflow.Impl.class);
        activities = new Fake(plan);
        worker.registerActivitiesImplementations(activities);
        env.start();
    }

    private DeclarativeAgentWorkflow newWorkflow(String id) {
        return env.getWorkflowClient().newWorkflowStub(DeclarativeAgentWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(QUEUE).setWorkflowId(id).build());
    }

    // ── tests ────────────────────────────────────────────────────────────────

    @Test
    void runsStepsAndWaitsForApproval() {
        startWorker(new DeclarativePlan(List.of(
                agent("a", List.of()), hitl("b", List.of("a")), agent("c", List.of("b")), end("end", List.of("c")))));

        DeclarativeAgentWorkflow wf = newWorkflow("wf-approve");
        WorkflowClient.start(wf::run, "def", "task");
        wf.approve(true);
        String result = WorkflowStub.fromTyped(wf).getResult(String.class);

        assertThat(activities.ran).containsExactly("a", "c");
        assertThat(result).contains("out-c");
    }

    @Test
    void stopsWhenRejected() {
        startWorker(new DeclarativePlan(List.of(
                agent("a", List.of()), hitl("b", List.of("a")), agent("c", List.of("b")), end("end", List.of("c")))));

        DeclarativeAgentWorkflow wf = newWorkflow("wf-reject");
        WorkflowClient.start(wf::run, "def", "task");
        wf.decide("b", false);
        String result = WorkflowStub.fromTyped(wf).getResult(String.class);

        assertThat(result).contains("rejected at node 'b'");
        assertThat(activities.ran).containsExactly("a");
    }

    @Test
    void decisionSelectsBranch() {
        DeclarativePlan plan = new DeclarativePlan(List.of(
                agent("start", List.of()),
                decision("pick", List.of("start"), List.of(new Branch("b1", "contains:expensive", "high")), "low"),
                agent("high", List.of("pick")),
                agent("low", List.of("pick")),
                end("end", List.of("high", "low"))));
        startWorker(plan);
        activities.outputs.put("start", t -> "the item is expensive");

        String result = newWorkflow("wf-decision").run("def", "task");

        assertThat(activities.ran).contains("start", "high");
        assertThat(activities.ran).doesNotContain("low");
        assertThat(result).contains("out-high");
    }

    @Test
    void loopIteratesUntilExitCondition() {
        int[] counter = {0};
        DeclarativePlan plan = new DeclarativePlan(List.of(
                agent("seed", List.of()),
                loop("loop", List.of("seed"), 3, "contains:DONE", List.of("work")),
                agent("work", List.of("loop")),
                end("end", List.of("loop"))));
        startWorker(plan);
        activities.outputs.put("work", t -> ++counter[0] >= 2 ? "DONE" : "not yet");

        newWorkflow("wf-loop").run("def", "task");

        assertThat(activities.ran.stream().filter("work"::equals).count()).isEqualTo(2);
    }

    @Test
    void forkRunsAllBranchesThenJoin() {
        DeclarativePlan plan = new DeclarativePlan(List.of(
                agent("start", List.of()),
                fork("fork", List.of("start"), List.of(new Branch("a", null, "a1"), new Branch("b", null, "b1"))),
                agent("a1", List.of("fork")),
                agent("b1", List.of("fork")),
                join("join", List.of("a1", "b1"), "all", null),
                end("end", List.of("join"))));
        startWorker(plan);

        newWorkflow("wf-fork").run("def", "task");

        assertThat(activities.ran).contains("a1", "b1");
    }

    // ── fakes + plan builders ─────────────────────────────────────────────────

    static class Fake implements DeclarativeActivities {
        final DeclarativePlan plan;
        final List<String> ran = new ArrayList<>();
        final Map<String, Function<String, String>> outputs = new HashMap<>();

        Fake(DeclarativePlan plan) {
            this.plan = plan;
        }

        @Override
        public DeclarativePlan plan(String workflowId) {
            return plan;
        }

        @Override
        public String runStep(DeclarativeStep step, String task) {
            ran.add(step.nodeId());
            Function<String, String> f = outputs.get(step.nodeId());
            return f == null ? "out-" + step.nodeId() : f.apply(task);
        }
    }

    private static NodeSpec agent(String id, List<String> deps) {
        return new NodeSpec(id, "agent", id, "a-" + id, "sys", List.of(), false, false, deps,
                List.of(), null, 1, null, List.of(), "all", null, List.of(), List.of(), List.of());
    }

    private static NodeSpec hitl(String id, List<String> deps) {
        return new NodeSpec(id, "hitl", id, null, null, List.of(), false, true, deps,
                List.of(), null, 1, null, List.of(), "all", null, List.of(), List.of(), List.of());
    }

    private static NodeSpec decision(String id, List<String> deps, List<Branch> branches, String def) {
        return new NodeSpec(id, "decision", id, null, null, List.of(), false, false, deps,
                branches, def, 1, null, List.of(), "all", null, List.of(), List.of(), List.of());
    }

    private static NodeSpec loop(String id, List<String> deps, int max, String exit, List<String> body) {
        return new NodeSpec(id, "loop", id, null, null, List.of(), false, false, deps,
                List.of(), null, max, exit, body, "all", null, List.of(), List.of(), List.of());
    }

    private static NodeSpec fork(String id, List<String> deps, List<Branch> branches) {
        return new NodeSpec(id, "fork", id, null, null, List.of(), false, false, deps,
                branches, null, 1, null, List.of(), "all", null, List.of(), List.of(), List.of());
    }

    private static NodeSpec join(String id, List<String> deps, String type, Integer n) {
        return new NodeSpec(id, "join", id, null, null, List.of(), false, false, deps,
                List.of(), null, 1, null, List.of(), type, n, List.of(), List.of(), List.of());
    }

    private static NodeSpec end(String id, List<String> deps) {
        return new NodeSpec(id, "end", id, null, null, List.of(), false, false, deps,
                List.of(), null, 1, null, List.of(), "all", null, List.of(), List.of(), List.of());
    }
}
