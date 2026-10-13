package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.CheckpointSnapshot;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.RoutingEvaluator;
import de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowRoutingScenarioTest {

    @Test
    void referenceWorkflowUsesLoopForkJoinAndEnd() throws Exception {
        WorkflowDefinition dag = load("workflows/reference-code-quality.json");
        Simulation sim = new Simulation(Map.of());

        RoutingEvaluator.Result first = sim.advance(dag);
        assertThat(first.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("research_step");

        sim.complete("research_step", Map.of("confidence", 0.72d));
        RoutingEvaluator.Result loopEntered = sim.advance(dag);
        assertThat(loopEntered.decisions()).extracting(RoutingEvaluator.Decision::chosenTarget)
                .containsExactly("refinement_loop");
        assertThat(loopEntered.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("refinement_loop");

        sim.complete("refinement_loop", Map.of("confidence", 0.84d));
        RoutingEvaluator.Result forked = sim.advance(dag);
        assertThat(forked.completed()).contains("parallel_fork");
        assertThat(forked.fired()).extracting(RoutingEvaluator.Fire::stepId)
                .containsExactly("b.code.gen", "b.test.prep");

        sim.complete("b.code.gen", Map.of("artifact", "code"));
        sim.complete("b.test.prep", Map.of("artifact", "tests"));
        RoutingEvaluator.Result joined = sim.advance(dag);
        assertThat(joined.completed()).contains("merge_gate");
        assertThat(joined.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("final_review");

        sim.complete("final_review", Map.of("approved", true));
        RoutingEvaluator.Result ended = sim.advance(dag);
        assertThat(ended.workflowEndReached()).isTrue();
    }

    @Test
    void errorTaxonomySkipsUnchosenBranchesAndConverges() throws Exception {
        WorkflowDefinition dag = load("workflows/error-taxonomy.json");
        Simulation sim = new Simulation(Map.of());

        sim.advance(dag);
        sim.complete("trigger", Map.of("mode", "retryable"));
        RoutingEvaluator.Result decided = sim.advance(dag);

        assertThat(decided.decisions()).extracting(RoutingEvaluator.Decision::chosenTarget)
                .containsExactly("b.retryable.task");
        assertThat(decided.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("b.retryable.task");
        assertThat(decided.pruned()).extracting(RoutingEvaluator.Prune::stepId)
                .contains("b.fatal.task", "b.hitl.task")
                .doesNotContain("b.retryable.task", "b.retryable.verify");

        sim.complete("b.retryable.task", Map.of("fixed", true));
        RoutingEvaluator.Result verifyFired = sim.advance(dag);
        assertThat(verifyFired.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("b.retryable.verify");

        sim.complete("b.retryable.verify", Map.of("verified", true));
        RoutingEvaluator.Result collectFired = sim.advance(dag);
        assertThat(collectFired.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("collect");

        sim.complete("collect", Map.of("done", true));
        assertThat(sim.advance(dag).workflowEndReached()).isTrue();
    }

    @Test
    void xorJoinPrunesLosers() throws Exception {
        WorkflowDefinition dag = load("workflows/xor-race.json");
        Simulation sim = new Simulation(Map.of());

        sim.advance(dag);
        sim.complete("plan", Map.of("ready", true));
        RoutingEvaluator.Result forked = sim.advance(dag);
        assertThat(forked.fired()).extracting(RoutingEvaluator.Fire::stepId)
                .containsExactly("b.fast.build", "b.slow.build");

        sim.complete("b.fast.build", Map.of("winner", true));
        RoutingEvaluator.Result joined = sim.advance(dag);
        assertThat(joined.completed()).contains("race_join");
        assertThat(joined.pruned()).extracting(RoutingEvaluator.Prune::stepId).contains("b.slow.build");
        assertThat(joined.fired()).extracting(RoutingEvaluator.Fire::stepId).contains("publish_winner");
    }

    @Test
    void nOfJoinFiresAfterQuorum() throws Exception {
        WorkflowDefinition dag = load("workflows/n-of-quorum.json");
        Simulation sim = new Simulation(Map.of());

        sim.advance(dag);
        sim.complete("prepare", Map.of("ready", true));
        sim.advance(dag);

        sim.complete("b.alpha.review", Map.of("vote", "yes"));
        RoutingEvaluator.Result afterOne = sim.advance(dag);
        assertThat(afterOne.completed()).doesNotContain("quorum_join");

        sim.complete("b.beta.review", Map.of("vote", "yes"));
        RoutingEvaluator.Result afterTwo = sim.advance(dag);
        assertThat(afterTwo.completed()).contains("quorum_join");
        assertThat(afterTwo.pruned()).extracting(RoutingEvaluator.Prune::stepId).contains("b.gamma.review");
        assertThat(afterTwo.fired()).extracting(RoutingEvaluator.Fire::stepId).contains("summarize");
    }

    @Test
    void guardSkipKeepsFlowMoving() throws Exception {
        WorkflowDefinition dag = load("workflows/guard-skip.json");
        Simulation sim = new Simulation(Map.of());

        sim.advance(dag);
        sim.complete("prepare", Map.of("runLint", false));
        RoutingEvaluator.Result afterGuard = sim.advance(dag);

        assertThat(afterGuard.pruned()).extracting(RoutingEvaluator.Prune::stepId).containsExactly("optional_lint");
        assertThat(afterGuard.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("package_output");

        sim.complete("package_output", Map.of("packaged", true));
        assertThat(sim.advance(dag).workflowEndReached()).isTrue();
    }

    private WorkflowDefinition load(String resource) throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resource)) {
            assertThat(input).isNotNull();
            String json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            WorkflowDefinitionUpload upload = EventJson.read(json, WorkflowDefinitionUpload.class);
            return upload.toDefinition(UUID.randomUUID().toString(), upload.version() == null ? 1 : upload.version(), true);
        }
    }

    private static final class Simulation {
        private final Map<String, CheckpointSnapshot> state = new HashMap<>();
        private final Map<String, Object> context = new LinkedHashMap<>();
        private final Map<String, Object> input;

        private Simulation(Map<String, Object> input) {
            this.input = input;
        }

        private RoutingEvaluator.Result advance(WorkflowDefinition dag) {
            RoutingEvaluator.Result result = RoutingEvaluator.evaluate(state, dag, context, input);
            for (RoutingEvaluator.Decision decision : result.decisions()) {
                state.put(decision.stepId(), snapshot(decision.stepId(), CheckpointStatus.COMPLETED, decision.chosenTarget()));
            }
            for (String stepId : result.completed()) {
                state.put(stepId, snapshot(stepId, CheckpointStatus.COMPLETED, null));
            }
            for (RoutingEvaluator.Prune prune : result.pruned()) {
                CheckpointStatus status = prune.prune() ? CheckpointStatus.PRUNED : CheckpointStatus.SKIPPED;
                state.put(prune.stepId(), snapshot(prune.stepId(), status, prune.reason()));
            }
            for (RoutingEvaluator.Fire fire : result.fired()) {
                state.put(fire.stepId(), snapshot(fire.stepId(), CheckpointStatus.IN_PROGRESS, null));
            }
            return result;
        }

        /**
         * Stores the context <em>exactly</em> as the orchestrator does in production.
         *
         * <p>{@code JdbcCheckpointStore.outputContext} builds
         * {@code context[stepId] = <output_payload>}, and {@code output_payload} is
         * the <em>flat</em> {@code result_data} of the worker. There is therefore
         * <strong>no</strong> intermediate level {@code resultData} — the valid path to
         * a field is {@code context['step'].field}.
         *
         * <p>This simulation previously created {@code {resultData: ...}}. That matched
         * the shipped workflows and their tests, but not the
         * production: the guard, the DECISION condition and the LOOP exit condition
         * would never have been satisfied in real operation and all branches would have
         * landed on {@code defaultTarget}. The contract is now pinned down in
         * {@code ExpressionEvaluatorContextContractTest}.
         */
        private void complete(String stepId, Map<String, Object> resultData) {
            state.put(stepId, snapshot(stepId, CheckpointStatus.COMPLETED, null));
            context.put(stepId, resultData);
        }

        private CheckpointSnapshot snapshot(String stepId, CheckpointStatus status, String branch) {
            CheckpointSnapshot existing = state.get(stepId);
            return new CheckpointSnapshot(
                    "wf-sim",
                    stepId,
                    existing == null ? null : existing.nodeType(),
                    status,
                    existing == null ? 1 : existing.iteration(),
                    branch,
                    existing == null ? null : existing.agentType()
            );
        }
    }
}
