package de.augmentia.agentplatform.common.routing;

import de.augmentia.agentplatform.common.model.AppAgentType;
import de.augmentia.agentplatform.common.model.CheckpointSnapshot;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden scenarios (SPEC2 §17.2): each checkpointState+dag maps to an expected
 * ordered result. Re-running any scenario must yield an identical result (G3).
 * The primary scenario mirrors the §15.7 reference trace.
 */
class RoutingEvaluatorTest {

    static final String WF = "wf-9876";

    /** Reference DAG per §5.3/§15.7. */
    static WorkflowDefinition referenceDag() {
        return new WorkflowDefinition("wf-def-ref", "reference", 1, true, List.of(
                step("research_step", AppAgentType.RESEARCHER, List.of()),
                DagNode.builder().stepId("quality_gate").nodeType(NodeType.DECISION)
                        .dependsOn(List.of("research_step"))
                        .branches(List.of(DagNode.Branch.decision("refine",
                                "context['research_step'].confidence < 0.8", "refinement_loop")))
                        .defaultTarget("parallel_fork")
                        .build(),
                DagNode.builder().stepId("refinement_loop").nodeType(NodeType.LOOP)
                        .agentType("refiner")
                        .dependsOn(List.of("quality_gate"))
                        .exitCondition("context['refinement_loop'].confidence >= 0.8")
                        .maxIterations(3)
                        .build(),
                DagNode.builder().stepId("parallel_fork").nodeType(NodeType.FORK)
                        .dependsOn(List.of("quality_gate", "refinement_loop"))
                        .branches(List.of(DagNode.Branch.fork("code", "b.code.gen"),
                                DagNode.Branch.fork("test", "b.test.prep")))
                        .build(),
                step("b.code.gen", AppAgentType.CODER, List.of("parallel_fork")),
                step("b.test.prep", AppAgentType.TESTER, List.of("parallel_fork")),
                DagNode.builder().stepId("merge_gate").nodeType(NodeType.JOIN)
                        .joinType("AND").dependsOn(List.of("b.code.gen", "b.test.prep")).build(),
                DagNode.builder().stepId("final_review").nodeType(NodeType.STEP)
                        .agentType(AppAgentType.REVIEWER).requiresHumanApproval(true)
                        .dependsOn(List.of("merge_gate")).build(),
                node("end", NodeType.END, List.of("final_review"))
        ));
    }

    private static DagNode step(String id, String agent, List<String> deps) {
        return node(id, NodeType.STEP, agent, deps);
    }

    private static DagNode node(String id, NodeType t, List<String> deps) {
        return node(id, t, null, deps);
    }

    private static DagNode node(String id, NodeType t, String agent, List<String> deps) {
        return DagNode.builder().stepId(id).nodeType(t).agentType(agent).dependsOn(deps).build();
    }

    private static CheckpointSnapshot cp(String wf, String step, CheckpointStatus st, int it, String branch) {
        return new CheckpointSnapshot(wf, step, null, st, it, branch, null);
    }

    private static Map<String, CheckpointSnapshot> state(CheckpointSnapshot... snapshots) {
        Map<String, CheckpointSnapshot> m = new HashMap<>();
        for (CheckpointSnapshot s : snapshots) m.put(s.stepId(), s);
        return m;
    }

    /* ------------------------- §15.7 trace scenarios ----------------------- */

    @Test
    void step1_startDispatchesResearchOnly() {
        Map<String, Object> ctx = Map.of("research_step", Map.of("confidence", 0.72));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(Map.of(), referenceDag(), ctx, Map.of());

        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("research_step");
        assertThat(r.decisions()).isEmpty();
        assertThat(r.pruned()).isEmpty();
        assertThat(r.workflowEndReached()).isFalse();
    }

    @Test
    void step2_researchCompletedSelectsLoopAndSkipsForkGate() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "research_step", CheckpointStatus.COMPLETED, 1, null));
        Map<String, Object> ctx = Map.of("research_step", Map.of("confidence", 0.72));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, referenceDag(), ctx, Map.of());

        assertThat(r.decisions()).extracting(RoutingEvaluator.Decision::stepId).containsExactly("quality_gate");
        assertThat(r.decisions().get(0).chosenTarget()).isEqualTo("refinement_loop");
        // first loop iteration dispatched; fork stays PENDING (quality_gate edge inactive)
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("refinement_loop");
        assertThat(r.pruned()).isEmpty();
        assertThat(r.completed()).isEmpty();
    }

    @Test
    void step3_loopIterationInFlightBlocksEverything() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "research_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "quality_gate", CheckpointStatus.COMPLETED, 1, "refinement_loop"),
                cp(WF, "refinement_loop", CheckpointStatus.IN_PROGRESS, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, referenceDag(), Map.of(), Map.of());
        assertSettled(r);
    }

    @Test
    void step4_loopExitedFiresForkAndBothBranches() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "research_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "quality_gate", CheckpointStatus.COMPLETED, 1, "refinement_loop"),
                cp(WF, "refinement_loop", CheckpointStatus.COMPLETED, 2, null));
        Map<String, Object> ctx = Map.of("refinement_loop", Map.of("confidence", 0.83));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, referenceDag(), ctx, Map.of());

        assertThat(r.completed()).containsExactly("parallel_fork");
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId)
                .containsExactly("b.code.gen", "b.test.prep");
        assertThat(r.workflowEndReached()).isFalse();
    }

    @Test
    void step5_andJoinWaitsForSecondBranch() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "research_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "quality_gate", CheckpointStatus.COMPLETED, 1, "refinement_loop"),
                cp(WF, "refinement_loop", CheckpointStatus.COMPLETED, 2, null),
                cp(WF, "parallel_fork", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "b.code.gen", CheckpointStatus.IN_PROGRESS, 1, null),
                cp(WF, "b.test.prep", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, referenceDag(), Map.of(), Map.of());

        assertThat(r.completed()).isEmpty(); // merge_gate AND waits for b.code.gen
        assertThat(r.fired()).isEmpty();
    }

    @Test
    void step6_joinCompleteDispatchesReview() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "research_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "quality_gate", CheckpointStatus.COMPLETED, 1, "refinement_loop"),
                cp(WF, "refinement_loop", CheckpointStatus.COMPLETED, 2, null),
                cp(WF, "parallel_fork", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "b.code.gen", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "b.test.prep", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, referenceDag(), Map.of(), Map.of());

        assertThat(r.completed()).containsExactly("merge_gate");
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("final_review");
    }

    @Test
    void step7_reviewDoneReachesEnd() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "research_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "quality_gate", CheckpointStatus.COMPLETED, 1, "refinement_loop"),
                cp(WF, "refinement_loop", CheckpointStatus.COMPLETED, 2, null),
                cp(WF, "parallel_fork", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "b.code.gen", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "b.test.prep", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "merge_gate", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "final_review", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, referenceDag(), Map.of(), Map.of());

        assertThat(r.completed()).containsExactly("end");
        assertThat(r.workflowEndReached()).isTrue();
    }

    /* -------------------------- guard & pruning ---------------------------- */

    private static WorkflowDefinition guardDag() {
        return new WorkflowDefinition("wf-def-guard", "guard", 1, true, List.of(
                DagNode.builder().stepId("optional").nodeType(NodeType.STEP).agentType("qcheck")
                        .guardCondition("context['optional'].enabled == true").build(),
                step("always", "coder", List.of("optional")),
                node("end", NodeType.END, List.of("always"))
        ));
    }

    @Test
    void guardFalseSkipsAndCountsEffective() {
        Map<String, Object> ctx = Map.of("optional", Map.of("enabled", false));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(Map.of(), guardDag(), ctx, Map.of());

        assertThat(r.pruned()).extracting(RoutingEvaluator.Prune::stepId).containsExactly("optional");
        RoutingEvaluator.Prune p = r.pruned().get(0);
        assertThat(p.prune()).isFalse(); // SKIPPED, not PRUNED
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("always");
    }

    @Test
    void guardTrueRunsStep() {
        Map<String, Object> ctx = Map.of("optional", Map.of("enabled", true));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(Map.of(), guardDag(), ctx, Map.of());
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("optional");
    }

    private static WorkflowDefinition xorDag() {
        return new WorkflowDefinition("wf-def-xor", "xor", 1, true, List.of(
                DagNode.builder().stepId("fork").nodeType(NodeType.FORK)
                        .branches(List.of(DagNode.Branch.fork("x", "x"), DagNode.Branch.fork("y", "y"),
                                DagNode.Branch.fork("z", "z"))).build(),
                step("x", "coder", List.of("fork")),
                step("y", "tester", List.of("fork")),
                step("z", "linter", List.of("fork")),
                DagNode.builder().stepId("race_join").nodeType(NodeType.JOIN).joinType("XOR")
                        .dependsOn(List.of("x", "y", "z")).build(),
                step("winner", "quality", List.of("race_join")),
                node("end", NodeType.END, List.of("winner"))
        ));
    }

    @Test
    void xorJoinPrunesLosers() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "fork", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "x", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "y", CheckpointStatus.IN_PROGRESS, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, xorDag(), Map.of(), Map.of());

        assertThat(r.completed()).hasSize(1).contains("race_join");
        assertThat(r.pruned()).filteredOn(x -> x.prune()).extracting(RoutingEvaluator.Prune::stepId)
                .containsExactlyInAnyOrder("y", "z");
        // z is PENDING in state, so it is dispatched in this pass BECAUSE the join had not
        // fired yet (level order) - then the still-in-flight loser is pruned (§15.5 XOR race).
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId)
                .contains("winner");
    }

    /* --------------------------- determinism ------------------------------ */

    @Test
    void replayReturnsIdenticalResult() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "research_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "quality_gate", CheckpointStatus.COMPLETED, 1, "refinement_loop"));
        Map<String, Object> ctx = Map.of("research_step", Map.of("confidence", 0.72));
        RoutingEvaluator.Result a = RoutingEvaluator.evaluate(st, referenceDag(), ctx, Map.of());
        RoutingEvaluator.Result b = RoutingEvaluator.evaluate(st, referenceDag(), ctx, Map.of());

        assertThat(a).isEqualTo(b); // G3
    }

    private static void assertSettled(RoutingEvaluator.Result r) {
        assertThat(r.fired()).isEmpty();
        assertThat(r.decisions()).isEmpty();
        assertThat(r.pruned()).isEmpty();
        assertThat(r.completed()).isEmpty();
        assertThat(r.workflowEndReached()).isFalse();
    }

    /* --------------------------- region loops ----------------------------- */

    /** Five agents in the loop body, then a review step as post-loop consumer. */
    private static WorkflowDefinition regionLoopDag() {
        return new WorkflowDefinition("wf-def-region", "region", 1, true, List.of(
                step("start", AppAgentType.RESEARCHER, List.of()),
                DagNode.builder().stepId("loop").nodeType(NodeType.LOOP)
                        .bodySteps(List.of("a1", "a2", "a3", "a4", "a5"))
                        .maxIterations(3)
                        .exitCondition("context['a5'].done == true")
                        .dependsOn(List.of("start"))
                        .build(),
                step("a1", AppAgentType.CODER, List.of("loop")),
                step("a2", AppAgentType.CODER, List.of("a1")),
                step("a3", AppAgentType.CODER, List.of("a2")),
                step("a4", AppAgentType.CODER, List.of("a3")),
                step("a5", AppAgentType.CODER, List.of("a4")),
                step("after", AppAgentType.REVIEWER, List.of("a5")),
                node("end", NodeType.END, List.of("after"))
        ));
    }

    @Test
    void regionLoopFiresOnlyFirstBodyStep() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopDag(), Map.of(), Map.of());

        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("a1");
        assertThat(r.fired().get(0).iteration()).isEqualTo(1);
        assertThat(r.completed()).doesNotContain("loop");
    }

    @Test
    void regionLoopChainsBodyStepsWithinOneIteration() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a1", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopDag(), Map.of(), Map.of());

        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("a2");
        assertThat(r.fired().get(0).iteration()).isEqualTo(1);
        // 'after' must NOT fire while the loop is still running
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).doesNotContain("after");
    }

    @Test
    void regionLoopRearmsAtNextIterationWhenNotExited() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "loop", CheckpointStatus.IN_PROGRESS, 1, null),
                cp(WF, "a1", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a2", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a3", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a4", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a5", CheckpointStatus.COMPLETED, 1, null));
        Map<String, Object> ctx = Map.of("a5", Map.of("done", false));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopDag(), ctx, Map.of());

        assertThat(r.completed()).doesNotContain("loop");
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("a1");
        assertThat(r.fired().get(0).iteration())
                .as("Re-arm starts the body one iteration further")
                .isEqualTo(2);
    }

    @Test
    void regionLoopCompletesOnExitAndReleasesDownstream() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "loop", CheckpointStatus.IN_PROGRESS, 1, null),
                cp(WF, "a1", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a2", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a3", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a4", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a5", CheckpointStatus.COMPLETED, 1, null));
        Map<String, Object> ctx = Map.of("a5", Map.of("done", true));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopDag(), ctx, Map.of());

        assertThat(r.completed()).contains("loop");
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("after");
    }

    @Test
    void regionLoopStopsAtMaxIterations() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "loop", CheckpointStatus.IN_PROGRESS, 3, null),
                cp(WF, "a1", CheckpointStatus.COMPLETED, 3, null),
                cp(WF, "a2", CheckpointStatus.COMPLETED, 3, null),
                cp(WF, "a3", CheckpointStatus.COMPLETED, 3, null),
                cp(WF, "a4", CheckpointStatus.COMPLETED, 3, null),
                cp(WF, "a5", CheckpointStatus.COMPLETED, 3, null));
        Map<String, Object> ctx = Map.of("a5", Map.of("done", false));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopDag(), ctx, Map.of());

        assertThat(r.completed()).contains("loop");
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("after");
    }

    @Test
    void regionLoopRefiresStaleBodyStepIncrementally() {
        // The engine evaluates after every result: a1 already ran in iteration 2, but
        // a2..a5 still carry their iteration-1 checkpoint. a2 must run again in
        // iteration 2, not be blocked by its stale COMPLETED row.
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a1", CheckpointStatus.COMPLETED, 2, null),
                cp(WF, "a2", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a3", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a4", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a5", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopDag(),
                Map.of("a5", Map.of("done", false)), Map.of());

        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("a2");
        assertThat(r.fired().get(0).iteration()).isEqualTo(2);
    }

    @Test
    void regionLoopIsDeterministic() {
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "loop", CheckpointStatus.IN_PROGRESS, 1, null),
                cp(WF, "a1", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a2", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a3", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a4", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "a5", CheckpointStatus.COMPLETED, 1, null));
        Map<String, Object> ctx = Map.of("a5", Map.of("done", false));
        RoutingEvaluator.Result a = RoutingEvaluator.evaluate(st, regionLoopDag(), ctx, Map.of());
        RoutingEvaluator.Result b = RoutingEvaluator.evaluate(st, regionLoopDag(), ctx, Map.of());
        assertThat(a).isEqualTo(b);
    }

    /* ------------------- control nodes inside a region body ---------------- */

    /** Loop body with FORK -> two STEPs -> JOIN; 'after' attaches directly to the LOOP. */
    private static WorkflowDefinition regionLoopWithForkDag() {
        return new WorkflowDefinition("wf-def-region-fork", "region-fork", 1, true, List.of(
                step("start", AppAgentType.RESEARCHER, List.of()),
                DagNode.builder().stepId("loop").nodeType(NodeType.LOOP)
                        .bodySteps(List.of("fk", "b.hit.hit_step", "b.miss.miss_step", "j"))
                        .maxIterations(2)
                        .exitCondition("context['hit'].good == true")
                        .dependsOn(List.of("start"))
                        .build(),
                DagNode.builder().stepId("fk").nodeType(NodeType.FORK)
                        .dependsOn(List.of("loop"))
                        .branches(List.of(DagNode.Branch.fork("hit", "b.hit.hit_step"),
                                DagNode.Branch.fork("miss", "b.miss.miss_step")))
                        .build(),
                step("b.hit.hit_step", AppAgentType.CODER, List.of("fk")),
                step("b.miss.miss_step", AppAgentType.REVIEWER, List.of("fk")),
                DagNode.builder().stepId("j").nodeType(NodeType.JOIN).joinType("AND")
                        .dependsOn(List.of("b.hit.hit_step", "b.miss.miss_step")).build(),
                step("after", AppAgentType.REVIEWER, List.of("loop")),
                node("end", NodeType.END, List.of("after"))
        ));
    }

    @Test
    void regionLoopFiresForkBranchesPerIteration() {
        // iteration 1 has run completely -> re-arm to 2: the FORK in the body must
        // close again in iteration 2 and fire both branches anew.
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "fk", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "b.hit.hit_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "b.miss.miss_step", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "j", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopWithForkDag(),
                Map.of("hit", Map.of("good", false)), Map.of());

        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId)
                .containsExactlyInAnyOrder("b.hit.hit_step", "b.miss.miss_step");
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::iteration).containsOnly(2);
        assertThat(r.completedSteps())
                .filteredOn(c -> c.stepId().equals("fk"))
                .extracting(RoutingEvaluator.CompletedStep::iteration)
                .as("FORK im Body schliesst erneut in Iteration 2 ab").containsExactly(2);
        assertThat(r.runningLoops()).containsExactly(new RoutingEvaluator.LoopProgress("loop", 2));
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId)
                .as("Consumers outside the region wait for the loop end").doesNotContain("after");
    }

    @Test
    void regionLoopRejoinsForkBodyInNextIteration() {
        // incremental engine state: the JOIN still carries its completion from iteration 1
        // while the branches in iteration 2 are passing through. The JOIN must in iteration 2
        // close it again, otherwise the region hangs permanently.
        Map<String, CheckpointSnapshot> st = state(
                cp(WF, "start", CheckpointStatus.COMPLETED, 1, null),
                cp(WF, "loop", CheckpointStatus.IN_PROGRESS, 2, null),
                cp(WF, "fk", CheckpointStatus.COMPLETED, 2, null),
                cp(WF, "b.hit.hit_step", CheckpointStatus.COMPLETED, 2, null),
                cp(WF, "b.miss.miss_step", CheckpointStatus.COMPLETED, 2, null),
                cp(WF, "j", CheckpointStatus.COMPLETED, 1, null));
        RoutingEvaluator.Result r = RoutingEvaluator.evaluate(st, regionLoopWithForkDag(),
                Map.of("hit", Map.of("good", false)), Map.of());

        assertThat(r.completedSteps())
                .filteredOn(c -> c.stepId().equals("j"))
                .extracting(RoutingEvaluator.CompletedStep::iteration)
                .as("a stale JOIN completion is lifted to the current iteration").containsExactly(2);
        assertThat(r.completedSteps())
                .filteredOn(c -> c.stepId().equals("loop"))
                .extracting(RoutingEvaluator.CompletedStep::iteration)
                .as("maxIterations erreicht -> Loop schliesst in Iteration 2 ab").containsExactly(2);
        assertThat(r.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("after");
        assertThat(r.workflowEndReached())
                .as("END still waits for 'after', which has just been dispatched").isFalse();
    }
}