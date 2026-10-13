package de.augmentia.agentplatform.common.routing;

import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Structural validation per SPEC2 §17.1. */
class WorkflowDefinitionValidatorTest {

    private static DagNode step(String id, String agent, List<String> deps) {
        return DagNode.builder().stepId(id).nodeType(NodeType.STEP).agentType(agent).dependsOn(deps).build();
    }

    private static DagNode end(String id, List<String> deps) {
        return DagNode.builder().stepId(id).nodeType(NodeType.END).dependsOn(deps).build();
    }

    @Test
    void referenceDagIsValid() {
        assertThat(WorkflowDefinitionValidator.validate(RoutingEvaluatorTest.referenceDag())).isEmpty();
        assertThat(WorkflowDefinitionValidator.isValid(RoutingEvaluatorTest.referenceDag())).isTrue();
    }

    @Test
    void rejectsCycleWithoutLoop() {
        WorkflowDefinition dag = new WorkflowDefinition("wf", "cyclic", 1, true, List.of(
                step("a", "x", List.of("b")),
                step("b", "x", List.of("c")),
                step("c", "x", List.of("a")),
                end("end", List.of("a"))
        ));
        List<String> v = WorkflowDefinitionValidator.validate(dag);
        assertThat(v).anyMatch(s -> s.startsWith("R2:"));
    }

    @Test
    void rejectsDecisionMissingDefaultTarget() {
        WorkflowDefinition dag = new WorkflowDefinition("wf", "dec", 1, true, List.of(
                step("a", "x", List.of()),
                DagNode.builder().stepId("dec").nodeType(NodeType.DECISION)
                        .dependsOn(List.of("a"))
                        .branches(List.of(DagNode.Branch.decision("b1", "input['x'] == 'y'", "loop")))
                        .build(),
                DagNode.builder().stepId("loop").nodeType(NodeType.LOOP).agentType("body")
                        .dependsOn(List.of("dec"))
                        .exitCondition("context['loop'].ok == true")
                        .maxIterations(3)
                        .build(),
                end("end", List.of("loop"))
        ));
        List<String> v = WorkflowDefinitionValidator.validate(dag);
        assertThat(v).anyMatch(s -> s.startsWith("R3:"));
    }

    @Test
    void rejectsBranchStepIdConventionViolation() {
        WorkflowDefinition dag = new WorkflowDefinition("wf", "b", 1, true, List.of(
                step("b.in valid", "x", List.of()),
                end("end", List.of("b.in valid"))
        ));
        assertThat(WorkflowDefinitionValidator.validate(dag)).anyMatch(s -> s.startsWith("R5:"));
    }

    @Test
    void rejectsUnknownDependency() {
        WorkflowDefinition dag = new WorkflowDefinition("wf", "dep", 1, true, List.of(
                step("a", "x", List.of("ghost")),
                end("end", List.of("a"))
        ));
        assertThat(WorkflowDefinitionValidator.validate(dag)).anyMatch(s -> s.startsWith("R7:"));
    }

    @Test
    void rejectsLoopWithoutMaxIterations() {
        WorkflowDefinition dag = new WorkflowDefinition("wf", "loop", 1, true, List.of(
                DagNode.builder().stepId("loop").nodeType(NodeType.LOOP).agentType("body")
                        .exitCondition("context['loop'].ok == true").build(),
                end("end", List.of("loop"))
        ));
        assertThat(WorkflowDefinitionValidator.validate(dag)).anyMatch(s -> s.startsWith("R8: LOOP"));
    }

    @Test
    void regionLoopBodyMayContainControlNodes() {
        // STEP and control nodes (FORK/JOIN) are allowed within a region.
        assertThat(WorkflowDefinitionValidator.validate(regionLoopWithControlNodes())).isEmpty();
    }

    @Test
    void rejectsNestedRegionLoopInBody() {
        WorkflowDefinition dag = new WorkflowDefinition("wf", "nested", 1, true, List.of(
                step("start", "x", List.of()),
                DagNode.builder().stepId("outer").nodeType(NodeType.LOOP)
                        .bodySteps(List.of("inner"))
                        .maxIterations(2).exitCondition("true")
                        .dependsOn(List.of("start")).build(),
                DagNode.builder().stepId("inner").nodeType(NodeType.LOOP)
                        .bodySteps(List.of("inner_step"))
                        .maxIterations(2).exitCondition("true")
                        .dependsOn(List.of("outer")).build(),
                step("inner_step", "y", List.of("inner")),
                end("end", List.of("inner_step"))
        ));
        assertThat(WorkflowDefinitionValidator.validate(dag))
                .anyMatch(s -> s.startsWith("R8: LOOP outer bodyStep inner is LOOP"));
    }

    @Test
    void rejectsEndNodeInsideRegionBody() {
        WorkflowDefinition dag = new WorkflowDefinition("wf", "end-in-body", 1, true, List.of(
                step("start", "x", List.of()),
                DagNode.builder().stepId("outer").nodeType(NodeType.LOOP)
                        .bodySteps(List.of("body", "end"))
                        .maxIterations(2).exitCondition("true")
                        .dependsOn(List.of("start")).build(),
                step("body", "y", List.of("outer")),
                end("end", List.of("body"))
        ));
        assertThat(WorkflowDefinitionValidator.validate(dag))
                .anyMatch(s -> s.startsWith("R8: LOOP outer bodyStep end is END"));
    }

    /** start -> LOOP(body: FORK -> zwei Branch-Steps -> JOIN) -> after -> END. */
    private static WorkflowDefinition regionLoopWithControlNodes() {
        return new WorkflowDefinition("wf", "region-control", 1, true, List.of(
                step("start", "x", List.of()),
                DagNode.builder().stepId("loop").nodeType(NodeType.LOOP)
                        .bodySteps(List.of("fk", "b.hit.hit_step", "b.miss.miss_step", "j"))
                        .maxIterations(3).exitCondition("context['loop'].ok == true")
                        .dependsOn(List.of("start")).build(),
                DagNode.builder().stepId("fk").nodeType(NodeType.FORK)
                        .dependsOn(List.of("loop"))
                        .branches(List.of(DagNode.Branch.fork("hit", "b.hit.hit_step"),
                                DagNode.Branch.fork("miss", "b.miss.miss_step")))
                        .build(),
                step("b.hit.hit_step", "y", List.of("fk")),
                step("b.miss.miss_step", "z", List.of("fk")),
                DagNode.builder().stepId("j").nodeType(NodeType.JOIN).joinType("AND")
                        .dependsOn(List.of("b.hit.hit_step", "b.miss.miss_step")).build(),
                step("after", "w", List.of("loop")),
                end("end", List.of("after"))
        ));
    }
}