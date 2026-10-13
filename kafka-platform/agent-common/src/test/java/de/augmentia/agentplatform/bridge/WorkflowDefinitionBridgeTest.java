package de.augmentia.agentplatform.bridge;

import de.augmentia.agentplatform.common.model.CheckpointSnapshot;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.RoutingEvaluator;
import de.augmentia.agentplatform.common.routing.WorkflowDefinitionValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract between quad-ui and the Kafka orchestrator.
 *
 * <p>Die beiden Formate sind <b>absichtlich verschieden</b>:
 *
 * <table>
 *   <caption>Formate</caption>
 *   <tr><th></th><th>quad-ui (flach)</th><th>Kafka-Orchestrator (verschachtelt)</th></tr>
 *   <tr><td>Struktur</td><td>{@code {nodes[], edges[]}}</td><td>{@code {dag_structure: {nodes[]}}}</td></tr>
 *   <tr><td>Node-ID</td><td>{@code id}</td><td>{@code stepId}</td></tr>
 *   <tr><td>Node-Typ</td><td>{@code type} ({@code agent}, {@code conditional}, …)</td>
 *       <td>{@code nodeType} ({@code STEP}, {@code DECISION}, …)</td></tr>
 *   <tr><td>Zusammenhang</td><td>explizite {@code edges[]}</td><td>{@code dependsOn[]} je Node</td></tr>
 *   <tr><td>Conditions</td><td>on the edge</td><td>on the DECISION branch</td></tr>
 * </table>
 *
 * <p>The test does not answer the question "are the formats still compatible" by
 * comparing the DTOs, but by sending a typical quad flat definition
 * through {@link WorkflowDefinitionBridge}, checking it against the validator
 * and then running it <em>through the real RoutingEvaluator</em>. A conversion
 * that compiles but produces an unusable DAG
 * shows up here.
 *
 * <p>Previously there was not a single test for the bridge.
 */
@DisplayName("Bridge quad-ui -> Kafka-Orchestrator")
class WorkflowDefinitionBridgeTest {

    // ── Fixtures: flat quad format, as the UI delivers it ──

    private static QuarkusNode node(String id, String type, String agentType) {
        return new QuarkusNode(id, type, agentType, id, Map.of(), null, null, null);
    }

    private static QuarkusNode node(String id, String type, String agentType, boolean approval) {
        return new QuarkusNode(id, type, agentType, id, Map.of(), null, null, approval);
    }

    private static QuarkusEdge edge(String source, String target) {
        return new QuarkusEdge(source, target, null, Map.of());
    }

    private static QuarkusEdge conditionalEdge(String source, String target, String condition) {
        return new QuarkusEdge(source, target, condition, Map.of());
    }

    /** condition: research_step, runs through research → gate → a/b. */
    private static QuarkusWorkflow gateWorkflow() {
        return new QuarkusWorkflow("quad-gate", List.of(
                node("research", "agent", "researcher"),
                node("gate", "conditional", null),
                node("a", "agent", "coder"),
                node("b", "agent", "tester"),
                node("end", "end", null)),
                List.of(
                        edge("research", "gate"),
                        conditionalEdge("gate", "a", "context['research'].confidence < 0.8"),
                        conditionalEdge("gate", "b", "context['research'].confidence >= 0.8"),
                        edge("a", "end"),
                        edge("b", "end")));
    }

    // ── Strukturelle Konvertierung ───────────────────────────────────

    @Test
    @DisplayName("id -> stepId and type -> nodeType are translated")
    void idsAndTypesAreTranslated() {
        List<DagNode> converted = WorkflowDefinitionBridge.convert(gateWorkflow());

        assertThat(converted).extracting(DagNode::stepId)
                .containsExactlyInAnyOrder("research", "gate", "a", "b", "end");
        assertThat(converted).filteredOn(n -> n.stepId().equals("research"))
                .singleElement()
                .extracting(DagNode::nodeType).isEqualTo(NodeType.STEP);
        assertThat(converted).filteredOn(n -> n.stepId().equals("gate"))
                .singleElement()
                .extracting(DagNode::nodeType).isEqualTo(NodeType.DECISION);
    }

    @Test
    @DisplayName("Edges are condensed into dependsOn")
    void edgesBecomeDependsOn() {
        List<DagNode> converted = WorkflowDefinitionBridge.convert(gateWorkflow());

        assertThat(converted).filteredOn(n -> n.stepId().equals("gate"))
                .singleElement()
                .extracting(DagNode::dependsOn).asList().containsExactly("research");
        assertThat(converted).filteredOn(n -> n.stepId().equals("a"))
                .singleElement()
                .extracting(DagNode::dependsOn).asList().containsExactly("gate");
    }

    @Test
    @DisplayName("DECISION: conditions move from the edges to the branches")
    void decisionCarriesEdgeConditionsAsBranches() {
        DagNode gate = WorkflowDefinitionBridge.convert(gateWorkflow()).stream()
                .filter(n -> n.stepId().equals("gate"))
                .findFirst().orElseThrow();

        assertThat(gate.branches()).hasSize(2);
        assertThat(gate.branches()).extracting(DagNode.Branch::condition)
                .containsExactlyInAnyOrder(
                        "context['research'].confidence < 0.8",
                        "context['research'].confidence >= 0.8");
        assertThat(gate.branches()).allSatisfy(b -> assertThat(b.target()).isNotBlank());
    }

    @Test
    @DisplayName("DECISION: Branch-IDs sind positionsbasiert (branch-0, branch-1)")
    void branchIdsArePositional() {
        DagNode gate = WorkflowDefinitionBridge.convert(gateWorkflow()).stream()
                .filter(n -> n.stepId().equals("gate"))
                .findFirst().orElseThrow();

        // Positional, not taken over from the source document: the bridge
        // invents the IDs. When re-sorting the edges in the editor, the
        // assignment branch ID <-> target changes with it. For the persistence of the
        // branchId (see RoutingEvaluator.Decision) that means: the ID is not
        // stable for UI workflows.
        assertThat(gate.branches()).extracting(DagNode.Branch::branchId)
                .containsExactlyInAnyOrder("branch-0", "branch-1");
    }

    @Test
    @DisplayName("FORK and JOIN are translated")
    void forkAndJoinAreTranslated() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-fork", List.of(
                node("start", "agent", "researcher"),
                node("fan", "fork", null),
                node("x", "agent", "coder"),
                node("y", "agent", "tester"),
                node("join", "join", null)),
                List.of(
                        edge("start", "fan"),
                        edge("fan", "x"),
                        edge("fan", "y"),
                        edge("x", "join"),
                        edge("y", "join")));

        List<DagNode> converted = WorkflowDefinitionBridge.convert(wf);

        assertThat(converted).filteredOn(n -> n.stepId().equals("fan"))
                .singleElement().satisfies(n -> {
                    assertThat(n.nodeType()).isEqualTo(NodeType.FORK);
                    assertThat(n.branches()).hasSize(2);
                });
        assertThat(converted).filteredOn(n -> n.stepId().equals("join"))
                .singleElement()
                .extracting(DagNode::nodeType).isEqualTo(NodeType.JOIN);
    }

    @Test
    @DisplayName("An unconditional edge becomes the defaultTarget")
    void unconditionalEdgeBecomesDefaultTarget() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-default", List.of(
                node("s", "agent", "researcher"),
                node("g", "conditional", null),
                node("maybe", "agent", "coder"),
                node("always", "agent", "tester")),
                List.of(
                        edge("s", "g"),
                        conditionalEdge("g", "maybe", "context['s'].confidence < 0.8"),
                        edge("g", "always")));

        DagNode gate = WorkflowDefinitionBridge.convert(wf).stream()
                .filter(n -> n.stepId().equals("g"))
                .findFirst().orElseThrow();

        assertThat(gate.defaultTarget())
                .as("The first edge without a condition is the default")
                .isEqualTo("always");
    }

    @Test
    @DisplayName("nested-workflow is rejected instead of being silently skipped")
    void nestedWorkflowIsRejected() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-nested", List.of(
                node("s", "agent", "researcher"),
                node("sub", "nested-workflow", "coder")),
                List.of(edge("s", "sub")));

        assertThatThrownBy(() -> WorkflowDefinitionBridge.convert(wf))
                .as("A node that cannot be mapped must not disappear silently")
                .isInstanceOf(WorkflowConversionException.class)
                .hasMessageContaining("nested-workflow")
                .hasMessageContaining("sub");
    }

    @Test
    @DisplayName("An unknown node type is rejected instead of being guessed as STEP")
    void unknownNodeTypeIsRejected() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-unknown", List.of(
                node("s", "agent", "researcher"),
                node("mystery", "time-travel", "coder")),
                List.of(edge("s", "mystery")));

        assertThatThrownBy(() -> WorkflowDefinitionBridge.convert(wf))
                .isInstanceOf(WorkflowConversionException.class)
                .hasMessageContaining("time-travel")
                .hasMessageContaining("mystery");
    }

    // ── LOOP: the body becomes a region ────────────────────────────────────

    private static QuarkusNode loopNode(String id) {
        return new QuarkusNode(id, "loop", null, id,
                Map.of("maxIterations", 3, "exitCondition", "iter>=3"), null, null, null);
    }

    @Test
    @DisplayName("LOOP: downstream body becomes the bodySteps region and passes the validator")
    void loopBodyBecomesRegion() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-loop", List.of(
                node("start", "agent", "researcher"),
                loopNode("loop"),
                node("body", "agent", "coder")),
                List.of(
                        edge("start", "loop"),
                        edge("loop", "body")));

        List<DagNode> converted = WorkflowDefinitionBridge.convert(wf);
        DagNode loop = converted.stream()
                .filter(n -> n.stepId().equals("loop")).findFirst().orElseThrow();

        assertThat(loop.isRegionLoop())
                .as("A quad LOOP iterates its body region, not a single agent")
                .isTrue();
        assertThat(loop.bodySteps()).containsExactly("body");
        assertThat(WorkflowDefinitionValidator.validate(
                new WorkflowDefinition("id", "quad-loop", 1, true, converted)))
                .as("A region with exactly one body step must be valid")
                .isEmpty();
    }

    @Test
    @DisplayName("LOOP: multiple agents in the body form a region")
    void loopWithMultipleBodyAgentsBecomesRegion() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-loop-multi", List.of(
                node("start", "agent", "researcher"),
                loopNode("loop"),
                node("a", "agent", "coder"),
                node("b", "agent", "tester")),
                List.of(
                        edge("start", "loop"),
                        edge("loop", "a"),
                        edge("loop", "b")));

        List<DagNode> converted = WorkflowDefinitionBridge.convert(wf);
        DagNode loop = converted.stream()
                .filter(n -> n.stepId().equals("loop")).findFirst().orElseThrow();

        assertThat(loop.bodySteps())
                .as("Mehrere Agenten im Loop-Body sind der eigentliche Zweck der Region")
                .containsExactly("a", "b");
        assertThat(WorkflowDefinitionValidator.validate(
                new WorkflowDefinition("id", "quad-loop-multi", 1, true, converted)))
                .isEmpty();
    }

    @Test
    @DisplayName("LOOP: a chain of five agents becomes the region completely")
    void loopBodyCanHoldManyAgents() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-loop-five", List.of(
                node("start", "agent", "researcher"),
                loopNode("loop"),
                node("a1", "agent", "agent1"),
                node("a2", "agent", "agent2"),
                node("a3", "agent", "agent3"),
                node("a4", "agent", "agent4"),
                node("a5", "agent", "agent5")),
                List.of(
                        edge("start", "loop"),
                        edge("loop", "a1"),
                        edge("a1", "a2"),
                        edge("a2", "a3"),
                        edge("a3", "a4"),
                        edge("a4", "a5")));

        List<DagNode> converted = WorkflowDefinitionBridge.convert(wf);
        DagNode loop = converted.stream()
                .filter(n -> n.stepId().equals("loop")).findFirst().orElseThrow();

        assertThat(loop.bodySteps()).containsExactly("a1", "a2", "a3", "a4", "a5");
        assertThat(WorkflowDefinitionValidator.validate(
                new WorkflowDefinition("id", "quad-loop-five", 1, true, converted)))
                .isEmpty();
    }

    @Test
    @DisplayName("LOOP without a reachable body is rejected")
    void loopWithoutBodyIsRejected() {
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-loop-empty", List.of(
                node("start", "agent", "researcher"),
                loopNode("loop")),
                List.of(edge("start", "loop")));

        assertThatThrownBy(() -> WorkflowDefinitionBridge.convert(wf))
                .isInstanceOf(WorkflowConversionException.class)
                .hasMessageContaining("has no body");
    }

    @Test
    @DisplayName("LOOP: loopTargetId delimits the region and leaves the rest behind it")
    void loopTargetIdNarrowsRegion() {
        QuarkusNode anchored = new QuarkusNode("loop", "loop", null, "loop",
                Map.of("maxIterations", 3, "exitCondition", "iter>=3", "loopTargetId", "b1"),
                null, null, null);
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-loop-anchor", List.of(
                node("start", "agent", "researcher"),
                anchored,
                node("b1", "agent", "agent1"),
                node("b2", "agent", "agent2"),
                node("after", "agent", "finalizer")),
                List.of(
                        edge("start", "loop"),
                        edge("loop", "b1"),
                        edge("b1", "b2"),
                        edge("b2", "after"),
                        edge("after", "loop"))); // back edge closes the body

        List<DagNode> converted = WorkflowDefinitionBridge.convert(wf);
        DagNode loop = converted.stream()
                .filter(n -> n.stepId().equals("loop")).findFirst().orElseThrow();

        assertThat(loop.bodySteps())
                .as("loopTargetId narrows the region to the range from 'b1' on (incl. body sink)")
                .containsExactly("b1", "b2", "after");
        assertThat(loop.dependsOn())
                .as("The back edge from 'after' is read as iteration, not as dependency")
                .containsExactly("start");
        assertThat(WorkflowDefinitionValidator.validate(
                new WorkflowDefinition("id", "quad-loop-anchor", 1, true, converted)))
                .isEmpty();
    }

    // ── Der eigentliche Konformitaetsbeweis ───────────────────────────

    @Test
    @DisplayName("The converted DAG passes the orchestrator validator")
    void convertedDagPassesValidation() {
        var problems = WorkflowDefinitionValidator.validate(
                new WorkflowDefinition(
                        "id", "quad-gate", 1, true,
                        WorkflowDefinitionBridge.convert(gateWorkflow())));

        assertThat(problems)
                .as("The bridge must not produce an invalid DAG")
                .isEmpty();
    }

    @Test
    @DisplayName("The converted DAG routes through the real evaluator")
    void convertedDagRoutesCorrectly() {
        var definition = new WorkflowDefinition(
                "id", "quad-gate", 1, true,
                WorkflowDefinitionBridge.convert(gateWorkflow()));

        // Hohe Confidence -> Zweig "b" (>= 0.8)
        Map<String, CheckpointSnapshot> state = new LinkedHashMap<>();
        Map<String, Object> context = Map.of("research", Map.of("confidence", 0.95d));
        RoutingEvaluator.Result first = RoutingEvaluator.evaluate(state, definition, context, Map.of());
        assertThat(first.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("research");

        // complete research and evaluate again
        state.put("research", snapshot("research"));
        context = Map.of("research", Map.of("confidence", 0.95d));
        RoutingEvaluator.Result second = RoutingEvaluator.evaluate(state, definition, context, Map.of());

        assertThat(second.decisions()).as("The condition >= 0.8 must match").hasSize(1);
        assertThat(second.decisions().get(0).chosenTarget()).isEqualTo("b");
        assertThat(second.decisions().get(0).branchId())
                .as("The branch ID is now part of the decisions and is persisted")
                .isNotBlank();
        assertThat(second.fired()).extracting(RoutingEvaluator.Fire::stepId).containsExactly("b");
    }

    @Test
    @DisplayName("Low confidence takes the other branch")
    void lowConfidenceTakesOtherBranch() {
        var definition = new WorkflowDefinition(
                "id", "quad-gate", 1, true,
                WorkflowDefinitionBridge.convert(gateWorkflow()));

        Map<String, CheckpointSnapshot> state = new LinkedHashMap<>();
        state.put("research", snapshot("research"));
        Map<String, Object> context = Map.of("research", Map.of("confidence", 0.2d));

        RoutingEvaluator.Result result = RoutingEvaluator.evaluate(state, definition, context, Map.of());

        assertThat(result.decisions()).hasSize(1);
        assertThat(result.decisions().get(0).chosenTarget()).isEqualTo("a");
    }

    @Test
    @DisplayName("The bridge conditions use the same context path as the others")
    void bridgeConditionsUseTheSameContextShape() {
        // The bridge passes the condition through unchanged. That means the
        // context path matches that of the delivered workflows: flat, without
        // intermediate levels. Had the bridge inserted a "resultData" level,
        // their conditions would never be satisfiable — the same defect as with the
        // Seed-Workflows.
        DagNode gate = WorkflowDefinitionBridge.convert(gateWorkflow()).stream()
                .filter(n -> n.stepId().equals("gate"))
                .findFirst().orElseThrow();

        assertThat(gate.branches()).allSatisfy(b ->
                assertThat(b.condition())
                        .as("No intermediate level 'resultData' — it does not exist in the context")
                        .doesNotContain("resultData"));
    }

    @Test
    @DisplayName("DECISION without defaultTarget stays stuck when no condition matches")
    void decisionWithoutDefaultTargetStalls() {
        // Documented behaviour, not a test of the bridge: the evaluator chooses
        // null when no condition matches and no defaultTarget is set.
        // Die Bridge erzeugt allerdings immer einen Default (Rueckfall auf die
        // first edge) — this test pins down that this is a fallback and
        // not a real null.
        QuarkusWorkflow wf = new QuarkusWorkflow("quad-nodeny", List.of(
                node("s", "agent", "researcher"),
                node("g", "conditional", null),
                node("only", "agent", "coder")),
                List.of(
                        edge("s", "g"),
                        conditionalEdge("g", "only", "context['s'].confidence < 0.1")));

        DagNode gate = WorkflowDefinitionBridge.convert(wf).stream()
                .filter(n -> n.stepId().equals("g"))
                .findFirst().orElseThrow();

        assertThat(gate.defaultTarget())
                .as("Fallback to the first edge, even if it carries a condition — "
                    + "a wrong condition thus still leads to executing this branch")
                .isEqualTo("only");
    }

    private static CheckpointSnapshot snapshot(String stepId) {
        return new CheckpointSnapshot("wf", stepId, NodeType.STEP,
                CheckpointStatus.COMPLETED, 1, null, null);
    }

    // ── The format quad's own workflow generator produces ──────────────

    /** Node as {@code WorkflowGeneratorService} writes it: binding and timeout in {@code config}. */
    private static QuarkusNode generatedNode(String id, String agentId, Integer timeoutSeconds) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("agentId", agentId);
        config.put("tools", List.of("grep_search"));
        if (timeoutSeconds != null) config.put("timeoutSeconds", timeoutSeconds);
        return new QuarkusNode(id, "agent", null, id, Map.of(), config, null, null, null);
    }

    @Test
    @DisplayName("config.agentId becomes the agent type — not the silent 'generic' fallback")
    void agentTypeIsTakenFromConfig() {
        var nodes = WorkflowDefinitionBridge.convert(new QuarkusWorkflow("gen", List.of(
                generatedNode("node-Researcher", "agent-1a2b3c4d", 30),
                generatedNode("node-Report", "agent-9f8e7d6c", null)), List.of()));

        assertThat(nodes).filteredOn(n -> n.nodeType() == NodeType.STEP)
                .extracting(DagNode::agentType)
                .containsExactly("agent-1a2b3c4d", "agent-9f8e7d6c");
    }

    @Test
    @DisplayName("timeoutSeconds from config becomes timeoutMs")
    void timeoutIsTakenFromConfig() {
        var nodes = WorkflowDefinitionBridge.convert(new QuarkusWorkflow("gen", List.of(
                generatedNode("node-Researcher", "agent-1a2b3c4d", 30)), List.of()));

        assertThat(nodes).filteredOn(n -> n.nodeType() == NodeType.STEP)
                .extracting(DagNode::timeoutMs)
                .containsExactly(30_000);
    }

    @Test
    @DisplayName("properties win over config when both carry the same attribute")
    void propertiesWinOverConfig() {
        Map<String, Object> config = Map.of("agentId", "from-config", "timeoutSeconds", 30);
        Map<String, Object> properties = Map.of("agentId", "from-properties");
        var node = new QuarkusNode("a", "agent", null, "a", properties, config, null, null, null);

        var nodes = WorkflowDefinitionBridge.convert(new QuarkusWorkflow("gen", List.of(node), List.of()));

        assertThat(nodes).filteredOn(n -> n.nodeType() == NodeType.STEP)
                .extracting(DagNode::agentType)
                .containsExactly("from-properties");
    }

    @Test
    @DisplayName("a node without any agent binding still converts, so the log can name it")
    void missingAgentFallsBackToGeneric() {
        var nodes = WorkflowDefinitionBridge.convert(new QuarkusWorkflow("gen",
                List.of(new QuarkusNode("a", "agent", null, "a", Map.of(), Map.of(), null, null, null)),
                List.of()));

        assertThat(nodes).filteredOn(n -> n.nodeType() == NodeType.STEP)
                .extracting(DagNode::agentType)
                .containsExactly("generic");
    }

    @Test
    @DisplayName("a blank top-level agentType does not win over config.agentId")
    void blankAgentTypeDoesNotShadowConfig() {
        var node = new QuarkusNode("a", "agent", "  ", "a", Map.of(),
                Map.of("agentId", "agent-from-config"), null, null, null);

        var nodes = WorkflowDefinitionBridge.convert(new QuarkusWorkflow("gen", List.of(node), List.of()));

        assertThat(nodes).filteredOn(n -> n.nodeType() == NodeType.STEP)
                .extracting(DagNode::agentType)
                .containsExactly("agent-from-config");
    }
}
