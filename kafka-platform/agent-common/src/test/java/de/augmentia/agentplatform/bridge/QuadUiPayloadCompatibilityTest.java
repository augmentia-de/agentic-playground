package de.augmentia.agentplatform.bridge;

import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.WorkflowDefinitionValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proof of the compatibility of the format delivered by quad-ui.
 *
 * <p>The UI knows exactly six node types:
 * {@code agent}, {@code conditional}, {@code fork}, {@code join}, {@code loop},
 * {@code nested-workflow} — <strong>no {@code end}</strong>
 * (proven on the canvas in {@code quad-ui/src/components/WorkflowComposer.tsx} and
 * by {@code convertQuadToKafka} in {@code kafkaApi.ts}). A workflow that
 * comes from there, therefore never contains an END node.
 *
 * <p>The orchestrator, in turn, requires an END node when saving
 * ({@code WorkflowDefinitionValidator} rule R6, called in
 * {@code WorkflowDefinitionService.create}). That raises the question of whether a
 * UI workflow reaches the Kafka backend at all — or whether the bridge adds an
 * END node.
 *
 * <p>This class answers that by rebuilding exactly the payload
 * {@code convertQuadToKafka} sends, and passing it through bridge and validator.
 */
@DisplayName("UI payload is compatible with the Kafka backend")
class QuadUiPayloadCompatibilityTest {

    /**
     * Exactly what {@code convertQuadToKafka} sends: flat nodes with the six
     * canvas types, no END node anywhere.
     */
    private static QuarkusWorkflow typicalUiWorkflow() {
        return new QuarkusWorkflow(
                "aus-der-ui",
                List.of(
                        new QuarkusNode("research", "agent", "researcher", "Research", Map.of(), null, null, null),
                        new QuarkusNode("gate", "conditional", null, "Gate", Map.of(), null, null, null),
                        new QuarkusNode("left", "agent", "coder", "Left", Map.of(), null, null, null),
                        new QuarkusNode("right", "agent", "tester", "Right", Map.of(), null, null, null)),
                List.of(
                        new QuarkusEdge("research", "gate", null, Map.of()),
                        new QuarkusEdge("gate", "left", "context['research'].confidence < 0.8", Map.of()),
                        new QuarkusEdge("gate", "right", "context['research'].confidence >= 0.8", Map.of())));
    }

    @Test
    @DisplayName("The bridge adds an END node so that the DAG satisfies rule R6")
    void bridgeAppendsSyntheticEndNode() {
        List<DagNode> converted = WorkflowDefinitionBridge.convert(typicalUiWorkflow());

        assertThat(converted)
                .filteredOn(n -> n.nodeType() == NodeType.END)
                .as("Without an END node WorkflowDefinitionService refuses to save "
                    + "(rule R6) — the bridge must add it because the UI "
                    + "does not offer it")
                .hasSize(1);
    }

    @Test
    @DisplayName("The converted UI workflow passes the orchestrator validation")
    void convertedUiWorkflowPassesValidation() {
        WorkflowDefinition definition = new WorkflowDefinition(
                "id", "aus-der-ui", 1, true,
                WorkflowDefinitionBridge.convert(typicalUiWorkflow()));

        List<String> violations = WorkflowDefinitionValidator.validate(definition);

        assertThat(violations)
                .as("Exactly this check runs in WorkflowDefinitionService.create before saving")
                .isEmpty();
    }

    @Test
    @DisplayName("The supplemented END node attaches to the steps without successors")
    void syntheticEndDependsOnTerminalSteps() {
        List<DagNode> converted = WorkflowDefinitionBridge.convert(typicalUiWorkflow());
        DagNode end = converted.stream()
                .filter(n -> n.nodeType() == NodeType.END)
                .findFirst().orElseThrow();

        assertThat(end.dependsOn())
                .as("The end attaches at the leaves of the DAG, not at the DECISION node")
                .containsExactlyInAnyOrder("left", "right");
    }

    @Test
    @DisplayName("A UI workflow with an explicit END does not get a second one")
    void explicitEndIsNotDuplicated() {
        QuarkusWorkflow withEnd = new QuarkusWorkflow(
                "mit-end",
                List.of(
                        new QuarkusNode("a", "agent", "coder", "A", Map.of(), null, null, null),
                        new QuarkusNode("b", "agent", "tester", "B", Map.of(), null, null, null),
                        new QuarkusNode("done", "end", null, "Done", Map.of(), null, null, null)),
                List.of(
                        new QuarkusEdge("a", "b", null, Map.of()),
                        new QuarkusEdge("b", "done", null, Map.of())));

        List<DagNode> converted = WorkflowDefinitionBridge.convert(withEnd);

        assertThat(converted)
                .filteredOn(n -> n.nodeType() == NodeType.END)
                .as("An already existing END must not be doubled — otherwise "
                    + "would make the graph ambiguous and the validator would reject it")
                .hasSize(1);
    }

    @Test
    @DisplayName("A workflow without edges gets exactly one END")
    void singleNodeWorkflowGetsEnd() {
        QuarkusWorkflow single = new QuarkusWorkflow(
                "single",
                List.of(new QuarkusNode("only", "agent", "coder", "Only", Map.of(), null, null, null)),
                List.of());

        List<DagNode> converted = WorkflowDefinitionBridge.convert(single);

        assertThat(converted).filteredOn(n -> n.nodeType() == NodeType.END).hasSize(1);
        assertThat(WorkflowDefinitionValidator.validate(
                new WorkflowDefinition("id", "single", 1, true, converted))).isEmpty();
    }
}
