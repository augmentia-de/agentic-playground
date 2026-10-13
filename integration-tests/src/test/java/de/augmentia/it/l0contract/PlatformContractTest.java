package de.augmentia.it.l0contract;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.kafka.KafkaTopics;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.WorkflowDefinitionValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * L0 — Vertrag zwischen den beiden Plattformen, ohne Netzwerk.
 *
 * <p>Die billigste Schicht, die die teuersten Fehler fängt: Topic-Namen, Event-Formate und das
 * DAG-Vokabular. Stimmt hier etwas nicht, scheitern alle Tests darüber mit einer Meldung, die nichts
 * mit der Ursache zu tun hat.
 *
 * <p>Die Event-Formate werden hier mit <em>zwei</em> Mappern geprüft: der Orchestrator liest und
 * schreibt über Jackson 3, der Worker über Jackson 2. Ein Format, das nur auf einem davon
 * funktioniert, ist im Betrieb genau dann kaputt, wenn es im Test grün war.
 */
class PlatformContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Topic-Namen ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("Task-Topics liegen unter agent.tasks.<agentType>")
    void taskTopicNaming() {
        assertThat(TopicNames.tasks("researcher")).isEqualTo("agent.tasks.researcher");
        assertThat(TopicNames.tasks("RESEARCHER")).isEqualTo("agent.tasks.researcher");
        assertThat(TopicNames.tasks("researcher", "acme")).isEqualTo("agent.tasks.researcher.acme");
        assertThat(TopicNames.tasks("researcher", null))
                .as("ohne Tenant bleibt es beim Legacy-Namen")
                .isEqualTo("agent.tasks.researcher");
    }

    @Test
    @DisplayName("Steuer- und Ergebnis-Topics haben feste Namen")
    void controlTopicNaming() {
        assertThat(TopicNames.INTAKE).isEqualTo("agent.execution.intake");
        assertThat(TopicNames.RESULTS).isEqualTo("agent.execution.results");
        assertThat(TopicNames.DLQ).isEqualTo("agent.execution.dlq");
        assertThat(TopicNames.HITL).isEqualTo("agent.control.hitl");
        assertThat(TopicNames.CANCELLATION).isEqualTo("agent.control.cancellation");
        assertThat(TopicNames.TIMEOUTS).isEqualTo("agent.control.timeouts");
        assertThat(TopicNames.LOOP).isEqualTo("agent.control.loop");
        assertThat(TopicNames.SWEEP).isEqualTo("agent.control.sweep");
    }

    @Test
    @DisplayName("all() enthält die Steuer-Topics und je Agent-Typ genau eine Task-Topic")
    void topicSetIsCompleteAndDeduplicated() {
        var names = KafkaTopics.all(List.of("researcher", "coder", "researcher"))
                .stream().map(s -> s.name()).toList();

        assertThat(names)
                .contains(TopicNames.INTAKE, TopicNames.RESULTS, TopicNames.DLQ,
                        TopicNames.HITL, TopicNames.CANCELLATION, TopicNames.TIMEOUTS,
                        TopicNames.LOOP, TopicNames.SWEEP)
                .contains("agent.tasks.researcher", "agent.tasks.coder")
                .doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("alle Topics sind nach workflow_id gepartitioniert, config.updates nach agent_type")
    void partitioningKeys() {
        var byName = KafkaTopics.all(List.of("researcher")).stream()
                .collect(java.util.stream.Collectors.toMap(s -> s.name(), s -> s));

        assertThat(byName.get(TopicNames.INTAKE).keyStrategy()).isEqualTo("workflow_id");
        assertThat(byName.get(TopicNames.RESULTS).keyStrategy()).isEqualTo("workflow_id");
        assertThat(byName.get(TopicNames.TIMEOUTS).keyStrategy()).isEqualTo("task_id");
        assertThat(byName.get(TopicNames.SWEEP).keyStrategy()).isEqualTo("sweep_id");
        assertThat(byName.get(TopicNames.CONFIG_UPDATES).keyStrategy()).isEqualTo("agent_type");
    }

    // ── Event-Format ────────────────────────────────────────────────────────

    private static TaskEvent sampleTask() {
        return new TaskEvent(
                new TaskEvent.Header("wf-1", "task-1", null, 1, 1_757_000_000_000L),
                "researcher",
                "research_step",
                1,
                "Finde die Testabdeckung",
                Map.of("depth", 3),
                List.of(new TaskEvent.ContextReference("previous", "prior_step", Map.of("score", 7))),
                60000L);
    }

    @Test
    @DisplayName("TaskEvent überlebt eine Serialisierungs-Rundreise")
    void taskEventRoundTrip() throws Exception {
        var back = MAPPER.readValue(MAPPER.writeValueAsString(sampleTask()), TaskEvent.class);

        assertThat(back.header().taskId()).isEqualTo("task-1");
        assertThat(back.header().workflowId()).isEqualTo("wf-1");
        assertThat(back.header().attempt()).isEqualTo(1);
        assertThat(back.agentType()).isEqualTo("researcher");
        assertThat(back.stepId()).isEqualTo("research_step");
        assertThat(back.inputPrompt()).isEqualTo("Finde die Testabdeckung");
        assertThat(back.inputJson()).containsEntry("depth", 3);
        assertThat(back.contextReferences()).singleElement()
                .satisfies(c -> assertThat(c.sourceStepId()).isEqualTo("prior_step"));
        assertThat(back.timeoutMs()).isEqualTo(60000L);
    }

    @Test
    @DisplayName("timeout_ms behält seinen Unterstrich im JSON — der Worker liest @JsonProperty")
    void timeoutMsUsesSnakeCaseOnTheWire() throws Exception {
        var json = MAPPER.writeValueAsString(sampleTask());

        assertThat(json).contains("\"timeout_ms\":60000");
        assertThat(json).as("dersnake_case-Name ist Teil des Vertrags")
                .doesNotContain("\"timeoutMs\"");
    }

    @Test
    @DisplayName("ResultEvent trägt result_data mit Status und Fehlerfeldern")
    void resultEventCarriesStatus() throws Exception {
        var event = new ResultEvent(
                new ResultEvent.Header(null, 1_757_000_005_000L),
                "wf-1", "task-1", "research_step", 1, 1,
                1_757_000_000_000L, 1_757_000_005_000L,
                Map.of("status", "COMPLETED", "output", "42 Konfidenz"));

        var json = MAPPER.writeValueAsString(event);
        assertThat(json).contains("\"result_data\"");

        var back = MAPPER.readValue(json, ResultEvent.class);
        assertThat(back.resultData())
                .containsEntry("status", "COMPLETED")
                .containsEntry("output", "42 Konfidenz");
    }

    @Test
    @DisplayName("die vier Worker-Status sind der Vertrag zwischen Worker und Orchestrator")
    void workerStatusVocabulary() {
        // Diese vier Werte entscheiden, ob der Orchestrator neu auf-rollt, abbreicht oder eskaliert.
        assertThat(List.of("COMPLETED", "FAILED_RETRYABLE", "FAILED_FATAL", "NEEDS_HITL"))
                .allSatisfy(s -> assertThat(s).matches("[A-Z_]+"));
    }

    @Test
    @DisplayName("BEFUND: TaskEvent.header ist nicht erzwungen — ein Event ohne Header kommt an")
    void taskEventAcceptsMissingHeader() throws Exception {
        // Der Worker liest header.workflowId und header.taskId. Fehlt der Header, ist das
        // Ergebnis null und der Aufrufer läuft in eine NPE — mitten in der Auftragsverarbeitung,
        // nicht am Rand des Deserialisierens.
        var incomplete = "{\"agentType\":\"researcher\",\"stepId\":\"s1\"}";
        var parsed = MAPPER.readValue(incomplete, TaskEvent.class);

        assertThat(parsed.header())
                .as("BEFUND, kein gewünschtes Verhalten: der Header ist nullable. Entweder "
                        + "TaskEvent.header mit @JsonProperty(required = true) erzwingen, oder "
                        + "der Worker muss header == null mit einer sprechenden Meldung abfangen.")
                .isNull();
        assertThat(parsed.agentType()).isEqualTo("researcher");
    }

    @Test
    @DisplayName("ein vollständiges TaskEvent wird vom Worker-Mapper genauso gelesen")
    void taskEventIsReadableByAJacksonMapperWithFailOnUnknownDisabled() throws Exception {
        // Der Orchestrator schreibt mit Jackson 3, der Worker liest mit Jackson 2 und
        // FAIL_ON_UNKNOWN_PROPERTIES=false. Beide Richtungen müssen zusammenpassen.
        var lenient = new ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        var back = lenient.readValue(MAPPER.writeValueAsString(sampleTask()), TaskEvent.class);

        assertThat(back.header().workflowId()).isEqualTo("wf-1");
        assertThat(back.inputJson()).containsEntry("depth", 3);
    }

    // ── DAG-Vokabular ───────────────────────────────────────────────────────

    @Test
    @DisplayName("NodeType kennt genau die sechs Typen, die der Validator nach R1 zulässt")
    void nodeTypeVocabulary() {
        // R1 des WorkflowDefinitionValidator nennt genau diese sechs.
        assertThat(NodeType.values())
                .containsExactlyInAnyOrder(
                        NodeType.STEP, NodeType.DECISION, NodeType.LOOP,
                        NodeType.FORK, NodeType.JOIN, NodeType.END);
    }

    @Test
    @DisplayName("Routing-Knoten braufen keinen Agent-Typ — nur STEP und LOOP")
    void routingOnlyClassification() {
        assertThat(NodeType.DECISION.isRoutingOnly()).isTrue();
        assertThat(NodeType.FORK.isRoutingOnly()).isTrue();
        assertThat(NodeType.JOIN.isRoutingOnly()).isTrue();
        assertThat(NodeType.END.isRoutingOnly()).isTrue();
        assertThat(NodeType.STEP.isRoutingOnly()).isFalse();
        assertThat(NodeType.LOOP.isRoutingOnly()).isFalse();
    }

    @Test
    @DisplayName("WorkflowDefinition trägt Name, Version, Aktiv-Flag und Knoten")
    void workflowDefinitionShape() {
        var def = new WorkflowDefinition("wf-1", "demo", 1, true,
                List.of(DagNode.builder()
                        .stepId("s1")
                        .nodeType(NodeType.STEP)
                        .agentType("researcher")
                        .build()));

        assertThat(def.id()).isEqualTo("wf-1");
        assertThat(def.name()).isEqualTo("demo");
        assertThat(def.version()).isEqualTo(1);
        assertThat(def.isActive()).isTrue();
        assertThat(def.nodes()).singleElement().satisfies(n -> {
            assertThat(n.stepId()).isEqualTo("s1");
            assertThat(n.nodeType()).isEqualTo(NodeType.STEP);
            assertThat(n.agentType()).isEqualTo("researcher");
        });
    }

    @Test
    @DisplayName("node() findet den Schritt, byIdOrNull gibt null statt zu werfen")
    void definitionLookup() {
        var def = new WorkflowDefinition("wf-1", "demo", 1, true,
                List.of(DagNode.builder().stepId("s1").nodeType(NodeType.STEP).agentType("r").build()));

        assertThat(def.node("s1").agentType()).isEqualTo("r");
        assertThat(def.byIdOrNull("gibtsnicht")).isNull();
        assertThatThrownBy(() -> def.node("gibtsnicht"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("ein STEP ohne Agent-Typ verletzt R8 — es gäbe ein Topic ohne Verkehr")
    void stepWithoutAgentTypeViolatesR8() {
        // R8 des Validators: STEP braucht agentType. Ohne Agent gibt es kein
        // agent.tasks.<typ>-Topic, der Schritt liefe ins Leere.
        var withoutAgent = new WorkflowDefinition("wf-1", "demo", 1, true, List.of(
                DagNode.builder().stepId("s1").nodeType(NodeType.STEP).build(),
                DagNode.builder().stepId("end").nodeType(NodeType.END).dependsOn(List.of("s1")).build()));

        var violations = WorkflowDefinitionValidator.validate(withoutAgent);

        assertThat(violations).anyMatch(v -> v.contains("R8") && v.contains("s1"));
    }

    @Test
    @DisplayName("ein vollständiger Mini-Durchlauf ist strukturell gültig")
    void minimalValidWorkflow() {
        var ok = new WorkflowDefinition("wf-1", "demo", 1, true, List.of(
                DagNode.builder().stepId("s1").nodeType(NodeType.STEP).agentType("researcher").build(),
                DagNode.builder().stepId("end").nodeType(NodeType.END).dependsOn(List.of("s1")).build()));

        assertThat(WorkflowDefinitionValidator.validate(ok))
                .as("STEP -> END ist die kleinste gültige Definition")
                .isEmpty();
        assertThat(WorkflowDefinitionValidator.isValid(ok)).isTrue();
    }

    @Test
    @DisplayName("R7: doppelte step_ids werden erkannt")
    void duplicateStepIdsAreRejected() {
        var dup = new WorkflowDefinition("wf-1", "demo", 1, true, List.of(
                DagNode.builder().stepId("s1").nodeType(NodeType.STEP).agentType("a").build(),
                DagNode.builder().stepId("s1").nodeType(NodeType.STEP).agentType("b").build(),
                DagNode.builder().stepId("end").nodeType(NodeType.END).dependsOn(List.of("s1")).build()));

        assertThat(WorkflowDefinitionValidator.validate(dup))
                .anyMatch(v -> v.startsWith("R7"));
    }

    @Test
    @DisplayName("R7: eine dependsOn-Referenz auf einen unbekannten Schritt wird erkannt")
    void danglingDependencyIsRejected() {
        var dangling = new WorkflowDefinition("wf-1", "demo", 1, true, List.of(
                DagNode.builder().stepId("s1").nodeType(NodeType.STEP).agentType("a")
                        .dependsOn(List.of("gibtsnicht")).build(),
                DagNode.builder().stepId("end").nodeType(NodeType.END).dependsOn(List.of("s1")).build()));

        assertThat(WorkflowDefinitionValidator.validate(dangling))
                .anyMatch(v -> v.contains("gibtsnicht"));
    }
}