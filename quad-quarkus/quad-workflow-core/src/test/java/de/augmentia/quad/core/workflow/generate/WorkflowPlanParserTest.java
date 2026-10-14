package de.augmentia.quad.core.workflow.generate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the shared, single-sourced planner-reply parser.
 *
 * <p>Both the quad and the agentflow dynamic workflow generators delegate here, so this suite is the
 * regression guard for the parsing/validation behaviour that used to be duplicated (and could drift)
 * in two places (Review A1/B6).
 */
class WorkflowPlanParserTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void parsesStepsAndMeta() {
        String json = """
            {"workflowName":"Research","description":"desc","steps":[
              {"agentRole":"Scout","instruction":"gather","tools":["webSearch"],"dependsOn":[],"timeoutSeconds":60},
              {"agentRole":"Writer","instruction":"write","tools":[],"dependsOn":["Scout"],
               "data":{"output":{"type":"object"}}}
            ]}""";
        var r = WorkflowPlanParser.parseSteps(mapper, json);

        assertEquals("Research", r.workflowName());
        assertEquals("desc", r.description());
        assertEquals(json, r.raw());
        assertEquals(2, r.steps().size());
        assertEquals(60, r.steps().get(0).timeoutSeconds());
        assertEquals(List.of("webSearch"), r.steps().get(0).tools());
        assertEquals(List.of("Scout"), r.steps().get(1).dependsOn());
        assertEquals(120, r.steps().get(1).timeoutSeconds(), "default timeout must be 120");
        assertEquals(Map.of("output", Map.of("type", "object")), r.steps().get(1).data());
    }

    @Test
    void missingStepsArrayRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkflowPlanParser.parseSteps(mapper, "{\"x\":1}"));
    }

    @Test
    void invalidJsonRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkflowPlanParser.parseSteps(mapper, "{not json"));
    }

    @Test
    void emptyStepsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkflowPlanParser.parseSteps(mapper, "{\"steps\":[]}"));
    }

    @Test
    void blankRoleRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkflowPlanParser.parseSteps(mapper, "{\"steps\":[{\"instruction\":\"x\"}]}"));
    }

    @Test
    void unknownDependencyRejected() {
        String json = "{\"steps\":[{\"agentRole\":\"A\",\"instruction\":\"a\",\"dependsOn\":[\"missing\"]}]}";
        assertThrows(IllegalArgumentException.class, () -> WorkflowPlanParser.parseSteps(mapper, json));
    }

    @Test
    void cycleRejected() {
        String json = "{\"steps\":[{\"agentRole\":\"A\",\"dependsOn\":[\"B\"]},"
                + "{\"agentRole\":\"B\",\"dependsOn\":[\"A\"]}]}";
        assertThrows(IllegalArgumentException.class, () -> WorkflowPlanParser.parseSteps(mapper, json));
    }

    @Test
    void duplicateRoleRejected() {
        String json = "{\"steps\":[{\"agentRole\":\"A\",\"instruction\":\"1\"},"
                + "{\"agentRole\":\"A\",\"instruction\":\"2\"}]}";
        assertThrows(IllegalArgumentException.class, () -> WorkflowPlanParser.parseSteps(mapper, json));
    }

    @Test
    void forkMergePlanParses() {
        // Parallel fork (Left, Right) merged by a step listing both predecessors.
        String json = """
            {"steps":[
              {"agentRole":"Root"},
              {"agentRole":"Left","dependsOn":["Root"]},
              {"agentRole":"Right","dependsOn":["Root"]},
              {"agentRole":"Merge","dependsOn":["Left","Right"]}
            ]}""";
        var r = WorkflowPlanParser.parseSteps(mapper, json);
        assertEquals(4, r.steps().size());
        assertEquals(List.of("Left", "Right"), r.steps().get(3).dependsOn());
    }

    @Test
    void stripFencesVariants() {
        assertEquals("{\"a\":1}", WorkflowPlanParser.stripFences("```json\n{\"a\":1}\n```"));
        assertEquals("{\"a\":1}", WorkflowPlanParser.stripFences("Here you go:\n{\"a\":1}\nDone."));
        assertEquals("{\"a\":1}", WorkflowPlanParser.stripFences("json\n{\"a\":1}"));
        assertEquals("", WorkflowPlanParser.stripFences(null));
    }
}
