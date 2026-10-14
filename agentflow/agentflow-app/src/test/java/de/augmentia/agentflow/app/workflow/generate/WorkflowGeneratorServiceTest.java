package de.augmentia.agentflow.app.workflow.generate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionLoader;
import de.augmentia.quad.core.workflow.generate.WorkflowPlanParser.GeneratedStep;
import de.augmentia.quad.core.workflow.generate.WorkflowPlanParser.WorkbookResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pure (LLM-free) tests for the Workflow-Architect parsing/validation and the v2 document conversion. */
class WorkflowGeneratorServiceTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final WorkflowGeneratorService service = new WorkflowGeneratorService(mapper, "test-model", 0);

    // ── stripFences ──────────────────────────────────────────────────────────

    @Test
    void stripFencesRemovesMarkdownFencesAndProse() {
        assertEquals("{\"steps\":[]}", service.stripFences("```json\n{\"steps\":[]}\n```"));
        assertEquals("{\"steps\":[]}", service.stripFences("Here you go:\n{\"steps\":[]}\nDone."));
        assertEquals("{\"steps\":[]}", service.stripFences("json\n{\"steps\":[]}"));
        assertEquals("", service.stripFences(null));
    }

    // ── parseSteps / validateSteps ───────────────────────────────────────────

    @Test
    void parsesValidSteps() {
        String json = """
            {"workflowName":"Research","steps":[
              {"agentRole":"Scout","instruction":"gather","tools":["webSearch"],"dependsOn":[],"timeoutSeconds":60},
              {"agentRole":"Writer","instruction":"write","tools":[],"dependsOn":["Scout"],"timeoutSeconds":120,
               "data":{"output":{"type":"object","properties":{"text":{"type":"string"}}}}}
            ]}""";
        WorkbookResult r = service.parseSteps(json);
        assertEquals("Research", r.workflowName());
        assertEquals(2, r.steps().size());
        assertEquals(List.of("Scout"), r.steps().get(1).dependsOn());
    }

    @Test
    void rejectsUnknownDependency() {
        String json = """
            {"steps":[{"agentRole":"A","instruction":"a","dependsOn":["missing"]}]}""";
        assertThrows(IllegalArgumentException.class, () -> service.parseSteps(json));
    }

    @Test
    void rejectsCycle() {
        String json = """
            {"steps":[
              {"agentRole":"A","instruction":"a","dependsOn":["B"]},
              {"agentRole":"B","instruction":"b","dependsOn":["A"]}]}""";
        assertThrows(IllegalArgumentException.class, () -> service.parseSteps(json));
    }

    @Test
    void rejectsDuplicateRole() {
        String json = """
            {"steps":[
              {"agentRole":"A","instruction":"1"},
              {"agentRole":"A","instruction":"2"}]}""";
        assertThrows(IllegalArgumentException.class, () -> service.parseSteps(json));
    }

    // ── toV2Document + publish/validate gate ─────────────────────────────────

    @Test
    void buildsValidPublishedV2Document() {
        List<GeneratedStep> steps = List.of(
                step("Classifier", "classify the ticket", List.of(), List.of(), null),
                step("Researcher", "research", List.of("webSearch", "bogusTool"), List.of("Classifier"), Map.of("output", Map.of("type", "object"))),
                step("Writer", "write", List.of(), List.of("Researcher"), null));

        JsonNode draft = service.toV2Document("workflow-dyn-test", "Test WF", "do it", steps);
        // Publishing runs the same compiler + semantic validator as classpath definitions.
        JsonNode published = new WorkflowDefinitionLoader(mapper).load(draft);

        assertEquals("quad.workflow", published.path("format").asText());
        assertEquals(2, published.path("schemaVersion").asInt());
        assertEquals("published", published.path("mainWorkflow").path("status").asText());
        assertTrue(published.path("contentHash").asText().startsWith("sha256:"));
        assertEquals("do it", published.path("mainWorkflow").path("initialTask").asText());

        assertEquals(3, published.path("agents").size());
        // Unknown tools are dropped; only the configured built-in survives.
        JsonNode researcher = published.path("agents").get(1);
        assertEquals(1, researcher.path("tools").size());
        assertEquals("webSearch", researcher.path("tools").get(0).asText());
        assertTrue(researcher.path("id").asText().startsWith("gen-"));

        JsonNode nodes = published.path("mainWorkflow").path("nodes");
        // 3 agent nodes + exactly one end node.
        assertEquals(4, nodes.size());
        int ends = 0;
        JsonNode end = null;
        for (JsonNode n : nodes) {
            if ("end".equals(n.path("type").asText())) {
                ends++;
                end = n;
            }
        }
        assertEquals(1, ends);
        // The end node depends on the terminal step (the Writer node).
        assertTrue(dependsOn(end).contains("node-writer"));
        // The Writer node consumes the Researcher output.
        assertTrue(dependsOn(findNode(nodes, "node-writer")).contains("node-researcher"));
    }

    // ── token limit / truncation handling (B11) ──────────────────────────────

    @Test
    void defaultTokenLimitApplied() {
        // 0 (the old default) must fall back to a sensible budget so reasoning models fit.
        var svc = new WorkflowGeneratorService(mapper, "test-model", 0);
        assertTrue(svc.chatParametersJson().contains("\"maxOutputTokens\":32768"),
                svc.chatParametersJson());
    }

    @Test
    void explicitTokenLimitHonoured() {
        var svc = new WorkflowGeneratorService(mapper, "test-model", 4096);
        assertTrue(svc.chatParametersJson().contains("\"maxOutputTokens\":4096"),
                svc.chatParametersJson());
    }

    @Test
    void truncatedPlanGivesActionableError() {
        var svc = new WorkflowGeneratorService(mapper, "test-model", 128);
        // The reply was cut off mid-object: the "steps" array is never closed.
        String truncated = "{\"steps\":[{\"agentRole\":\"A\",\"instruction\":\"do the thing";
        var ex = assertThrows(IllegalStateException.class, () -> svc.parsePlannerReply(truncated));
        assertTrue(ex.getMessage().toLowerCase().contains("truncat"), ex.getMessage());
        assertTrue(ex.getMessage().contains("max-output-tokens"), ex.getMessage());
    }

    @Test
    void nonTruncatedBadJsonKeepsParseError() {
        var svc = new WorkflowGeneratorService(mapper, "test-model", 1024);
        // Balanced JSON that simply lacks "steps": must stay a normal parse error, not "truncated".
        assertThrows(IllegalArgumentException.class, () -> svc.parsePlannerReply("{\"nope\":true}"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private GeneratedStep step(String role, String instruction,
                               List<String> tools, List<String> dependsOn,
                               Map<String, Object> data) {
        return new GeneratedStep(role, instruction, tools, dependsOn, 120, null, data);
    }

    private JsonNode findNode(JsonNode nodes, String id) {
        for (JsonNode n : nodes) {
            if (id.equals(n.path("id").asText())) return n;
        }
        throw new AssertionError("node not found: " + id);
    }

    private List<String> dependsOn(JsonNode node) {
        List<String> out = new java.util.ArrayList<>();
        for (JsonNode d : node.path("dependsOn")) out.add(d.asText());
        return out;
    }
}
