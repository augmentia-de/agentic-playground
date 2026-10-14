package de.augmentia.quad.core.workflow.generate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared, LLM-free parsing and validation of the Workflow-Architect planner reply.
 *
 * <p>The quad (nodes/edges {@code WorkflowDef}) and the agentflow (v2 {@code quad.workflow} document)
 * dynamic workflow generators each used to carry near-verbatim private copies of this code. It is the
 * part that silently decides whether a generated plan is accepted, so it lives here once and both
 * apps delegate to it; only the target-specific graph building stays in each application.
 *
 * <p>Keeping this single-sourced prevents the two generators from drifting apart again (see Review
 * A1/B6).
 */
public final class WorkflowPlanParser {

    private WorkflowPlanParser() {
    }

    /**
     * A single step in the generated task DAG, following the Workflow-Architect schema:
     * {@code agentRole}, {@code instruction}, {@code tools}, {@code dependsOn}, {@code timeoutSeconds}.
     */
    public record GeneratedStep(String agentRole, String instruction,
                                List<String> tools, List<String> dependsOn, int timeoutSeconds,
                                String systemPrompt, Map<String, Object> data) {
    }

    /** Parsed steps + workflow meta + the last raw (pre-parse) planner reply. */
    public record WorkbookResult(List<GeneratedStep> steps, String workflowName,
                                 String description, String raw) {
    }

    /**
     * Parses the Workflow-Architect JSON output into steps + workflow meta and validates the DAG.
     *
     * @param mapper the Jackson mapper to use (each app passes its own)
     * @param json   the planner reply, already run through {@link #stripFences(String)}
     * @throws IllegalArgumentException if the reply is not valid JSON, is missing the {@code steps}
     *                                  array, or the step graph is malformed (unknown dependency,
     *                                  cycle, duplicate role)
     */
    public static WorkbookResult parseSteps(ObjectMapper mapper, String json) {
        try {
            JsonNode root = mapper.readTree(json);
            if (root == null || !root.has("steps") || !root.get("steps").isArray()) {
                throw new IllegalArgumentException("JSON missing 'steps' array");
            }
            String workflowName = root.hasNonNull("workflowName") ? root.get("workflowName").asText() : null;
            String description = root.hasNonNull("description") ? root.get("description").asText() : null;
            List<GeneratedStep> steps = new ArrayList<>();
            Map<String, GeneratedStep> byRole = new LinkedHashMap<>();
            for (JsonNode sn : root.get("steps")) {
                String agentRole = sn.hasNonNull("agentRole") ? sn.get("agentRole").asText() : "";
                if (agentRole.isBlank()) {
                    throw new IllegalArgumentException("Step is missing 'agentRole'");
                }
                String instruction = sn.hasNonNull("instruction") ? sn.get("instruction").asText() : "";
                List<String> tools = new ArrayList<>();
                if (sn.has("tools") && sn.get("tools").isArray()) {
                    for (JsonNode t : sn.get("tools")) tools.add(t.asText());
                }
                List<String> dependsOn = new ArrayList<>();
                if (sn.has("dependsOn") && sn.get("dependsOn").isArray()) {
                    for (JsonNode dep : sn.get("dependsOn")) dependsOn.add(dep.asText());
                }
                int timeoutSeconds = sn.hasNonNull("timeoutSeconds") && !sn.get("timeoutSeconds").isNull()
                        ? sn.get("timeoutSeconds").asInt() : 120;
                String systemPrompt = sn.hasNonNull("systemPrompt") ? sn.get("systemPrompt").asText() : null;
                Map<String, Object> data = null;
                if (sn.has("data") && sn.get("data").isObject()) {
                    data = mapper.convertValue(sn.get("data"), new TypeReference<Map<String, Object>>() {});
                }
                GeneratedStep s = new GeneratedStep(
                        agentRole, instruction, tools, dependsOn, timeoutSeconds, systemPrompt, data);
                if (byRole.put(agentRole, s) != null) {
                    throw new IllegalArgumentException("Duplicate agentRole: " + agentRole);
                }
                steps.add(s);
            }
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("Parsed 0 steps from JSON");
            }
            validateSteps(steps, byRole);
            return new WorkbookResult(steps, workflowName, description, json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON from planner: " + e.getMessage(), e);
        }
    }

    /** Pure validation over parsed steps: dependency existence + acyclicity. */
    public static void validateSteps(List<GeneratedStep> steps, Map<String, GeneratedStep> byRole) {
        for (GeneratedStep s : steps) {
            for (String dep : s.dependsOn()) {
                if (!byRole.containsKey(dep)) {
                    throw new IllegalArgumentException(
                            "Step '" + s.agentRole() + "' depends on non-existent step '" + dep + "'");
                }
            }
        }
        Set<String> visited = new HashSet<>();
        Set<String> stack = new HashSet<>();
        for (GeneratedStep s : steps) {
            if (hasCycle(s.agentRole(), byRole, visited, stack)) {
                throw new IllegalArgumentException(
                        "Cycle detected in DAG involving step '" + s.agentRole() + "'");
            }
        }
    }

    private static boolean hasCycle(String role, Map<String, GeneratedStep> byRole,
                                    Set<String> visited, Set<String> stack) {
        if (stack.contains(role)) return true;
        if (visited.contains(role)) return false;
        visited.add(role);
        stack.add(role);
        GeneratedStep s = byRole.get(role);
        if (s != null) {
            for (String dep : s.dependsOn()) {
                if (hasCycle(dep, byRole, visited, stack)) return true;
            }
        }
        stack.remove(role);
        return false;
    }

    /**
     * Strips markdown fences and surrounding prose from the raw LLM reply, so only the JSON object
     * remains. Handles {@code ```json ... ```} fences, a bare leading {@code json} token (emitted by
     * some compatible models) and a reasoning preamble before the first {@code {}.
     */
    public static String stripFences(String raw) {
        if (raw == null) return "";
        String t = raw.trim();
        t = t.replaceFirst("(?s)^```(?:json)?\\s*", "");
        t = t.replaceFirst("(?s)\\s*```\\s*$", "");
        // Some compatible models emit a bare 'json' token before the object.
        t = t.replaceFirst("(?s)^json\\s*(?=\\{)", "");
        // Reasoning models prepend a sentence: cut everything before the first '{' / after last '}'.
        int firstBrace = t.indexOf('{');
        int lastBrace = t.lastIndexOf('}');
        if (firstBrace >= 0 && lastBrace > firstBrace) {
            t = t.substring(firstBrace, lastBrace + 1);
        }
        return t;
    }
}
