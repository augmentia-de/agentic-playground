package de.augmentia.agentflow.app.workflow.generate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.augmentia.agentflow.app.quad.QuadCapabilities;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionLoader;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.workflow.generate.WorkflowPlanParser;
import de.augmentia.quad.core.workflow.generate.WorkflowPlanParser.GeneratedStep;
import de.augmentia.quad.core.workflow.generate.WorkflowPlanParser.WorkbookResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Dynamic workflow creation: an LLM ("Workflow Architect") decomposes a task description into a
 * DAG of agent steps; the steps are converted into an agentflow <b>v2 QUAD document</b>
 * ({@code quad.workflow} / {@code schemaVersion: 2}, inline {@code agents[]} + {@code mainWorkflow}
 * nodes with {@code dependsOn}) and published through the same compiler/validator as the
 * classpath definitions — so a generated workflow is guaranteed to be loadable and runnable by the
 * durable Temporal engine ({@code DeclarativeAgentWorkflow}).
 *
 * <p>Ported from the quad-quarkus {@code WorkflowGeneratorService} (same Workflow-Architect prompt
 * and step parsing/validation), but adapted to the v2 document model instead of the nodes/edges
 * {@code WorkflowDef}. The LLM call goes through quad-core (LangChain4j) just like every other
 * agentflow step, so it honours the very same {@code OPENAI_*}/{@code QUAD_LLM_*} configuration
 * and dynamic provider headers.</p>
 */
@Service
public class WorkflowGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowGeneratorService.class);

    private static final int DEFAULT_MAX_RETRIES = 3;

    // ─────────────────────────────────────────────────────────────────────────────
    // Workflow-Architect system prompt (given verbatim to the planner agent).
    // ─────────────────────────────────────────────────────────────────────────────
    static final String WORKFLOW_ARCHITECT_SYSTEM = """
        You are a highly specialized Workflow Architect. Your task is to break down complex user requests into a structured, parallelized or sequential multi-agent workflow.

        ### INPUT
        You will receive:
        1. A goal or objective (User Intent).
        2. A list of available tools (Tool Repository).

        ### ARCHITECTURE RULES
        1. **Agent Roles (`agentRole`):** Create concise, unique role names (e.g., "SQL-Expert", "Data-Cleaner", "Report-Generator").
        2. **Instructions (`instruction`):** Each instruction must be precise and self-contained. If a task depends on a previous step, explicitly instruct the agent that the predecessor results are provided in the step context under "Previous results".
        3. **Tools (`tools`):** Assign ONLY tools that exist in the provided Tool Repository. If an agent does not require tools, pass an empty array `[]`.
        4. **Dependencies (`dependsOn`):**
           - Define dependencies ONLY when a step strictly requires the output of a previous step.
           - Maximize parallelism: Independent tasks MUST NOT have `dependsOn` set, allowing them to execute concurrently.
           - **Fork/Join Pattern:** When parallel branches produce results that a downstream step needs to aggregate, that step MUST declare `dependsOn` listing ALL predecessor branches (merge point). The platform feeds all predecessor outputs as text into the step context.
        5. **Timeouts (`timeoutSeconds`):** Default is 120 seconds. Increase up to 300+ seconds for computationally intensive tasks (e.g., builds, complex data processing, test suites).

        CRITICAL: The provided tools of the Tool Repository can ONLY be assigned as strings inside the `tools` parameter of `steps`.
            You, the Orchestrator, CANNOT execution-call these tools yourself!

        ### OUTPUT FORMAT
        Respond ONLY with a valid JSON object. Do not include any conversational text or markdown explanation outside the JSON block.

        JSON Schema:
        {
          "workflowName": "Short descriptive name of the workflow",
          "description": "Summary of the overall goal and process",
          "steps": [
            {
              "agentRole": "String",
              "instruction": "String",
              "tools": ["String"],
              "dependsOn": ["String"],
              "timeoutSeconds": Number,
              "systemPrompt": "Optional: dedicated system prompt for this step's agent. Omit to use the platform default.",
              "data": {
                 "output": { "type": "object", "properties": { "field": { "type": "string" } } },
                 "outputDescription": "Human-readable description of the JSON data this step produces and hands to its successors."
              }
            }
          ]
        }

        ### DATA CONTRACT
        - Every step SHOULD declare the JSON data it produces under `data.output` (a JSON-schema-like object) and a short `data.outputDescription`.
        - The `instruction` MUST tell the step's agent to return its result according to `data.output` and MUST explicitly name where to find received predecessor data (the step context contains the predecessor results under "Previous results").
        """;

    private final ObjectMapper mapper;
    private final WorkflowDefinitionLoader loader;
    private final String model;
    private final int maxOutputTokens;

    /**
     * Planner completion budget applied when none is configured (or a non-positive value is given).
     * Reasoning models need room for reasoning tokens AND the JSON answer; at a small limit the plan
     * arrives truncated and parsing fails with "Unexpected end-of-input".
     */
    static final int DEFAULT_MAX_OUTPUT_TOKENS = 32768;

    public WorkflowGeneratorService(ObjectMapper mapper,
                                    @Value("${spring.ai.openai.chat.options.model:gpt-4o-mini}") String model,
                                    @Value("${agentflow.workflow.generator.max-output-tokens:0}") int maxOutputTokens) {
        this.mapper = mapper;
        this.loader = new WorkflowDefinitionLoader(mapper);
        this.model = (model == null || model.isBlank()) ? "gpt-4o-mini" : model;
        this.maxOutputTokens = maxOutputTokens > 0 ? maxOutputTokens : DEFAULT_MAX_OUTPUT_TOKENS;
    }

    // ── public API ───────────────────────────────────────────────────────────────

    /** Generates and publishes a v2 workflow document for the given task. */
    public JsonNode generate(String task, String name) {
        return generate(task, name, DEFAULT_MAX_RETRIES);
    }

    /** Generates a v2 workflow document, retrying the LLM call up to {@code maxRetries} times. */
    public JsonNode generate(String task, String name, int maxRetries) {
        if (task == null || task.isBlank()) {
            throw new IllegalArgumentException("task required");
        }
        String sessionId = "gen-" + UUID.randomUUID().toString().substring(0, 8);
        String raw = callPlanner(planningMessage(task), sessionId, Math.max(1, maxRetries));
        WorkbookResult r = parsePlannerReply(raw);
        String resolvedName = firstNonBlank(name, r.workflowName(), "Dynamic Workflow");
        String id = "workflow-dyn-" + UUID.randomUUID().toString().substring(0, 8);
        JsonNode draft = toV2Document(id, resolvedName, task, r.steps());
        // Publish through the same compiler + semantic validator as the classpath definitions.
        return loader.load(draft);
    }

    // ── LLM call (quad-core / LangChain4j, honours OPENAI_*/QUAD_LLM_* + dynamic headers) ──

    private String planningMessage(String task) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("task", task);
        payload.put("toolList", QuadCapabilities.BUILTIN_TOOLS);
        try {
            return mapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize planner user message", e);
        }
    }

    private String callPlanner(String userMessage, String sessionId, int maxRetries) {
        String working = userMessage;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            AgentSessionState session = AgentSessionState.create(sessionId);
            CurrentSession.setCurrent(session);
            try {
                AgentBuilder<de.augmentia.quad.core.agent.QuadAgent> builder = AgentBuilder.defaultAgent()
                        .withModel(ModelFactory.createOpenAiFromEnv())
                        .withSystemPrompt(WORKFLOW_ARCHITECT_SYSTEM)
                        .withChatParametersJson(chatParametersJson());
                String raw = builder.build().run(working, session);
                if (raw == null || raw.isBlank()) {
                    throw new IllegalStateException("empty planner reply");
                }
                return raw;
            } catch (Exception e) {
                log.warn("Workflow planner attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    throw new IllegalStateException(
                            "Workflow generation failed after " + maxRetries + " attempts: " + e.getMessage(), e);
                }
                working += "\n\n[SYSTEM: previous attempt failed. Return ONLY valid JSON matching the schema. No markdown.]";
            } finally {
                CurrentSession.setCurrent(null);
            }
        }
        throw new IllegalStateException("unreachable");
    }

    // package-private for unit tests: the request parameters sent to the planner
    String chatParametersJson() {
        return "{\"temperature\":0.0,\"maxOutputTokens\":" + maxOutputTokens + "}";
    }

    // ── parsing + validation (pure, package-private for unit tests) ───────────────

    /** Parses the Workflow-Architect JSON output into steps + workflow meta. */
    WorkbookResult parseSteps(String json) {
        return WorkflowPlanParser.parseSteps(mapper, json);
    }

    /** Pure validation over parsed steps: dependency existence + acyclicity. */
    void validateSteps(List<GeneratedStep> steps, Map<String, GeneratedStep> byRole) {
        WorkflowPlanParser.validateSteps(steps, byRole);
    }

    /** Strips markdown fences and surrounding prose from the raw LLM reply. */
    String stripFences(String raw) {
        return WorkflowPlanParser.stripFences(raw);
    }

    /**
     * Normalises and parses one planner reply, turning a reply that was cut off mid-JSON (the
     * provider hit {@code finish_reason=length}) into an actionable error instead of a bare
     * "Invalid JSON from planner" parser failure.
     */
    WorkbookResult parsePlannerReply(String raw) {
        String cleaned = stripFences(raw);
        try {
            return parseSteps(cleaned);
        } catch (IllegalArgumentException e) {
            if (looksTruncated(cleaned)) {
                throw new IllegalStateException(
                        "The planner reply appears truncated (provider finish_reason=length). Increase "
                                + "agentflow.workflow.generator.max-output-tokens (currently " + maxOutputTokens
                                + "); the reply ended in the middle of the JSON. Cause: " + e.getMessage(), e);
            }
            throw e;
        }
    }

    /**
     * Heuristic: a JSON object/array with more open than close tokens (ignoring braces inside
     * strings) was cut off. Used only to improve the error message, never to accept invalid input.
     */
    static boolean looksTruncated(String json) {
        if (json == null || json.isBlank()) return false;
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) continue;
            if (c == '{' || c == '[') depth++;
            else if (c == '}' || c == ']') depth--;
        }
        return depth > 0;
    }

    // ── conversion: steps → v2 QUAD document (package-private for unit tests) ─────

    JsonNode toV2Document(String id, String name, String task, List<GeneratedStep> steps) {
        ObjectNode doc = mapper.createObjectNode();
        doc.put("format", "quad.workflow");
        doc.put("schemaVersion", 2);

        ObjectNode main = doc.putObject("mainWorkflow");
        main.put("id", id);
        main.put("name", name);
        main.put("version", "1.0.0");
        main.put("status", "draft");
        main.put("initialTask", task);
        ArrayNode nodes = main.putArray("nodes");
        ArrayNode agents = doc.putArray("agents");

        // 1. Deterministic, unique ids per step (agent id + node id share the slug).
        Set<String> usedSlugs = new LinkedHashSet<>();
        Map<String, String> roleToNodeId = new LinkedHashMap<>();
        Map<String, String> roleToAgentId = new LinkedHashMap<>();
        for (GeneratedStep s : steps) {
            String slug = uniqueSlug(slug(s.agentRole()), usedSlugs);
            roleToNodeId.put(s.agentRole(), "node-" + slug);
            roleToAgentId.put(s.agentRole(), "gen-" + slug);
        }

        // 2. One inline agent per step.
        for (GeneratedStep s : steps) {
            ObjectNode a = agents.addObject();
            a.put("id", roleToAgentId.get(s.agentRole()));
            a.put("name", s.agentRole());
            a.put("version", "1.0.0");
            a.put("category", "generated");
            a.put("model", model);
            a.put("temperature", 0.2);
            a.put("maxTokens", 1024);
            a.put("topP", 1.0);
            ArrayNode tools = a.putArray("tools");
            for (String t : allowedTools(s.tools())) tools.add(t);
            a.putArray("guardrailsInput");
            a.putArray("guardrailsOutput");
            a.putArray("hooks");
            a.put("jsonOutput", s.data() != null && !s.data().isEmpty());
            a.put("active", true);
            a.put("systemPrompt", systemPromptFor(s));
        }

        // 3. One agent node per step (dependsOn wired role→node). Exactly one END node.
        Set<String> referencedAsDependency = new HashSet<>();
        for (GeneratedStep s : steps) {
            ObjectNode n = nodes.addObject();
            n.put("id", roleToNodeId.get(s.agentRole()));
            n.put("type", "agent");
            n.put("title", s.agentRole());
            if (s.instruction() != null && !s.instruction().isBlank()) {
                n.put("description", s.instruction());
            }
            ArrayNode deps = n.putArray("dependsOn");
            for (String dep : s.dependsOn()) {
                String depNodeId = roleToNodeId.get(dep);
                deps.add(depNodeId);
                referencedAsDependency.add(depNodeId);
            }
            ObjectNode ref = n.putObject("agent");
            ref.put("ref", roleToAgentId.get(s.agentRole()));
        }

        // 4. The single END node depends on all terminal steps (nothing depends on them).
        List<String> terminals = new ArrayList<>();
        for (GeneratedStep s : steps) {
            String nodeId = roleToNodeId.get(s.agentRole());
            if (!referencedAsDependency.contains(nodeId)) {
                terminals.add(nodeId);
            }
        }
        if (terminals.isEmpty()) {
            terminals = new ArrayList<>(roleToNodeId.values());
        }
        ObjectNode end = nodes.addObject();
        end.put("id", "end");
        end.put("type", "end");
        end.put("title", "Done");
        ArrayNode endDeps = end.putArray("dependsOn");
        for (String t : terminals) endDeps.add(t);

        return doc;
    }

    private String systemPromptFor(GeneratedStep s) {
        StringBuilder sb = new StringBuilder();
        if (s.systemPrompt() != null && !s.systemPrompt().isBlank()) {
            sb.append(s.systemPrompt().trim());
        } else {
            sb.append("You are the '").append(s.agentRole()).append("' agent in a multi-agent workflow.");
        }
        if (s.instruction() != null && !s.instruction().isBlank()) {
            sb.append("\n\nTask: ").append(s.instruction().trim());
        }
        if (s.data() != null && !s.data().isEmpty()) {
            sb.append("\n\nReturn your result as a JSON object.");
        }
        return sb.toString();
    }

    private List<String> allowedTools(List<String> tools) {
        if (tools == null || tools.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String t : tools) {
            if (t != null && QuadCapabilities.BUILTIN_TOOLS.contains(t) && !out.contains(t)) {
                out.add(t);
            }
        }
        return out;
    }

    private String slug(String raw) {
        String s = (raw == null ? "step" : raw.toLowerCase(Locale.ROOT))
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("(^-+)|(-+$)", "");
        return s.isBlank() ? "step" : s;
    }

    private String uniqueSlug(String base, Set<String> used) {
        String candidate = base;
        int i = 2;
        while (!used.add(candidate)) {
            candidate = base + "-" + i++;
        }
        return candidate;
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return "";
    }

    // ── data types ────────────────────────────────────────────────────────────────
    // GeneratedStep / WorkbookResult are the shared, single-sourced types from
    // de.augmentia.quad.core.workflow.generate.WorkflowPlanParser (imported above).
}
