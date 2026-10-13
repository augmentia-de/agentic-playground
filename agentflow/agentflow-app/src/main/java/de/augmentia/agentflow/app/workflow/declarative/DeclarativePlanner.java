package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flattens a v2 document into an executable {@link DeclarativePlan}: parses every node type and
 * inlines {@code subworkflow} nodes (recursively, depth-limited). Sub-workflows are resolved from
 * the document's own {@code subWorkflows} array first, then from the registry. The
 * {@code subworkflow} node id becomes an alias for the inlined sub-workflow's {@code end} node, so
 * external {@code dependsOn} references keep working.
 */
public class DeclarativePlanner {

    private static final int MAX_DEPTH = 5;

    private final WorkflowDefinitionRegistry registry;

    public DeclarativePlanner(WorkflowDefinitionRegistry registry) {
        this.registry = registry;
    }

    public DeclarativePlan plan(String workflowId) {
        JsonNode doc = registry.require(workflowId);
        List<NodeSpec> flat = new ArrayList<>();
        Map<String, String> alias = new HashMap<>();
        expand(doc.path("mainWorkflow"), agentsById(doc), subWorkflowsById(doc), "", List.of(), 0, flat, alias);
        List<NodeSpec> rewritten = new ArrayList<>();
        for (NodeSpec n : flat) {
            rewritten.add(rewrite(n, alias));
        }
        return new DeclarativePlan(rewritten);
    }

    private void expand(JsonNode workflow, Map<String, JsonNode> agents, Map<String, JsonNode> subWorkflows,
                        String prefix, List<String> entryDeps, int depth,
                        List<NodeSpec> out, Map<String, String> alias) {
        if (depth > MAX_DEPTH) {
            throw new IllegalStateException("subworkflow nesting exceeds " + MAX_DEPTH);
        }
        for (JsonNode node : workflow.path("nodes")) {
            String id = prefix + node.path("id").asText();
            if ("subworkflow".equals(node.path("type").asText())) {
                String ref = node.path("workflowRef").asText();
                JsonNode subWorkflow = subWorkflows.get(ref);
                Map<String, JsonNode> subAgents = agents;
                Map<String, JsonNode> subSubs = subWorkflows;
                if (subWorkflow == null) {
                    JsonNode subDoc = registry.require(ref);
                    subWorkflow = subDoc.path("mainWorkflow");
                    subAgents = agentsById(subDoc);
                    subSubs = subWorkflowsById(subDoc);
                }
                expand(subWorkflow, subAgents, subSubs, id + "/", deps(node, prefix, entryDeps), depth + 1, out, alias);
                alias.put(id, id + "/end");
                continue;
            }
            out.add(spec(node, id, prefix, entryDeps, agents));
        }
    }

    private NodeSpec spec(JsonNode node, String id, String prefix, List<String> entryDeps,
                          Map<String, JsonNode> agents) {
        String type = node.path("type").asText();
        String agentId = null;
        String systemPrompt = "";
        List<String> tools = List.of();
        boolean jsonOutput = false;
        List<String> guardrailsInput = List.of();
        List<String> guardrailsOutput = List.of();
        List<String> hooks = List.of();
        if ("agent".equals(type)) {
            agentId = node.path("agent").path("ref").asText();
            JsonNode agent = agents.get(agentId);
            if (agent != null) {
                systemPrompt = agent.path("systemPrompt").asText("");
                tools = strings(agent, "tools");
                jsonOutput = agent.path("jsonOutput").asBoolean(false);
                guardrailsInput = strings(agent, "guardrailsInput");
                guardrailsOutput = strings(agent, "guardrailsOutput");
                hooks = strings(agent, "hooks");
            }
        }

        List<Branch> branches = new ArrayList<>();
        for (JsonNode b : node.path("branches")) {
            String target = b.hasNonNull("target") ? b.get("target").asText() : b.path("firstStep").asText();
            branches.add(new Branch(b.path("branchId").asText(""), b.path("condition").asText(null), target));
        }

        return new NodeSpec(
                id,
                type,
                node.path("title").asText(""),
                agentId,
                systemPrompt,
                tools,
                jsonOutput,
                node.path("requiresHumanApproval").asBoolean(false),
                deps(node, prefix, entryDeps),
                branches,
                node.hasNonNull("defaultTarget") ? node.get("defaultTarget").asText() : null,
                node.path("maxIterations").asInt(1),
                node.path("exitCondition").asText(null),
                strings(node, "bodySteps"),
                node.path("joinType").asText("all"),
                node.has("joinN") ? node.get("joinN").asInt() : null,
                guardrailsInput,
                guardrailsOutput,
                hooks);
    }

    private List<String> deps(JsonNode node, String prefix, List<String> entryDeps) {
        List<String> raw = strings(node, "dependsOn");
        if (raw.isEmpty()) {
            return new ArrayList<>(entryDeps);
        }
        List<String> out = new ArrayList<>();
        for (String d : raw) {
            out.add(prefix + d);
        }
        return out;
    }

    private NodeSpec rewrite(NodeSpec n, Map<String, String> alias) {
        List<String> deps = new ArrayList<>();
        for (String d : n.dependsOn()) {
            deps.add(resolve(d, alias));
        }
        List<Branch> branches = new ArrayList<>();
        for (Branch b : n.branches()) {
            branches.add(new Branch(b.branchId(), b.condition(), b.target() == null ? null : resolve(b.target(), alias)));
        }
        String defaultTarget = n.defaultTarget() == null ? null : resolve(n.defaultTarget(), alias);
        return new NodeSpec(n.id(), n.type(), n.title(), n.agentId(), n.systemPrompt(), n.tools(), n.jsonOutput(),
                n.requiresHumanApproval(), deps, branches, defaultTarget, n.maxIterations(), n.exitCondition(),
                n.bodySteps(), n.joinType(), n.joinN(), n.guardrailsInput(), n.guardrailsOutput(), n.hooks());
    }

    private String resolve(String id, Map<String, String> alias) {
        String current = id;
        for (int i = 0; i <= MAX_DEPTH + 2; i++) {
            String next = alias.get(current);
            if (next == null || next.equals(current)) {
                return current;
            }
            current = next;
        }
        return current;
    }

    private Map<String, JsonNode> agentsById(JsonNode doc) {
        Map<String, JsonNode> map = new HashMap<>();
        for (JsonNode agent : doc.path("agents")) {
            map.put(agent.path("id").asText(), agent);
        }
        return map;
    }

    private Map<String, JsonNode> subWorkflowsById(JsonNode doc) {
        Map<String, JsonNode> map = new HashMap<>();
        for (JsonNode wf : doc.path("subWorkflows")) {
            map.put(wf.path("id").asText(), wf);
        }
        return map;
    }

    private List<String> strings(JsonNode node, String field) {
        List<String> out = new ArrayList<>();
        if (node != null && node.path(field).isArray()) {
            node.get(field).forEach(v -> out.add(v.asText()));
        }
        return out;
    }
}
