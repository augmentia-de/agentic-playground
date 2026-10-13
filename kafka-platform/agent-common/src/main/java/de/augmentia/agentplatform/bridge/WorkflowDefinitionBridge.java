package de.augmentia.agentplatform.bridge;

import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts Quarkus/quad-ui flat workflow format ({@code {name, nodes[], edges[]}})
 * into the Kafka-Orchestrator nested DAG format ({@code DagNode}s with explicit dependsOn).
 *
 * <p>Quad → Kafka mappings:
 * <ul>
 *   <li>{@code agent} → {@link NodeType#STEP}</li>
 *   <li>{@code conditional} → {@link NodeType#DECISION}</li>
 *   <li>{@code fork} → {@link NodeType#FORK}</li>
 *   <li>{@code join} → {@link NodeType#JOIN}</li>
 *   <li>{@code loop} → {@link NodeType#LOOP}</li>
 *   <li>{@code end} → {@link NodeType#END}</li>
 * </ul>
 *
 * <p>Dependencies are extracted from edges and turned into explicit {@code dependsOn}.
 * Branch routing (for DECISION) is derived from edges leaving a conditional node.
 * Join strategy is mapped from edge-level {@code joinPolicy} attributes.
 *
 * <p><b>Fail-fast:</b> Types the orchestrator cannot execute are no longer
 * silently skipped ({@code nested-workflow}) or guessed (unknown →
 * STEP). The conversion then throws {@link WorkflowConversionException}. A LOOP
 * without a reachable body is rejected as well.
 *
 * <p><b>LOOP → Region:</b> The body of a LOOP is the set of steps repeated
 * per iteration — by default the entire downstream region of the loop node
 * (any number of agents), with a set {@code loopTargetId} the region reachable
 * from there. That corresponds to the quad model
 * ({@code GraphUtils.computeLoopBody}). A Kafka LOOP with a set {@code agentType}
 * and an empty body remains the cooperative single-agent loop.
 *
 * <h3>END node</h3>
 * The canvas offers <em>no</em> END node — the six types above are the
 * full repertoire. The orchestrator, however, demands one when saving
 * ({@code WorkflowDefinitionValidator} rule R6, called in
 * {@code WorkflowDefinitionService.create}). Without an addition,
 * <em>every</em> workflow arriving from the UI would be
 * rejected with {@code "R6: no END node"}.
 *
 * <p>That is why the bridge adds a synthetic END node when none is
 * present. It attaches to the steps without an outgoing edge — the leaves
 * of the graph. An existing END is left untouched.
 */
public class WorkflowDefinitionBridge {

    /** stepId of the synthetic END node. Prefix like other internal IDs. */
    public static final String SYNTHETIC_END_ID = "__end";

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(WorkflowDefinitionBridge.class);

    /**
     * Full conversion entry-point.
     *
     * @param quadFormat flat format with name + nodes + edges
     * @return list of Kafka-compatible DAG nodes
     */
    public static List<DagNode> convert(QuarkusWorkflow quadFormat) {
        Map<String, List<QuarkusEdge.Edge>> adjacency = buildAdjacency(quadFormat.edges());
        Set<String> loopIds = new HashSet<>();
        for (var node : quadFormat.nodes()) {
            if ("loop".equalsIgnoreCase(node.type())) loopIds.add(node.id());
        }
        // Back edges into a loop express iteration, not a dependency — otherwise every loop
        // with a back edge would turn into a dependency cycle in the Kafka DAG. The entry edge
        // (from an upstream node) is *not* a back edge; only edges whose source lies in the
        // loop's own downstream region are.
        Set<String> backEdges = new HashSet<>();
        for (String loopId : loopIds) {
            Set<String> downstream = downstreamOf(loopId, adjacency);
            for (var entry : adjacency.entrySet()) {
                for (var e : entry.getValue()) {
                    if (e.target().equals(loopId) && downstream.contains(e.source())) {
                        backEdges.add(edgeKey(e));
                    }
                }
            }
        }
        Map<String, List<String>> dependsOnMap = reverseAdjacency(adjacency, backEdges);

        List<DagNode> result = new ArrayList<>();

        for (var node : quadFormat.nodes()) {
            switch (node.type().toLowerCase()) {
                case "agent", "generic" ->
                    result.add(convertStep(node, dependsOnMap.getOrDefault(node.id(), List.of())));
                case "conditional" ->
                    result.add(convertDecision(node, adjacency, dependsOnMap.getOrDefault(node.id(), List.of())));
                case "fork" ->
                    result.add(convertFork(node, adjacency, dependsOnMap.getOrDefault(node.id(), List.of())));
                case "join" ->
                    result.add(convertJoin(node, adjacency, dependsOnMap.getOrDefault(node.id(), List.of())));
                case "loop" ->
                    result.add(convertLoop(node, adjacency, dependsOnMap.getOrDefault(node.id(), List.of())));
                case "end" ->
                    result.add(convertEnd(node, dependsOnMap.getOrDefault(node.id(), List.of())));
                case "nested-workflow" ->
                    // FEATURE GAP: the Kafka orchestrator does not support nested workflows.
                    // Previously the node was silently skipped — outgoing edges then pointed at
                    // a non-existent step and the validation reported
                    // a misleading R7 error. Now the conversion aborts.
                    throw new WorkflowConversionException(
                            "Node '" + node.id() + "' has unsupported type 'nested-workflow': "
                            + "the Kafka orchestrator cannot execute nested workflows");
                default ->
                    throw new WorkflowConversionException(
                            "Node '" + node.id() + "' has unsupported type '" + node.type() + "': "
                            + "supported types are agent/generic, conditional, fork, join, loop, end");
            }
        }

        appendSyntheticEndIfMissing(result, quadFormat.edges(), backEdges);
        return result;
    }

    /**
     * Adds an END node if the result contains none.
     *
     * <p>Terminal are steps from which <em>no</em> edge leaves. Control nodes
     * (DECISION, FORK, JOIN) do not count as terminal — they are processed by the
     * evaluator, not executed as a task.
     */
    private static void appendSyntheticEndIfMissing(List<DagNode> nodes, List<QuarkusEdge> edges,
                                                    Set<String> backEdges) {
        boolean hasEnd = nodes.stream().anyMatch(n -> n.nodeType() == NodeType.END);
        if (hasEnd) {
            return;
        }
        Set<String> withSuccessor = new HashSet<>();
        for (QuarkusEdge edge : edges) {
            if (backEdges.contains(edge.source() + "->" + edge.target())) continue; // loop-back
            withSuccessor.add(edge.source());
        }

        List<String> terminals = nodes.stream()
                .filter(n -> n.nodeType() == NodeType.STEP)
                .filter(n -> !withSuccessor.contains(n.stepId()))
                .map(DagNode::stepId)
                .toList();

        if (terminals.isEmpty()) {
            log.warn("Workflow contains no END node and no terminal steps — "
                    + "the validation of the orchestrator will reject this (R6)");
            return;
        }

        nodes.add(DagNode.builder()
                .stepId(SYNTHETIC_END_ID)
                .nodeType(NodeType.END)
                .dependsOn(terminals)
                .build());
        log.debug("END node '{}' synthetically appended to {}", SYNTHETIC_END_ID, terminals);
    }

    private static DagNode convertEnd(QuarkusNode n, List<String> dependsOn) {
        return DagNode.builder()
                .stepId(n.id())
                .nodeType(NodeType.END)
                .dependsOn(dependsOn)
                .build();
    }

    // ── Node converters ────────────────────────────────────────────────

    private static DagNode convertStep(QuarkusNode n, List<String> dependsOn) {
        return DagNode.builder()
                .stepId(n.id())
                .nodeType(NodeType.STEP)
                .agentType(agentTypeOf(n))
                .dependsOn(dependsOn)
                .timeoutMs(timeoutFrom(n))
                .requiresHumanApproval(isHitl(n))
                .guardCondition(guardFrom(n))
                .build();
    }

    private static DagNode convertDecision(QuarkusNode n,
                                           Map<String, List<QuarkusEdge.Edge>> adjacency,
                                           List<String> dependsOn) {
        List<DagNode.Branch> branches = extractBranches(n.id(), adjacency);
        String defaultTarget = findDefaultTarget(n.id(), adjacency);

        return DagNode.builder()
                .stepId(n.id())
                .nodeType(NodeType.DECISION)
                .branches(branches)
                .defaultTarget(defaultTarget)
                .dependsOn(dependsOn)
                .build();
    }

    private static DagNode convertFork(QuarkusNode n,
                                       Map<String, List<QuarkusEdge.Edge>> adjacency,
                                       List<String> dependsOn) {
        List<DagNode.Branch> branchSteps = new ArrayList<>();
        for (var target : adjacency.getOrDefault(n.id(), List.of())) {
            branchSteps.add(DagNode.Branch.fork("branch-" + target.target(), target.target()));
        }

        return DagNode.builder()
                .stepId(n.id())
                .nodeType(NodeType.FORK)
                .branches(branchSteps)
                .dependsOn(dependsOn)
                .build();
    }

    private static DagNode convertJoin(QuarkusNode n,
                                       Map<String, List<QuarkusEdge.Edge>> adjacency,
                                       List<String> dependsOn) {
        String joinType = joinPolicyToJoinType(joinPolicyFrom(n));
        List<String> joinBranches = getSuccessors(n.id(), adjacency);

        return DagNode.builder()
                .stepId(n.id())
                .nodeType(NodeType.JOIN)
                .joinType(joinType)
                .joinBranches(joinBranches)
                .dependsOn(dependsOn)
                .build();
    }

    private static DagNode convertLoop(QuarkusNode n,
                                       Map<String, List<QuarkusEdge.Edge>> adjacency,
                                       List<String> dependsOn) {
        // Body = the loop's whole downstream region (any number of agents), narrowed to
        // the `loopTargetId` anchor when set — the quad model (GraphUtils.computeLoopBody).
        List<String> loopBody = extractLoopBody(n, adjacency);
        if (loopBody.isEmpty()) {
            throw new WorkflowConversionException(
                    "LOOP node '" + n.id() + "' has no body: the orchestrator iterates the nodes "
                    + "downstream of the loop, but none are reachable from it");
        }
        String exitCond = exitConditionFrom(n);
        Integer maxIter = maxIterationsFrom(n);

        return DagNode.builder()
                .stepId(n.id())
                .nodeType(NodeType.LOOP)
                .agentType(n.agentType()) // only meaningful for single-agent loops; region uses bodySteps
                .bodySteps(loopBody)
                .maxIterations(maxIter)
                .exitCondition(exitCond)
                .dependsOn(dependsOn)
                .build();
    }

    // ── Adjacency helpers ──────────────────────────────────────────────

    private static Map<String, List<QuarkusEdge.Edge>> buildAdjacency(List<QuarkusEdge> edges) {
        Map<String, List<QuarkusEdge.Edge>> adj = new LinkedHashMap<>();
        for (var e : edges) {
            adj.computeIfAbsent(e.source(), k -> new ArrayList<>()).add(e.toKafkaEdge());
        }
        return adj;
    }

    private static Map<String, List<String>> reverseAdjacency(Map<String, List<QuarkusEdge.Edge>> adjacency,
                                                              Set<String> backEdges) {
        Map<String, List<String>> rev = new LinkedHashMap<>();
        for (var entry : adjacency.entrySet()) {
            for (var e : entry.getValue()) {
                if (backEdges.contains(edgeKey(e))) continue; // loop-back edge, not a dependency
                rev.computeIfAbsent(e.target(), k -> new ArrayList<>()).add(entry.getKey());
            }
        }
        return rev;
    }

    private static String edgeKey(QuarkusEdge.Edge e) {
        return e.source() + "->" + e.target();
    }

    private static List<DagNode.Branch> extractBranches(String sourceId, Map<String, List<QuarkusEdge.Edge>> adjacency) {
        var outEdges = adjacency.getOrDefault(sourceId, List.of());
        List<DagNode.Branch> result = new ArrayList<>();
        int i = 0;
        for (var e : outEdges) {
            String branchId = "branch-" + (i++);
            String condition = conditionFromEdge(e);
            result.add(DagNode.Branch.decision(branchId, condition, e.target()));
        }
        return result;
    }

    private static String findDefaultTarget(String sourceId, Map<String, List<QuarkusEdge.Edge>> adjacency) {
        // Default = first edge that has no condition
        for (var e : adjacency.getOrDefault(sourceId, List.of())) {
            if (conditionFromEdge(e) == null || conditionFromEdge(e).isBlank()) {
                return e.target();
            }
        }
        // Fallback to first edge
        var edges = adjacency.getOrDefault(sourceId, List.of());
        return edges.isEmpty() ? null : edges.get(0).target();
    }

    private static List<String> getSuccessors(String nodeId, Map<String, List<QuarkusEdge.Edge>> adjacency) {
        return adjacency.getOrDefault(nodeId, List.of()).stream()
                .map(e -> e.target())
                .toList();
    }

    /**
     * Determines the body region of a LOOP (the steps repeated per iteration).
     *
     * <p>Default (no {@code loopTargetId}): the <em>entire</em> downstream region of the
     * loop node — that way any number of agents can lie in the body (quad:
     * {@code GraphUtils.computeLoopBody}). If {@code loopTargetId} is set, the body is
     * narrowed to the region reachable from there; nodes with a back edge to the loop end
     * the region. That way a step can be placed <em>after</em> the loop.
     */
    private static List<String> extractLoopBody(QuarkusNode loop, Map<String, List<QuarkusEdge.Edge>> adjacency) {
        String loopId = loop.id();
        Set<String> downstream = downstreamOf(loopId, adjacency);
        if (downstream.isEmpty()) return List.of();

        String loopTargetId = propertyAsString(loop, "loopTargetId");
        if (loopTargetId != null && !loopTargetId.isBlank() && downstream.contains(loopTargetId)) {
            Set<String> backEdgeSources = backEdgeSources(loopId, adjacency);
            List<String> body = new ArrayList<>();
            Set<String> visited = new LinkedHashSet<>();
            Deque<String> queue = new ArrayDeque<>();
            queue.add(loopTargetId);
            visited.add(loopTargetId);
            while (!queue.isEmpty()) {
                String cur = queue.poll();
                if (!downstream.contains(cur)) continue;
                body.add(cur);
                if (backEdgeSources.contains(cur)) continue; // body sink, do not leave the region
                for (var e : adjacency.getOrDefault(cur, List.of())) {
                    if (visited.add(e.target())) queue.add(e.target());
                }
            }
            return body;
        }

        // Default: whole downstream segment in stable BFS order.
        return new ArrayList<>(downstream);
    }

    /** All nodes reachable from the given node (excluding itself). */
    private static Set<String> downstreamOf(String sourceId, Map<String, List<QuarkusEdge.Edge>> adjacency) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(sourceId);
        while (!queue.isEmpty()) {
            String cur = queue.poll();
            for (var e : adjacency.getOrDefault(cur, List.of())) {
                if (seen.add(e.target())) queue.add(e.target());
            }
        }
        seen.remove(sourceId);
        return seen;
    }

    /** Nodes from which an edge leads back to the loop (back-edge sources). */
    private static Set<String> backEdgeSources(String loopId, Map<String, List<QuarkusEdge.Edge>> adjacency) {
        Set<String> sources = new HashSet<>();
        for (var entry : adjacency.entrySet()) {
            for (var e : entry.getValue()) {
                if (e.target().equals(loopId)) sources.add(entry.getKey());
            }
        }
        return sources;
    }

    private static String propertyAsString(QuarkusNode n, String key) {
        Object value = n.properties().get(key);
        return value == null ? null : value.toString();
    }

    // ── Attribute extraction ───────────────────────────────────────────

    /**
     * All node attributes, regardless of which object the producer put them in.
     *
     * <p>Two producers exist and they disagree: the quad-ui writes {@code properties}, while
     * quad's own workflow generator ({@code WorkflowGeneratorService}) writes {@code config}.
     * Every extractor used to read {@code properties} alone, so a generated workflow lost its
     * agent binding, timeout, guard and loop settings without a word. {@code properties} wins
     * on conflict — it is the format the UI sends and the one that was already honoured.
     */
    private static Map<String, Object> attrs(QuarkusNode n) {
        if (n.config().isEmpty()) return n.properties();
        if (n.properties().isEmpty()) return n.config();
        Map<String, Object> merged = new LinkedHashMap<>(n.config());
        merged.putAll(n.properties());
        return merged;
    }

    private static Object attr(QuarkusNode n, String key) {
        return attrs(n).get(key);
    }

    /**
     * The agent type a step is dispatched under, i.e. the value that selects the topic
     * {@code agent.tasks.<agentType>} and the row in {@code agent_definitions}.
     *
     * <p>Previously this returned {@code "generic"} for every node without a top-level
     * {@code agentType} — silently. Every step of a generated workflow then ran on the
     * auto-registered stub agent (generic prompt, gpt-4o-mini, 1024 tokens) instead of the
     * agent it named, and nothing in the response indicated a substitution.
     *
     * <p>quad's generator stores the binding as {@code config.agentId}, and that id is exactly
     * what {@code AgentDefinitionBridge} registers as the agent type, so it is read as a
     * fallback rather than left to chance.
     */
    private static String agentTypeOf(QuarkusNode n) {
        if (n.agentType() != null && !n.agentType().isBlank()) return n.agentType();
        for (String key : new String[]{"agentType", "agentId", "agent_id"}) {
            Object v = attr(n, key);
            if (v != null && !v.toString().isBlank()) return v.toString();
        }
        log.warn("Node '{}' of type '{}' names no agent; falling back to agent type 'generic'. "
                        + "It will run on whatever is registered for 'generic' "
                        + "(an auto-created stub agent if none exists). Set agentType or "
                        + "config.agentId to bind the real agent.",
                n.id(), n.type());
        return "generic";
    }

    private static Integer timeoutFrom(QuarkusNode n) {
        if (n.timeoutMs() != null) return n.timeoutMs();
        if (n.timeoutSeconds() != null) return n.timeoutSeconds() * 1000;
        // quad's generator writes the timeout into config, in seconds or milliseconds.
        Object ms = attr(n, "timeoutMs");
        if (ms instanceof Number m) return m.intValue();
        Object secs = attr(n, "timeoutSeconds");
        if (secs instanceof Number s) return s.intValue() * 1000;
        return null;
    }

    private static boolean isHitl(QuarkusNode n) {
        if (n.requiresHumanApproval() != null) return n.requiresHumanApproval();
        return Boolean.parseBoolean(String.valueOf(attr(n, "requiresHumanApproval")));
    }

    private static String guardFrom(QuarkusNode n) {
        Object guard = attr(n, "guardCondition");
        return guard != null ? guard.toString() : null;
    }

    private static String joinPolicyFrom(QuarkusNode n) {
        // Check inline property first
        Object policy = attr(n, "joinPolicy");
        if (policy != null) return policy.toString();
        // fall back to the "all" default
        return "all";
    }

    private static String conditionFromEdge(QuarkusEdge.Edge e) {
        return e.condition();
    }

    private static String joinPolicyToJoinType(String policy) {
        return switch (policy.toLowerCase()) {
            case "all" -> "AND";
            case "any" -> "XOR";
            default -> "N_OF";
        };
    }

    private static String exitConditionFrom(QuarkusNode n) {
        Object cond = attr(n, "exitCondition");
        return cond != null ? cond.toString() : null;
    }

    private static Integer maxIterationsFrom(QuarkusNode n) {
        Object maxIter = attr(n, "maxIterations");
        if (maxIter instanceof Integer i) return i;
        if (maxIter instanceof Number n2) return n2.intValue();
        return null;
    }
}
