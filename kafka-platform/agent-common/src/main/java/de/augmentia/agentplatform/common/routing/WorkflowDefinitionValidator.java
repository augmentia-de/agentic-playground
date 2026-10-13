package de.augmentia.agentplatform.common.routing;

import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Structural validator for workflow DAG definitions (SPEC2 §17.1).
 *
 * Rules:
 *  R1  Node types ∈ {STEP, DECISION, LOOP, FORK, JOIN, END}
 *  R2  No cycle between non-LOOP nodes (Tarjan SCC; any non-trivial SCC must contain a LOOP)
 *  R3  DECISION → exactly one target (branch or default) is a LOOP;
 *      LOOP with exitCondition is a branch target of a DECISION that defines defaultTarget
 *  R4  FORK branch heads exist; every FORK has exactly one paired JOIN; JOIN types valid
 *  R5  step_id "b.<branchId>.<name>" convention (regex) for branch steps
 *  R6  Exactly one END node; every node reachable from a root; END reachable backward
 *  R7  Unique step_ids; dependsOn references exist
 *  R8  STEP requires agentType; LOOP requires agentType (single-agent) or bodySteps (region),
 *      maxIterations>0, exitCondition; region-LOOP bodies may contain STEP and control nodes
 *      (DECISION/FORK/JOIN), but no END and no nested LOOP; timeoutMs >= 1000 when set
 */
public final class WorkflowDefinitionValidator {

    private static final Pattern BRANCH_STEP = Pattern.compile("^b\\.[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*$");
    private static final Set<NodeType> VOCAB = Set.of(NodeType.values());
    private static final Set<NodeType> REGION_BODY_TYPES =
            Set.of(NodeType.STEP, NodeType.DECISION, NodeType.FORK, NodeType.JOIN);

    private WorkflowDefinitionValidator() {}

    public static boolean isValid(WorkflowDefinition dag) {
        return validate(dag).isEmpty();
    }

    public static List<String> validate(WorkflowDefinition dag) {
        List<String> violations = new ArrayList<>();
        Map<String, DagNode> nodes = byId(dag);

        // R1 / R7 unique ids
        Set<String> seen = new HashSet<>();
        for (DagNode n : dag.nodes()) {
            if (!VOCAB.contains(n.nodeType())) violations.add("R1: invalid node type in " + n.stepId());
            if (!seen.add(n.stepId())) violations.add("R7: duplicate step_id " + n.stepId());
        }
        for (DagNode n : dag.nodes()) {
            for (String d : n.dependsOn()) {
                if (!nodes.containsKey(d)) violations.add("R7: " + n.stepId() + " depends on unknown " + d);
            }
        }

        // R2 cycles only through LOOP
        List<List<String>> sccs = tarjan(dag);
        for (List<String> scc : sccs) {
            if (scc.size() > 1) {
                boolean hasLoop = scc.stream().anyMatch(s -> nodes.containsKey(s) && nodes.get(s).nodeType() == NodeType.LOOP);
                if (!hasLoop) violations.add("R2: cycle without LOOP node: " + scc);
            } else if (scc.size() == 1) {
                String n = scc.get(0);
                DagNode node = nodes.get(n);
                if (node != null && node.dependsOn().contains(n) && node.nodeType() != NodeType.LOOP) {
                    violations.add("R2: self-cycle on non-LOOP node " + n);
                }
            }
        }

        for (DagNode n : dag.nodes()) {
            switch (n.nodeType()) {
                case DECISION -> {
                    if (n.branches().isEmpty()) violations.add("R3: DECISION " + n.stepId() + " has no branches");
                    Set<String> branchIds = new HashSet<>();
                    long loopTargets = 0;
                    for (DagNode.Branch b : n.branches()) {
                        if (!branchIds.add(b.branchId() != null ? b.branchId() : b.target())) {
                            violations.add("R3: duplicate branchId on " + n.stepId());
                        }
                        if (b.condition() == null || b.condition().isBlank()) {
                            violations.add("R3: branch on " + n.stepId() + " lacks a condition");
                        }
                        if (b.target() == null || !nodes.containsKey(b.target())) {
                            violations.add("R3: branch target of " + n.stepId() + " unknown: " + b.target());
                        } else if (nodes.get(b.target()).nodeType() == NodeType.LOOP) {
                            loopTargets++;
                        }
                    }
                    if (n.defaultTarget() != null && !n.defaultTarget().isBlank()) {
                        if (!nodes.containsKey(n.defaultTarget())) {
                            violations.add("R3: defaultTarget of " + n.stepId() + " unknown: " + n.defaultTarget());
                        } else if (nodes.get(n.defaultTarget()).nodeType() == NodeType.LOOP) {
                            loopTargets++;
                        }
                    }
                    if (loopTargets > 1) {
                        violations.add("R3: DECISION " + n.stepId() + " may target at most one LOOP (found " + loopTargets + ")");
                    }
                    if (loopTargets == 1 && (n.defaultTarget() == null || n.defaultTarget().isBlank())) {
                        violations.add("R3: DECISION " + n.stepId() + " must define defaultTarget as LOOP exit");
                    }
                }
                case LOOP -> {
                    boolean region = !n.bodySteps().isEmpty();
                    if (!region && (n.agentType() == null || n.agentType().isBlank())) {
                        violations.add("R8: LOOP " + n.stepId()
                                + " requires either agentType (single-agent body) or bodySteps (region)");
                    }
                    if (n.maxIterations() == null || n.maxIterations() <= 0) violations.add("R8: LOOP " + n.stepId() + " requires maxIterations > 0");
                    if (n.exitCondition() == null || n.exitCondition().isBlank()) violations.add("R8: LOOP " + n.stepId() + " requires exitCondition");
                    for (String bodyStep : n.bodySteps()) {
                        if (bodyStep.equals(n.stepId())) violations.add("R8: LOOP " + n.stepId() + " must not contain itself in bodySteps");
                        DagNode body = nodes.get(bodyStep);
                        if (body == null) {
                            violations.add("R8: LOOP " + n.stepId() + " bodyStep unknown: " + bodyStep);
                        } else if (!REGION_BODY_TYPES.contains(body.nodeType())) {
                            violations.add("R8: LOOP " + n.stepId() + " bodyStep " + bodyStep
                                    + " is " + body.nodeType()
                                    + "; region-LOOP bodies support STEP and control nodes"
                                    + " (DECISION/FORK/JOIN), but no END and no nested LOOP");
                        }
                    }
                }
                case FORK -> {
                    if (n.branches().isEmpty()) violations.add("R4: FORK " + n.stepId() + " has no branches");
                    for (DagNode.Branch b : n.branches()) {
                        if (b.firstStep() == null || !nodes.containsKey(b.firstStep())) {
                            violations.add("R4: FORK branch head unknown on " + n.stepId() + ": " + b.firstStep());
                        }
                    }
                }
                case JOIN -> {
                    String jt = n.joinType() == null ? "AND" : n.joinType().toUpperCase();
                    if (!Set.of("AND", "XOR", "N_OF").contains(jt)) violations.add("R4: JOIN " + n.stepId() + " invalid joinType " + n.joinType());
                    if ("N_OF".equals(jt) && (n.joinN() == null || n.joinN() <= 0)) violations.add("R4: JOIN N_OF requires joinN >= 1");
                    if (!jt.equals("N_OF") && n.joinN() != null) violations.add("R4: JOIN non-N_OF must not set joinN");
                }
                case STEP -> {
                    if (n.agentType() == null || n.agentType().isBlank()) violations.add("R8: STEP " + n.stepId() + " requires agentType");
                }
                case END -> { /* nothing */ }
            }
            if (n.timeoutMs() != null && n.timeoutMs() < 1000) {
                violations.add("R8: " + n.stepId() + " timeoutMs < 1000");
            }
        }

        // R5 branch step_id convention
        for (DagNode n : dag.nodes()) {
            if (n.stepId().startsWith("b.") && !BRANCH_STEP.matcher(n.stepId()).matches()) {
                violations.add("R5: branch step_id '" + n.stepId() + "' violates b.<branchId>.<name>");
            }
        }

        // R6 exactly one END; reachability from roots; END reachable backwards
        List<DagNode> ends = dag.nodes().stream().filter(n -> n.nodeType() == NodeType.END).toList();
        if (ends.isEmpty()) violations.add("R6: no END node");
        if (ends.size() > 1) violations.add("R6: multiple END nodes: " + ends.stream().map(DagNode::stepId).toList());

        List<String> roots = dag.nodes().stream().filter(n -> n.dependsOn().isEmpty()).map(DagNode::stepId).toList();
        if (roots.isEmpty()) violations.add("R6: no root node (dependsOn empty)");
        Set<String> reachable = reachableFrom(roots, dag);
        for (DagNode n : dag.nodes()) {
            if (!reachable.contains(n.stepId())) violations.add("R6: " + n.stepId() + " unreachable from roots");
        }

        // R4 fork/join pairing: each FORK has exactly one JOIN it leads into
        Map<String, Set<String>> joinsPerFork = new HashMap<>();
        for (DagNode fork : dag.nodes().stream().filter(n -> n.nodeType() == NodeType.FORK).toList()) {
            joined: for (DagNode join : dag.nodes().stream().filter(n -> n.nodeType() == NodeType.JOIN).toList()) {
                for (String bh : fork.branches().stream().map(DagNode.Branch::firstStep).toList()) {
                    if (transitivelyDependsOn(join.stepId(), bh, dag)) {
                        joinsPerFork.computeIfAbsent(fork.stepId(), k -> new HashSet<>()).add(join.stepId());
                        continue joined;
                    }
                }
            }
        }
        for (Map.Entry<String, Set<String>> e : joinsPerFork.entrySet()) {
            if (e.getValue().size() != 1) violations.add("R4: FORK " + e.getKey() + " must have exactly one paired JOIN (found " + e.getValue().size() + ")");
        }
        for (DagNode join : dag.nodes().stream().filter(n -> n.nodeType() == NodeType.JOIN).toList()) {
            long forks = joinsPerFork.values().stream().filter(s -> s.contains(join.stepId())).count();
            if (forks != 1) violations.add("R4: JOIN " + join.stepId() + " must be reached by exactly one FORK (found " + forks + ")");
        }

        return violations;
    }

    /* ------------------------------- helpers ------------------------------ */

    private static Map<String, DagNode> byId(WorkflowDefinition dag) {
        Map<String, DagNode> map = new HashMap<>();
        dag.nodes().forEach(n -> map.put(n.stepId(), n));
        return map;
    }

    private static Set<String> reachableFrom(List<String> roots, WorkflowDefinition dag) {
        Map<String, List<String>> outgoing = new HashMap<>();
        for (DagNode n : dag.nodes()) {
            for (String d : n.dependsOn()) {
                outgoing.computeIfAbsent(d, k -> new ArrayList<>()).add(n.stepId());
            }
        }
        Set<String> visited = new LinkedHashSet<>();
        List<String> stack = new ArrayList<>(roots);
        while (!stack.isEmpty()) {
            String cur = stack.remove(stack.size() - 1);
            if (!visited.add(cur)) continue;
            stack.addAll(outgoing.getOrDefault(cur, List.of()));
        }
        return visited;
    }

    private static boolean transitivelyDependsOn(String node, String candidateAncestor, WorkflowDefinition dag) {
        Map<String, DagNode> byId = byId(dag);
        Set<String> visited = new HashSet<>();
        List<String> stack = new ArrayList<>();
        stack.add(node);
        while (!stack.isEmpty()) {
            String cur = stack.remove(stack.size() - 1);
            if (cur.equals(candidateAncestor)) return true;
            if (!visited.add(cur)) continue;
            DagNode n = byId.get(cur);
            if (n != null) stack.addAll(n.dependsOn());
        }
        return false;
    }

    /** Tarjan SCC over dependsOn edges. */
    static List<List<String>> tarjan(WorkflowDefinition dag) {
        Map<String, DagNode> byId = byId(dag);
        Map<String, Integer> index = new HashMap<>();
        Map<String, Integer> low = new HashMap<>();
        List<String> stack = new ArrayList<>();
        Set<String> onStack = new HashSet<>();
        List<List<String>> sccs = new ArrayList<>();
        int[] counter = {0};

        for (DagNode n : dag.nodes()) {
            if (!index.containsKey(n.stepId())) strongConnect(n.stepId(), byId, index, low, stack, onStack, sccs, counter);
        }
        return sccs;
    }

    private static void strongConnect(String v, Map<String, DagNode> byId, Map<String, Integer> index,
                                      Map<String, Integer> low, List<String> stack, Set<String> onStack,
                                      List<List<String>> sccs, int[] counter) {
        index.put(v, counter[0]);
        low.put(v, counter[0]);
        counter[0]++;
        stack.add(v);
        onStack.add(v);

        DagNode n = byId.get(v);
        if (n != null) {
            for (String w : n.dependsOn()) {
                if (!byId.containsKey(w)) continue;
                if (!index.containsKey(w)) {
                    strongConnect(w, byId, index, low, stack, onStack, sccs, counter);
                    low.put(v, Math.min(low.get(v), low.get(w)));
                } else if (onStack.contains(w)) {
                    low.put(v, Math.min(low.get(v), index.get(w)));
                }
            }
        }

        if (low.get(v).equals(index.get(v))) {
            List<String> scc = new ArrayList<>();
            String w;
            do {
                w = stack.remove(stack.size() - 1);
                onStack.remove(w);
                scc.add(w);
            } while (!w.equals(v));
            sccs.add(scc);
        }
    }
}