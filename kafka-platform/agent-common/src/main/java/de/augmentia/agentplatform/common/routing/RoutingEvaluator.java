package de.augmentia.agentplatform.common.routing;

import de.augmentia.agentplatform.common.condition.ExpressionEvaluator;
import de.augmentia.agentplatform.common.model.CheckpointSnapshot;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure deterministic routing evaluator (SPEC2 §15.3).
 *
 * fireableSteps = evaluateRouting(persistedCheckpointState, dagDefinition)
 *
 * No side effects: reads only the persisted checkpoint map + pinned definition.
 * Same inputs always produce the same ordered result (G3/G4).
 */
public final class RoutingEvaluator {

    /** Action: a step (or first loop iteration) to be dispatched to an agent. */
    public record Fire(String stepId, NodeType nodeType, String agentType, int iteration) {}

    /** A DECISION that must be persisted: stepId + chosen target stepId (§15.3 decision_branch). */
    /**
 * A DECISION step that was taken.
 *
 * @param stepId        the DECISION node itself
 * @param branchId      the chosen branch ID, or {@link #DEFAULT_BRANCH_ID},
 *                      if the {@code defaultTarget} was taken. That makes it
 *                      readable from the persistence <em>which</em> branch ran —
 *                      previously this information was lost.
 * @param chosenTarget  the target step of the chosen branch
 */
public record Decision(String stepId, String branchId, String chosenTarget, int iteration) {

    /** branchId when no condition matched and the defaultTarget was used. */
    public static final String DEFAULT_BRANCH_ID = "default";

    public Decision(String stepId, String chosenTarget) {
        this(stepId, null, chosenTarget, 1);
    }

    public Decision(String stepId, String branchId, String chosenTarget) {
        this(stepId, branchId, chosenTarget, 1);
    }
}

    /** A step to be marked SKIPPED (guard / cascade, prune=false) or PRUNED (unchosen branch, prune=true). */
    public record Prune(String stepId, NodeType nodeType, boolean prune, String reason, int iteration) {
        public Prune(String stepId, NodeType nodeType, boolean prune, String reason) {
            this(stepId, nodeType, prune, reason, 1);
        }
    }

    /** A routing node (FORK/JOIN/END or region-LOOP) auto-completed in {@code iteration}. */
    public record CompletedStep(String stepId, int iteration) {}

    /** A region-LOOP currently running in {@code iteration} (routing-only, never dispatched). */
    public record LoopProgress(String stepId, int iteration) {}

    /** Result of one evaluateRouting pass. */
    public record Result(
            List<Fire> fired,
            List<Decision> decisions,
            List<Prune> pruned,
            List<CompletedStep> completedSteps,
            List<LoopProgress> runningLoops,
            boolean workflowEndReached
    ) {
        /** Back-compat view of the auto-completed step ids, in evaluation order. */
        public List<String> completed() {
            return completedSteps.stream().map(CompletedStep::stepId).toList();
        }
    }

    private RoutingEvaluator() {}

    public static List<DagNode> orderedNodes(WorkflowDefinition dag) {
        return sortByLevel(dag);
    }

    /**
     * Evaluate routing state once (fixpoint iteration).
     *
     * @param state   persisted checkpoints keyed by stepId; absent = PENDING
     * @param dag     pinned workflow definition (G5)
     * @param context accumulated workflow output context (context['stepId'].field...)
     * @param input   execution input params
     */
    public static Result evaluate(Map<String, CheckpointSnapshot> state, WorkflowDefinition dag,
                                  Map<String, Object> context, Map<String, Object> input) {
        Map<String, CheckpointSnapshot> work = new HashMap<>(state);
        Map<String, DagNode> nodes = new HashMap<>();
        dag.nodes().forEach(n -> nodes.put(n.stepId(), n));
        Region region = Region.of(dag, work);

        List<Fire> fired = new ArrayList<>();
        List<Decision> decisions = new ArrayList<>();
        List<Prune> pruned = new ArrayList<>();
        List<CompletedStep> completed = new ArrayList<>();
        Map<String, Integer> runningLoops = new LinkedHashMap<>();

        List<DagNode> sorted = sortByLevel(dag);
        boolean endReached = false;
        boolean progressed = true;
        int guard = 0;
        int maxPasses = 8 * Math.max(1, dag.nodes().size());

        while (progressed && guard++ < maxPasses) {
            progressed = false;
            for (DagNode s : sorted) {
                CheckpointSnapshot st = work.get(s.stepId());
                int iteration = region.iterationFor(s.stepId());

                // A body node whose stored checkpoint belongs to an earlier iteration is
                // stale: the region advanced past it, so it must run again in the current
                // iteration. It is therefore not skipped as terminal (the engine evaluates
                // incrementally per result, so the re-arm PENDING reset is not replayed).
                // This applies to every body node type (STEP and control nodes alike).
                boolean staleBodyNode = staleBodyNode(s.stepId(), st, region, iteration);

                if (st != null && terminalForRouting(st.status()) && !staleBodyNode) continue;

                // §15.3 cascade: a PENDING node whose deps are ALL dead (pruned/skipped)
                // can never mature -> mark SKIPPED ("rekursiv SKIPPED").
                if (st == null && allDepsTerminalDead(s, nodes, work, region) && s.nodeType() != NodeType.END) {
                    work.put(s.stepId(), snapshot(st, s, CheckpointStatus.SKIPPED, "decision:cascade", iteration));
                    pruned.add(new Prune(s.stepId(), s.nodeType(), false, "decision:cascade", iteration));
                    progressed = true;
                    continue;
                }

                switch (s.nodeType()) {
                    case STEP -> {
                        if (isSatisfied(s, nodes, work, region)
                                && (st == null || st.status() == CheckpointStatus.PENDING || staleBodyNode)) {
                            if (guardFails(s, context, input)) {
                                work.put(s.stepId(), snapshot(st, s, CheckpointStatus.SKIPPED, "guard:false", iteration));
                                pruned.add(new Prune(s.stepId(), s.nodeType(), false, "guard:false", iteration));
                                progressed = true;
                            } else {
                                addFire(fired, s, iteration);
                                work.put(s.stepId(), snapshot(st, s, CheckpointStatus.IN_PROGRESS, null, iteration));
                                progressed = true;
                            }
                        }
                    }
                    case LOOP -> {
                        if (s.isRegionLoop()) {
                            // Routing-only region loop: repeats the bodySteps region for N iterations.
                            if (regionLoopStep(s, nodes, work, region, context, input, completed, runningLoops)) {
                                progressed = true;
                            }
                        } else if (isSatisfied(s, nodes, work, region)
                                && (st == null || st.status() == CheckpointStatus.PENDING)) {
                            // Cooperative single-agent loop: first iteration fires like a STEP to the
                            // loop's agentType; later iterations are dispatched by the loop handler
                            // via agent.control.loop (§15.4).
                            int it = st == null ? 1 : st.iteration();
                            addFire(fired, s, it);
                            work.put(s.stepId(), snapshot(st, s, CheckpointStatus.IN_PROGRESS, null, it));
                            progressed = true;
                        }
                    }
                    case DECISION -> {
                        if (isSatisfied(s, nodes, work, region)
                                && (st == null || st.decisionBranch() == null || staleBodyNode)) {
                            Chosen chosen = chooseTarget(s, context, input);
                            if (chosen != null) {
                                decisions.add(new Decision(s.stepId(), chosen.branchId(), chosen.target(), iteration));
                                work.put(s.stepId(),
                                        snapshot(st, s, CheckpointStatus.COMPLETED, chosen.target(), iteration));
                                pruneOtherBranches(s, chosen.target(), nodes, work, pruned, region);
                                progressed = true;
                            }
                        }
                    }
                    case FORK, JOIN -> {
                        // A control node in a loop body re-completes once per iteration;
                        // stale (previous-iteration) completions are re-armed, not skipped.
                        if (isSatisfied(s, nodes, work, region)
                                && (st == null || st.status() != CheckpointStatus.COMPLETED || staleBodyNode)) {
                            work.put(s.stepId(), snapshot(st, s, CheckpointStatus.COMPLETED, null, iteration));
                            completed.add(new CompletedStep(s.stepId(), iteration));
                            if (s.nodeType() == NodeType.JOIN) pruneJoinLosers(s, work, nodes, pruned, region);
                            progressed = true;
                        }
                    }
                    case END -> {
                        if (isSatisfied(s, nodes, work, region) && !endReached) {
                            work.put(s.stepId(), snapshot(st, s, CheckpointStatus.COMPLETED, null, iteration));
                            completed.add(new CompletedStep(s.stepId(), iteration));
                            endReached = true;
                            progressed = true;
                        }
                    }
                }
            }
        }
        List<LoopProgress> loops = runningLoops.entrySet().stream()
                .map(e -> new LoopProgress(e.getKey(), e.getValue()))
                .toList();
        return new Result(fired, decisions, pruned, completed, loops, endReached);
    }

    /**
     * Region LOOP: repeats the {@code bodySteps} region until the exit condition matches
     * or {@code maxIterations} is reached. The LOOP node is routing-only — it
     * never dispatches to a worker but controls the body execution.
     *
     * <p>All body steps of an iteration carry the same iteration number. If the region
     * of an iteration is complete, the loop either becomes COMPLETED (exit/max) or the
     * region is reset to iteration+1 (re-arm); the body entry then fires again in the
     * same pass.
     *
     * @return true if the routing state has changed
     */
    private static boolean regionLoopStep(DagNode loop, Map<String, DagNode> nodes,
                                          Map<String, CheckpointSnapshot> work, Region region,
                                          Map<String, Object> context, Map<String, Object> input,
                                          List<CompletedStep> completed, Map<String, Integer> runningLoops) {
        CheckpointSnapshot lst = work.get(loop.stepId());
        if (lst != null && lst.status() == CheckpointStatus.COMPLETED) return false;
        if (!isSatisfied(loop, nodes, work, region)) return false;

        int cur = region.loopIteration(loop.stepId());
        boolean allDone = true;
        for (String b : loop.bodySteps()) {
            if (!iterationDone(work.get(b), cur)) {
                allDone = false;
                break;
            }
        }

        if (!allDone) {
            // keep the loop active so the body may fire; idempotent across fixpoint passes
            runningLoops.put(loop.stepId(), cur);
            if (lst == null || lst.status() != CheckpointStatus.IN_PROGRESS || lst.iteration() != cur) {
                work.put(loop.stepId(), snapshot(lst, loop, CheckpointStatus.IN_PROGRESS, null, cur));
                return true;
            }
            return false;
        }

        boolean exit = loop.exitCondition() != null && !loop.exitCondition().isBlank()
                && ExpressionEvaluator.eval(loop.exitCondition(), context, input);
        int maxIter = loop.maxIterations() == null ? 1 : loop.maxIterations();
        if (exit || cur >= maxIter) {
            work.put(loop.stepId(), snapshot(lst, loop, CheckpointStatus.COMPLETED, null, cur));
            completed.add(new CompletedStep(loop.stepId(), cur));
            runningLoops.remove(loop.stepId());
            return true;
        }

        int next = cur + 1;
        region.advance(loop.stepId(), next);
        for (String b : loop.bodySteps()) {
            DagNode bn = nodes.get(b);
            if (bn != null) work.put(b, snapshot(work.get(b), bn, CheckpointStatus.PENDING, null, next));
        }
        work.put(loop.stepId(), snapshot(lst, loop, CheckpointStatus.IN_PROGRESS, null, next));
        runningLoops.put(loop.stepId(), next);
        return true;
    }

    /** A body step is done for the running iteration (also SKIPPED/PRUNED). */
    private static boolean iterationDone(CheckpointSnapshot bs, int cur) {
        if (bs == null || bs.iteration() != cur) return false;
        return switch (bs.status()) {
            case COMPLETED, SKIPPED, PRUNED, CANCELLED -> true;
            default -> false;
        };
    }

    /**
     * Iteration assignment of the region LOOPs: which body step belongs to which LOOP and
     * which iteration the region is currently going through. Derived from the persisted body
     * checkpoints (the highest occurring iteration), so that the iteration survives restarts.
     */
    private static final class Region {
        private final Map<String, String> bodyOwner = new HashMap<>();
        private final Map<String, Integer> iteration = new HashMap<>();

        static Region of(WorkflowDefinition dag, Map<String, CheckpointSnapshot> work) {
            Region r = new Region();
            for (DagNode n : dag.nodes()) {
                if (!n.isRegionLoop()) continue;
                int cur = 1;
                for (String b : n.bodySteps()) {
                    r.bodyOwner.put(b, n.stepId());
                    CheckpointSnapshot bs = work.get(b);
                    if (bs != null && bs.iteration() > cur) cur = bs.iteration();
                }
                r.iteration.put(n.stepId(), cur);
            }
            return r;
        }

        int iterationFor(String stepId) {
            String owner = bodyOwner.get(stepId);
            if (owner == null) return 1;
            return iteration.getOrDefault(owner, 1);
        }

        /** The iteration the LOOP itself is currently going through. */
        int loopIteration(String loopStepId) {
            return iteration.getOrDefault(loopStepId, 1);
        }

        void advance(String loopStepId, int next) {
            iteration.put(loopStepId, next);
        }

        boolean isBodyStep(String stepId) {
            return bodyOwner.containsKey(stepId);
        }
    }

    /* --------------------- status / terminality --------------------------- */

    private static boolean terminalForRouting(CheckpointStatus s) {
        return s == CheckpointStatus.COMPLETED
                || s == CheckpointStatus.SKIPPED
                || s == CheckpointStatus.PRUNED
                || s == CheckpointStatus.CANCELLED
                || s == CheckpointStatus.WAITING_HITL
                || s == CheckpointStatus.FAILED;
    }

    /**
     * A body node whose stored checkpoint originates from an earlier iteration of its
     * region LOOP. The region has meanwhile advanced, so the node must
     * run/be evaluated again in the current iteration instead of counting as terminal.
     */
    private static boolean staleBodyNode(String stepId, CheckpointSnapshot st, Region region, int iteration) {
        if (st == null || !region.isBodyStep(stepId) || st.iteration() >= iteration) return false;
        return switch (st.status()) {
            case COMPLETED, SKIPPED, PRUNED, CANCELLED -> true;
            default -> false;
        };
    }

    /* ------------------------------ guards -------------------------------- */

    private static boolean guardFails(DagNode s, Map<String, Object> context, Map<String, Object> input) {
        if (s.guardCondition() == null || s.guardCondition().isBlank()) return false;
        return !ExpressionEvaluator.eval(s.guardCondition(), context, input);
    }

    /* -------------------------- branch selection -------------------------- */

    /**
     * Deterministically choose the active target of a DECISION node (§15.3:
     * decision_branch = chosen target stepId). First true condition in declaration
     * order wins; otherwise defaultTarget.
     */
    /** Result of the DECISION selection: chosen branch ID and its target. */
    private record Chosen(String branchId, String target) {}

    private static Chosen chooseTarget(DagNode s, Map<String, Object> context, Map<String, Object> input) {
        for (DagNode.Branch b : s.branches()) {
            if (b.condition() != null && ExpressionEvaluator.eval(b.condition(), context, input)) {
                // branchId can be missing in a definition; then we replace it
                // with the target so that the column is never empty.
                return new Chosen(b.branchId() == null ? b.target() : b.branchId(), b.target());
            }
        }
        if (s.defaultTarget() != null && !s.defaultTarget().isBlank()) {
            return new Chosen(Decision.DEFAULT_BRANCH_ID, s.defaultTarget());
        }
        return null;
    }

    /* ---------------------------- pruning --------------------------------- */

    /** Prune non-chosen DECISION branch subtrees ("b.<branchId>.<name>" convention, §15.3). */
    private static void pruneOtherBranches(DagNode s, String chosenTarget, Map<String, DagNode> nodes,
                                           Map<String, CheckpointSnapshot> work, List<Prune> pruned,
                                           Region region) {
        for (DagNode.Branch b : s.branches()) {
            if (b.target() != null && b.target().equals(chosenTarget)) continue;
            String branchId = b.branchId() == null ? b.target() : b.branchId();
            if (branchId == null) continue;
            for (String stepId : List.copyOf(nodes.keySet())) {
                boolean owns = stepId.startsWith("b." + branchId + ".");
                boolean direct = b.target() != null && stepId.equals(b.target());
                if (owns || direct) skipNext(stepId, work, nodes, pruned, region);
            }
        }
        // The default target of a DECISION is the loop-exit continuation (e.g. parallel_fork
        // in §15.7) and must stay live; only loopless if/else decisions prune their default path.
        boolean feedsLoop = s.branches().stream().anyMatch(b -> {
            DagNode t = nodes.get(b.target());
            return t != null && t.nodeType() == NodeType.LOOP;
        });
        if (!feedsLoop && s.defaultTarget() != null && !s.defaultTarget().equals(chosenTarget)) {
            skipNext(s.defaultTarget(), work, nodes, pruned, region);
        }
    }

    /** After an AND/XOR/N_OF join fires, prune remaining non-completed branches (§15.5). */
    private static void pruneJoinLosers(DagNode s, Map<String, CheckpointSnapshot> work,
                                        Map<String, DagNode> nodes, List<Prune> pruned, Region region) {
        if (s.joinType() == null) return;
        String jt = s.joinType().toUpperCase();
        if (!jt.equals("XOR") && !jt.equals("N_OF")) return;
        for (String d : s.dependsOn()) {
            CheckpointSnapshot ds = work.get(d);
            if (ds == null || ds.status() == CheckpointStatus.COMPLETED) continue;
            if (!ds.status().isDead()) pruneNext(d, work, nodes, pruned, region);
        }
    }

    private static void skipNext(String stepId, Map<String, CheckpointSnapshot> work,
                                 Map<String, DagNode> nodes, List<Prune> pruned, Region region) {
        CheckpointSnapshot st = work.get(stepId);
        int iteration = region.iterationFor(stepId);
        if (st == null || st.status() == CheckpointStatus.PENDING || st.status() == CheckpointStatus.IN_PROGRESS
                || staleBodyNode(stepId, st, region, iteration)) {
            DagNode n = nodes.get(stepId);
            if (n == null) return;
            work.put(stepId, snapshot(st, n, CheckpointStatus.SKIPPED, "decision:not-chosen", iteration));
            pruned.add(new Prune(stepId, n.nodeType(), false, "decision:not-chosen", iteration));
        }
    }

    private static void pruneNext(String stepId, Map<String, CheckpointSnapshot> work,
                                  Map<String, DagNode> nodes, List<Prune> pruned, Region region) {
        CheckpointSnapshot st = work.get(stepId);
        int iteration = region.iterationFor(stepId);
        if (st == null || st.status() == CheckpointStatus.PENDING || st.status() == CheckpointStatus.IN_PROGRESS
                || staleBodyNode(stepId, st, region, iteration)) {
            DagNode n = nodes.get(stepId);
            if (n == null) return;
            work.put(stepId, snapshot(st, n, CheckpointStatus.PRUNED, "pruned", iteration));
            pruned.add(new Prune(stepId, n.nodeType(), true, "pruned", iteration));
        }
    }

    /* -------------------------- satisfaction ------------------------------- */

    private static boolean isSatisfied(DagNode s, Map<String, DagNode> nodes,
                                       Map<String, CheckpointSnapshot> work, Region region) {
        if (s.dependsOn() == null || s.dependsOn().isEmpty()) return true;

        if (s.nodeType() == NodeType.FORK) {
            for (String d : s.dependsOn()) {
                if (edgeCompleted(nodes.get(d), work.get(d), s.stepId(), nodes, work, region)) return true;
            }
            return false;
        }

        if (s.nodeType() == NodeType.JOIN) {
            List<String> incoming = new ArrayList<>();
            for (String d : s.dependsOn()) {
                CheckpointSnapshot ds = work.get(d);
                if (ds == null) { incoming.add(d); continue; }
                if (!ds.status().isDead()) incoming.add(d);
            }
            List<String> effective = new ArrayList<>();
            for (String d : incoming) {
                CheckpointSnapshot ds = work.get(d);
                if (ds != null && ds.status().isEffective()) effective.add(d);
            }
            String joinType = s.joinType() == null ? "AND" : s.joinType().toUpperCase();
            return switch (joinType) {
                case "AND" -> effective.size() == incoming.size();
                case "XOR" -> effective.stream().anyMatch(d -> {
                    CheckpointSnapshot ds = work.get(d);
                    return ds != null && ds.status() == CheckpointStatus.COMPLETED;
                });
                case "N_OF" -> {
                    int n = s.joinN() == null ? 1 : s.joinN();
                    yield effective.size() >= n;
                }
                default -> effective.size() == incoming.size();
            };
        }

        for (String d : s.dependsOn()) {
            if (!edgeCompleted(nodes.get(d), work.get(d), s.stepId(), nodes, work, region)) return false;
        }
        return true;
    }

    /**
     * edgeCompleted(d → s): the dependency is COMPLETED or SKIPPED (effective, §15.3/§15.5);
     * DECISION dependencies only count as edges to the chosen target (§15.3 active edge).
     *
     * <p>Region LOOPs change that:
     * <ul>
     *   <li>An edge from the LOOP into its body is valid as long as the loop is running
     *       (IN_PROGRESS) — a COMPLETED loop must not open its body again.
     *       Nodes <em>outside</em> the region, by contrast, are only released by the LOOP at
     *       COMPLETED.</li>
     *   <li>A body step only releases its consumers <em>outside</em> the region once
     *       the loop is COMPLETED; within the region the normal rules apply,
     *       so that step-to-step edges in the body take effect per iteration.</li>
     * </ul>
     */
    private static boolean edgeCompleted(DagNode dep, CheckpointSnapshot ds, String targetStepId,
                                         Map<String, DagNode> nodes, Map<String, CheckpointSnapshot> work,
                                         Region region) {
        if (dep == null) return false;

        String depOwner = region.bodyOwner.get(dep.stepId());
        if (depOwner != null) {
            String targetOwner = region.bodyOwner.get(targetStepId);
            if (targetOwner == null || !depOwner.equals(targetOwner)) {
                CheckpointSnapshot ls = work.get(depOwner);
                return ls != null && ls.status() == CheckpointStatus.COMPLETED;
            }
            // Same region: the dependency must be done in the loop's *current* iteration,
            // otherwise a stale COMPLETED checkpoint from iteration N-1 would release
            // step N prematurely (e.g. a4 firing before a3 ran again).
            if (ds == null) return false;
            if (ds.status() != CheckpointStatus.COMPLETED && ds.status() != CheckpointStatus.SKIPPED) {
                return false;
            }
            return ds.iteration() >= region.loopIteration(depOwner);
        } else if (dep.isRegionLoop()) {
            if (ds == null) return false;
            // The loop only opens its own region: body nodes may start while it runs,
            // consumers outside the body wait until the loop is done.
            if (region.isBodyStep(targetStepId)) {
                return ds.status() == CheckpointStatus.IN_PROGRESS;
            }
            return ds.status() == CheckpointStatus.COMPLETED;
        }

        if (ds == null) return false;
        if (ds.status() != CheckpointStatus.COMPLETED && ds.status() != CheckpointStatus.SKIPPED) return false;
        if (dep.nodeType() == NodeType.DECISION) {
            return ds.decisionBranch() != null && ds.decisionBranch().equals(targetStepId);
        }
        return true;
    }

    /** All deps present and hard-dead (PRUNED or CANCELLED); guard-SKIPPED deps are effective (§15.3). */
    private static boolean allDepsTerminalDead(DagNode s, Map<String, DagNode> nodes,
                                               Map<String, CheckpointSnapshot> work, Region region) {
        if (s.dependsOn() == null || s.dependsOn().isEmpty()) return false;
        for (String d : s.dependsOn()) {
            if (region.isBodyStep(d)) return false; // loop-body deps are gated by the loop, never dead
            CheckpointSnapshot ds = work.get(d);
            if (ds == null || !isDeadForCascade(ds)) return false;
        }
        return true;
    }

    private static boolean isDeadForCascade(CheckpointSnapshot snapshot) {
        if (snapshot.status() == CheckpointStatus.PRUNED || snapshot.status() == CheckpointStatus.CANCELLED) {
            return true;
        }
        return snapshot.status() == CheckpointStatus.SKIPPED
                && snapshot.decisionBranch() != null
                && snapshot.decisionBranch().startsWith("decision:");
    }

    /* ---------------------------- sorting --------------------------------- */

    /** Deterministic order: topological level (longest chain) then stepId (G4). */
    private static List<DagNode> sortByLevel(WorkflowDefinition dag) {
        Map<String, DagNode> nodes = new HashMap<>();
        dag.nodes().forEach(n -> nodes.put(n.stepId(), n));
        Map<String, Integer> memo = new HashMap<>();
        for (DagNode n : dag.nodes()) level(n, memo, nodes);
        return dag.nodes().stream()
                .sorted(Comparator.comparingInt((DagNode n) -> memo.getOrDefault(n.stepId(), 0))
                        .thenComparing(DagNode::stepId))
                .toList();
    }

    private static int level(DagNode n, Map<String, Integer> memo, Map<String, DagNode> nodes) {
        Integer cached = memo.get(n.stepId());
        if (cached != null) return cached;
        int max = 0;
        for (String d : n.dependsOn()) {
            DagNode dn = nodes.get(d);
            if (dn != null) max = Math.max(max, level(dn, memo, nodes) + 1);
        }
        memo.put(n.stepId(), Math.max(1, max));
        return memo.get(n.stepId());
    }

    private static void addFire(List<Fire> fired, DagNode s, int iteration) {
        if (fired.stream().noneMatch(f -> f.stepId().equals(s.stepId()))) {
            fired.add(new Fire(s.stepId(), s.nodeType(), s.agentType(), iteration));
        }
    }

    private static CheckpointSnapshot snapshot(CheckpointSnapshot st, DagNode s, CheckpointStatus status,
                                              String branch, int iteration) {
        // COMPLETED keeps the original iteration for in-place updates, but a body node
        // re-completed in a new region iteration must advance to that iteration.
        int it = st == null ? iteration
                : (status == CheckpointStatus.COMPLETED ? Math.max(iteration, st.iteration()) : iteration);
        return new CheckpointSnapshot(
                st == null ? null : st.workflowExecutionId(),
                s.stepId(), s.nodeType(), status, it, branch, s.agentType());
    }
}