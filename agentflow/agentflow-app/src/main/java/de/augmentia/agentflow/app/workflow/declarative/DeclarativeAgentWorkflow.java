package de.augmentia.agentflow.app.workflow.declarative;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Runs a declarative v2 workflow as a durable Temporal workflow, interpreting all node types:
 * {@code agent}, {@code decision}, {@code loop}, {@code fork}, {@code join}, {@code hitl} and
 * {@code end} (sub-workflows are inlined by the planner). Agent execution happens in activities;
 * branches/loops/joins are deterministic workflow code.
 *
 * <p>Nodes run when their {@code dependsOn} dependencies are resolved; {@code decision} nodes prune
 * the non-selected branch targets (transitively, see {@link Impl#pruneCascade}). HITL nodes wait for
 * {@link #decide(String, boolean)} (or a blanket {@link #approve(boolean)}).</p>
 *
 * <p>All workflow state uses insertion-ordered collections and dependency lists are sorted when a
 * context is composed, so a replay sees exactly the same inputs (Temporal determinism).</p>
 */
@WorkflowInterface
public interface DeclarativeAgentWorkflow {

    @WorkflowMethod
    String run(String workflowId, String task);

    /** Blanket approval for all HITL nodes. */
    @SignalMethod
    void approve(boolean approved);

    /** Per-node decision for HITL nodes. */
    @SignalMethod
    void decide(String nodeId, boolean approved);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements DeclarativeAgentWorkflow {

        /**
         * Hard upper bound for any single {@code loop} node, whatever the document declares. Guards
         * against open-ended loops that never meet their exit condition.
         */
        static final int MAX_LOOP_ITERATIONS = 100;

        private final Logger log = Workflow.getLogger(Impl.class);

        private final DeclarativeActivities activities = Workflow.newActivityStub(
                DeclarativeActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        private final Map<String, Boolean> decisions = new LinkedHashMap<>();
        private Boolean approveAll;
        private String rejectedAt;

        @Override
        public String run(String workflowId, String task) {
            DeclarativePlan plan = activities.plan(workflowId);

            Map<String, NodeSpec> byId = new LinkedHashMap<>();
            Set<String> owned = new LinkedHashSet<>();
            for (NodeSpec n : plan.nodes()) {
                byId.put(n.id(), n);
                owned.addAll(n.bodySteps());
            }

            Map<String, String> outputs = new LinkedHashMap<>();
            Set<String> done = new LinkedHashSet<>();
            Set<String> skipped = new LinkedHashSet<>();
            Set<String> pruned = new LinkedHashSet<>();

            boolean progress = true;
            while (progress) {
                progress = false;
                for (NodeSpec node : plan.nodes()) {
                    String id = node.id();
                    if (done.contains(id) || skipped.contains(id) || owned.contains(id)) {
                        continue;
                    }
                    if (pruned.contains(id)) {
                        skipped.add(id);
                        progress = true;
                        continue;
                    }
                    if (!depsResolved(node, done, skipped) || shouldSkip(node, done, skipped)) {
                        if (shouldSkip(node, done, skipped)) {
                            skipped.add(id);
                            progress = true;
                        }
                        continue;
                    }
                    progress = true;
                    execute(node, task, outputs, done, skipped, pruned, byId);
                    if (rejectedAt != null) {
                        return "rejected at node '" + rejectedAt + "'";
                    }
                }
                if (endReached(plan, done)) {
                    break;
                }
            }
            return result(plan, outputs, done);
        }

        private void execute(NodeSpec node, String task, Map<String, String> outputs, Set<String> done,
                             Set<String> skipped, Set<String> pruned, Map<String, NodeSpec> byId) {
            String id = node.id();
            switch (node.type()) {
                case "agent" -> {
                    outputs.put(id, activities.runStep(step(node), context(node, task, outputs)));
                    done.add(id);
                }
                case "hitl" -> {
                    Workflow.await(() -> decisions.containsKey(id) || approveAll != null);
                    boolean ok = decisions.containsKey(id) ? decisions.get(id) : Boolean.TRUE.equals(approveAll);
                    if (!ok) {
                        rejectedAt = id;
                        skipped.add(id);
                        return;
                    }
                    outputs.put(id, context(node, task, outputs));
                    done.add(id);
                }
                case "decision" -> {
                    String chosen = choose(node, context(node, task, outputs));
                    outputs.put(id, chosen == null ? "" : chosen);
                    done.add(id);
                    Set<String> targets = new LinkedHashSet<>();
                    for (Branch b : node.branches()) {
                        if (b.target() != null) {
                            targets.add(b.target());
                        }
                    }
                    if (node.defaultTarget() != null) {
                        targets.add(node.defaultTarget());
                    }
                    for (String t : targets) {
                        if (!t.equals(chosen)) {
                            pruned.add(t);
                        }
                    }
                    pruneCascade(pruned, byId);
                }
                case "join" -> {
                    outputs.put(id, context(node, task, outputs));
                    done.add(id);
                }
                case "fork" -> done.add(id);
                case "loop" -> runLoop(node, task, outputs, done, byId);
                default -> done.add(id);
            }
        }

        /**
         * Executes a {@code loop} node: runs its body steps, feeding each iteration's output into the
         * next, until the exit condition matches or the effective bound is reached. The bound is the
         * declared {@code maxIterations} clamped to {@link #MAX_LOOP_ITERATIONS}; a non-positive
         * value means "unset" and is treated as the hard cap (never silently exactly one iteration).
         * Reaching the cap without meeting the exit condition is logged.
         */
        private void runLoop(NodeSpec node, String task, Map<String, String> outputs,
                             Set<String> done, Map<String, NodeSpec> byId) {
            int iterations = effectiveMaxIterations(node);
            if (node.maxIterations() > MAX_LOOP_ITERATIONS) {
                log.warn("Loop '{}' declares maxIterations={} — capped at the repository limit {}",
                        node.id(), node.maxIterations(), MAX_LOOP_ITERATIONS);
            }
            String last = "";
            int executed = 0;
            for (; executed < iterations; executed++) {
                StringBuilder iteration = new StringBuilder();
                for (String bodyId : node.bodySteps()) {
                    NodeSpec body = byId.get(bodyId);
                    if (body == null) {
                        continue;
                    }
                    String out = activities.runStep(step(body),
                            context(body, task, outputs) + "\n" + iteration);
                    outputs.put(bodyId, out);
                    done.add(bodyId);
                    iteration.append("\n").append(out);
                }
                last = iteration.toString().trim();
                if (ConditionEvaluator.matches(node.exitCondition(), last)) {
                    break;
                }
            }
            if (executed == iterations) {
                log.warn("Loop '{}' stopped at the iteration limit {} without meeting exitCondition '{}'",
                        node.id(), iterations, node.exitCondition());
            }
            outputs.put(node.id(), last);
            done.add(node.id());
        }

        @Override
        public void approve(boolean approved) {
            this.approveAll = approved;
        }

        @Override
        public void decide(String nodeId, boolean approved) {
            this.decisions.put(nodeId, approved);
        }

        // ── helpers (deterministic) ─────────────────────────────────────────────

        private boolean depsResolved(NodeSpec node, Set<String> done, Set<String> skipped) {
            List<String> deps = node.dependsOn();
            if (deps.isEmpty()) {
                return true;
            }
            long resolved = deps.stream().filter(d -> done.contains(d) || skipped.contains(d)).count();
            if ("join".equals(node.type())) {
                long doneCount = deps.stream().filter(done::contains).count();
                return doneCount >= joinNeed(node) || resolved == deps.size();
            }
            return resolved == deps.size();
        }

        private boolean shouldSkip(NodeSpec node, Set<String> done, Set<String> skipped) {
            List<String> deps = node.dependsOn();
            if (deps.isEmpty() || "end".equals(node.type())) {
                return false;
            }
            long resolved = deps.stream().filter(d -> done.contains(d) || skipped.contains(d)).count();
            if ("join".equals(node.type())) {
                if (resolved < deps.size()) {
                    return false;
                }
                long doneCount = deps.stream().filter(done::contains).count();
                return doneCount < joinNeed(node);
            }
            return deps.stream().anyMatch(skipped::contains);
        }

        /**
         * Extends the pruned set transitively: a node whose dependencies <b>all</b> lie in a pruned
         * branch can never run, so it is marked up front — a pruned descendant is never silently left
         * in limbo. {@code end} nodes are deliberately exempt (they still have to close the workflow
         * and emit the result), mirroring {@link #shouldSkip}. Joins needing only a subset
         * ({@code any}/{@code n_of_m}) keep running as long as a non-pruned predecessor exists.
         */
        private void pruneCascade(Set<String> pruned, Map<String, NodeSpec> byId) {
            boolean changed = true;
            while (changed) {
                changed = false;
                for (NodeSpec n : byId.values()) {
                    if (pruned.contains(n.id()) || "end".equals(n.type())) {
                        continue;
                    }
                    List<String> deps = n.dependsOn();
                    if (!deps.isEmpty() && deps.stream().allMatch(pruned::contains)) {
                        pruned.add(n.id());
                        changed = true;
                    }
                }
            }
        }

        /**
         * Effective loop bound: the declared {@code maxIterations} clamped to the repository hard cap
         * ({@link #MAX_LOOP_ITERATIONS}). Non-positive = unset = the hard cap, so an unset loop is
         * bounded rather than silently run exactly once.
         */
        private int effectiveMaxIterations(NodeSpec node) {
            int declared = node.maxIterations();
            return declared <= 0 ? MAX_LOOP_ITERATIONS : Math.min(declared, MAX_LOOP_ITERATIONS);
        }

        private int joinNeed(NodeSpec node) {
            String type = node.joinType() == null ? "all" : node.joinType();
            return switch (type) {
                case "any" -> 1;
                case "n_of_m" -> node.joinN() == null ? 1 : node.joinN();
                default -> node.dependsOn().size();
            };
        }

        private String choose(NodeSpec node, String context) {
            for (Branch b : node.branches()) {
                if (ConditionEvaluator.matches(b.condition(), context)) {
                    return b.target();
                }
            }
            return node.defaultTarget();
        }

        private String context(NodeSpec node, String task, Map<String, String> outputs) {
            List<String> deps = new ArrayList<>(node.dependsOn());
            deps.sort(Comparator.naturalOrder());
            List<String> parts = new ArrayList<>();
            for (String dep : deps) {
                String out = outputs.get(dep);
                if (out != null && !out.isBlank()) {
                    parts.add(out);
                }
            }
            if (parts.isEmpty()) {
                return task;
            }
            return task + "\n\nPrevious results:\n" + String.join("\n\n", parts);
        }

        private boolean endReached(DeclarativePlan plan, Set<String> done) {
            for (NodeSpec n : plan.nodes()) {
                if ("end".equals(n.type()) && done.contains(n.id())) {
                    return true;
                }
            }
            return false;
        }

        private String result(DeclarativePlan plan, Map<String, String> outputs, Set<String> done) {
            for (NodeSpec n : plan.nodes()) {
                if ("end".equals(n.type()) && done.contains(n.id())) {
                    List<String> deps = new ArrayList<>();
                    for (String dep : n.dependsOn()) {
                        String out = outputs.get(dep);
                        if (out != null && !out.isBlank()) {
                            deps.add(out);
                        }
                    }
                    if (!deps.isEmpty()) {
                        return String.join("\n\n", deps);
                    }
                }
            }
            List<String> all = new ArrayList<>();
            for (NodeSpec n : plan.nodes()) {
                if ("agent".equals(n.type()) && outputs.containsKey(n.id())) {
                    all.add(outputs.get(n.id()));
                }
            }
            return String.join("\n\n", all);
        }

        private DeclarativeStep step(NodeSpec node) {
            return new DeclarativeStep(node.id(), node.title(), node.agentId(), node.systemPrompt(),
                    node.tools(), node.jsonOutput(), node.requiresHumanApproval(),
                    node.guardrailsInput(), node.guardrailsOutput(), node.hooks());
        }
    }
}
