package de.augmentia.agentflow.app.workflow.declarative;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
 * the non-selected branch targets. HITL nodes wait for {@link #decide(String, boolean)} (or a
 * blanket {@link #approve(boolean)}).</p>
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

        private final DeclarativeActivities activities = Workflow.newActivityStub(
                DeclarativeActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        private final Map<String, Boolean> decisions = new HashMap<>();
        private Boolean approveAll;
        private String rejectedAt;

        @Override
        public String run(String workflowId, String task) {
            DeclarativePlan plan = activities.plan(workflowId);

            Map<String, NodeSpec> byId = new HashMap<>();
            Set<String> owned = new HashSet<>();
            for (NodeSpec n : plan.nodes()) {
                byId.put(n.id(), n);
                owned.addAll(n.bodySteps());
            }

            Map<String, String> outputs = new HashMap<>();
            Set<String> done = new HashSet<>();
            Set<String> skipped = new HashSet<>();
            Set<String> pruned = new HashSet<>();

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
                    Set<String> targets = new HashSet<>();
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
                }
                case "join" -> {
                    outputs.put(id, context(node, task, outputs));
                    done.add(id);
                }
                case "fork" -> done.add(id);
                case "loop" -> {
                    String last = "";
                    for (int i = 0; i < Math.max(1, node.maxIterations()); i++) {
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
                    outputs.put(id, last);
                    done.add(id);
                }
                default -> done.add(id);
            }
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
            List<String> parts = new ArrayList<>();
            for (String dep : node.dependsOn()) {
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
