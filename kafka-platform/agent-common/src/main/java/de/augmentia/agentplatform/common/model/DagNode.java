package de.augmentia.agentplatform.common.model;

import java.util.List;

/**
 * A node of the DAG definition (SPEC2 5.1/5.2).
 * Routing-relevant fields are read-only after upload; the pinned definition is
 * what the evaluator consumes (G5).
 */
public record DagNode(
        String stepId,
        NodeType nodeType,
        String agentType,
        boolean requiresHumanApproval,
        List<String> dependsOn,
        String guardCondition,
        List<Branch> branches,
        String defaultTarget,
        String joinType,
        List<String> joinBranches,
        Integer joinN,
        Integer maxIterations,
        String exitCondition,
        Integer timeoutMs,
        List<String> bodySteps
) {

    public DagNode {
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        branches = branches == null ? List.of() : List.copyOf(branches);
        joinBranches = joinBranches == null ? List.of() : List.copyOf(joinBranches);
        bodySteps = bodySteps == null ? List.of() : List.copyOf(bodySteps);
    }

    /**
     * Whether this LOOP iterates a graph region ({@link #bodySteps}) instead of a
     * single cooperating agent ({@link #agentType}). Region loops are how a loop
     * body of several agents (e.g. five steps) runs in the orchestrator.
     */
    public boolean isRegionLoop() {
        return nodeType == NodeType.LOOP && !bodySteps.isEmpty();
    }

    /** A named branch (DECISION: condition+target; FORK: firstStep of branch head). */
    public record Branch(String branchId, String condition, String target, String firstStep) {
        public static Branch decision(String branchId, String condition, String target) {
            return new Branch(branchId, condition, target, null);
        }
        public static Branch fork(String branchId, String firstStep) {
            return new Branch(branchId, null, null, firstStep);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String stepId;
        private NodeType nodeType;
        private String agentType;
        private boolean requiresHumanApproval;
        private List<String> dependsOn = List.of();
        private String guardCondition;
        private List<Branch> branches = List.of();
        private String defaultTarget;
        private String joinType;
        private List<String> joinBranches = List.of();
        private Integer joinN;
        private Integer maxIterations;
        private String exitCondition;
        private Integer timeoutMs;
        private List<String> bodySteps = List.of();

        public Builder stepId(String v) { this.stepId = v; return this; }
        public Builder nodeType(NodeType v) { this.nodeType = v; return this; }
        public Builder agentType(String v) { this.agentType = v; return this; }
        public Builder requiresHumanApproval(boolean v) { this.requiresHumanApproval = v; return this; }
        public Builder dependsOn(List<String> v) { this.dependsOn = v == null ? List.of() : v; return this; }
        public Builder guardCondition(String v) { this.guardCondition = v; return this; }
        public Builder branches(List<Branch> v) { this.branches = v == null ? List.of() : v; return this; }
        public Builder defaultTarget(String v) { this.defaultTarget = v; return this; }
        public Builder joinType(String v) { this.joinType = v; return this; }
        public Builder joinBranches(List<String> v) { this.joinBranches = v == null ? List.of() : v; return this; }
        public Builder joinN(Integer v) { this.joinN = v; return this; }
        public Builder maxIterations(Integer v) { this.maxIterations = v; return this; }
        public Builder exitCondition(String v) { this.exitCondition = v; return this; }
        public Builder timeoutMs(Integer v) { this.timeoutMs = v; return this; }
        public Builder bodySteps(List<String> v) { this.bodySteps = v == null ? List.of() : v; return this; }

        public DagNode build() {
            return new DagNode(stepId, nodeType, agentType, requiresHumanApproval, dependsOn,
                    guardCondition, branches, defaultTarget, joinType, joinBranches, joinN,
                    maxIterations, exitCondition, timeoutMs, bodySteps);
        }
    }
}