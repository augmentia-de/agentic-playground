package de.augmentia.agentplatform.orchestrator.definition;

import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.NodeType;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record WorkflowDefinitionUpload(
        String name,
        Integer version,
        @JsonProperty("dag_structure") DagStructure dagStructure
) {

    public WorkflowDefinition toDefinition(String id, int resolvedVersion, boolean active) {
        List<DagNode> nodes = dagStructure == null || dagStructure.nodes() == null
                ? List.of()
                : dagStructure.nodes().stream().map(DagNodeUpload::toDagNode).toList();
        return new WorkflowDefinition(id, name, resolvedVersion, active, nodes);
    }

    public record DagStructure(List<DagNodeUpload> nodes) {}

    public record DagNodeUpload(
            String stepId,
            String nodeType,
            String agentType,
            String bodyAgentType,
            Boolean requiresHumanApproval,
            List<String> dependsOn,
            String guardCondition,
            List<BranchUpload> branches,
            String defaultTarget,
            String joinType,
            List<String> joinBranches,
            Integer joinN,
            Integer maxIterations,
            String exitCondition,
            Integer timeoutMs,
            List<String> bodySteps
    ) {

        public DagNode toDagNode() {
            String resolvedAgentType = agentType != null && !agentType.isBlank() ? agentType : bodyAgentType;
            return DagNode.builder()
                    .stepId(stepId)
                    .nodeType(NodeType.valueOf(nodeType))
                    .agentType(resolvedAgentType)
                    .requiresHumanApproval(Boolean.TRUE.equals(requiresHumanApproval))
                    .dependsOn(dependsOn)
                    .guardCondition(guardCondition)
                    .branches(branches == null ? List.of() : branches.stream().map(BranchUpload::toBranch).toList())
                    .defaultTarget(defaultTarget)
                    .joinType(joinType)
                    .joinBranches(joinBranches)
                    .joinN(joinN)
                    .maxIterations(maxIterations)
                    .exitCondition(exitCondition)
                    .timeoutMs(timeoutMs)
                    .bodySteps(bodySteps)
                    .build();
        }
    }

    public record BranchUpload(
            String branchId,
            String condition,
            String target,
            String firstStep
    ) {

        public DagNode.Branch toBranch() {
            return new DagNode.Branch(branchId, condition, target, firstStep);
        }
    }
}
