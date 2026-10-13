package de.augmentia.agentplatform.orchestrator.service;

public class WorkflowNotFoundException extends RuntimeException {

    private final String workflowId;

    public WorkflowNotFoundException(String workflowId) {
        super("Workflow execution not found: " + workflowId);
        this.workflowId = workflowId;
    }

    public String workflowId() {
        return workflowId;
    }
}
