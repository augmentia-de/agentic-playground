package de.augmentia.agentplatform.orchestrator.service;

public class WorkflowDefinitionNotFoundException extends RuntimeException {

    public WorkflowDefinitionNotFoundException(String message) {
        super(message);
    }
}
