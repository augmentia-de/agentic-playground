package de.augmentia.agentplatform.orchestrator.service;

public class DuplicateWorkflowDefinitionException extends RuntimeException {

    public DuplicateWorkflowDefinitionException(String name, int version) {
        super("Workflow definition already exists: " + name + " v" + version);
    }
}
