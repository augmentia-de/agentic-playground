package de.augmentia.agentplatform.orchestrator.service;

import java.util.List;

public class WorkflowValidationException extends RuntimeException {

    private final List<String> violations;

    public WorkflowValidationException(List<String> violations) {
        super("Workflow definition is invalid: " + violations);
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
