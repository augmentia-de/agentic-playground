package de.augmentia.agentplatform.bridge;

/**
 * Signals that a quad-ui flat workflow cannot be converted into a Kafka-orchestrator
 * DAG. Thrown instead of silently dropping or re-typing nodes: a conversion that cannot
 * preserve the source semantics must fail loudly, not produce a subtly different workflow.
 */
public class WorkflowConversionException extends RuntimeException {

    public WorkflowConversionException(String message) {
        super(message);
    }
}
